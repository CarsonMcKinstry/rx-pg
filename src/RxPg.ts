import { Observable, from as rxFrom, of, from, timer, concat } from 'rxjs';
import {
    switchMap,
    pluck,
    map,
    share,
    concatMap,
    withLatestFrom,
    mapTo,
    switchMapTo,
    scan,
    filter,
    takeUntil,
    merge,
    ignoreElements,
} from 'rxjs/operators';
import { GetInterface, InsertInterface } from './_types';
import { Pool, PoolConfig, QueryResult, PoolClient } from 'pg';
import { processWhere, processJoins } from './utils';
import Transaction from './Transaction';

export default class RxPg {
    private _pool: Pool;
    private _transaction?: Transaction;

    constructor(readonly poolConfig?: PoolConfig) {
        this._pool = new Pool(poolConfig);
    }

    /**
     * Close the current database connection
     */
    public async close(): Promise<any> {
        return this._pool.end();
    }

    /**
     * get a client
     */
    get client(): Observable<PoolClient> {
        return this.isTransactionOpen().pipe(
            switchMap(open =>
                open
                    ? of(this._transaction!.client)
                    : rxFrom(this._pool.connect())
            )
        );
    }

    /**
     * Determines if there is an active transaction
     */
    private isTransactionOpen(): Observable<boolean> {
        if (this._transaction) {
            return of(this._transaction._open);
        }

        return of(false);
    }

    /**
     * Opens a new transaction and errors if one is already open
     */
    public transaction(): Observable<this> {
        return this.isTransactionOpen().pipe(
            switchMap(open => {
                if (open) {
                    throw new Error(
                        'transaction is already open, please call one of the other methods'
                    );
                }

                return rxFrom(this._pool.connect());
            }),
            switchMap(client => {
                this._transaction = new Transaction(client);

                return this._transaction.open();
            }),
            mapTo(this)
        );
    }

    /**
     * Perform a select query
     * @param query an interface represententing the query
     */
    public get(query: GetInterface): Observable<QueryResult> {
        const {
            where,
            select = '*',
            from,
            offset = 0,
            limit,
            step = 1000,
            join,
        } = query;
        const { statement: whereStatement, values: whereValues } = processWhere(
            where,
            1
        );
        const joinStatement = processJoins(join);

        if (limit && limit < step) {
            return this.client.pipe(
                switchMap(client => {
                    const query = `
                        SELECT ${select ? select : '*'} FROM ${from} 
                            ${joinStatement}
                            ${whereStatement}
                        LIMIT ${limit}
                        OFFSET ${offset};
                    `;

                    return rxFrom(client.query(query, whereValues));
                })
            );
        }

        const stepCount$ =
            limit && limit > step
                ? of(Math.ceil(limit / step))
                : this.client.pipe(
                      switchMap(client =>
                          client.query(
                              `select count(*) from ${query.from} ${whereStatement};`,
                              whereValues
                          )
                      ),
                      pluck('rows'),
                      map(([{ count }]) => parseInt(count, 10)),
                      map(count => Math.ceil(count / step)),
                      share()
                  );

        // const queryInterval$ = stepCount$.pipe(switchMapTo(timer(0)));

        // const numberOfQueriesExecuted$ = queryInterval$.pipe(
        //     scan(acc => acc + 1, 0)
        // );
        // const stopSignal$ = numberOfQueriesExecuted$.pipe(
        //     withLatestFrom(stepCount$),
        //     filter(
        //         ([numberOfQueriesExecuted, stepCount]) =>
        //             numberOfQueriesExecuted === stepCount + 1
        //     )
        // );

        // const query$ = stepCount$.pipe(
        //     withLatestFrom(stepCount$),
        //     concatMap(([n, stepCount]) => {
        //         const client = this.client;

        //         const calculatedStep =
        //             limit && limit > step ? limit % step : step;
        //         const calculatedOffset = (offset ? offset : 0) + n * step;
        //         const queryString = `
        //             SELECT ${select ? select : '*'} FROM ${from}
        //                 ${joinStatement}
        //                 ${whereStatement}
        //             LIMIT ${n + 1 === stepCount ? calculatedStep : step}
        //             OFFSET ${calculatedOffset};
        //         `;

        //         return client.pipe(
        //             switchMap(async c => {
        //                 const q = await c.query(queryString, whereValues);

        //                 await c.release();

        //                 return q;
        //             })
        //         )
        //     }),
        //     takeUntil(stopSignal$)
        // );

        // return query$;

        const query$ = stepCount$.pipe(
            switchMap(queryCount => rxFrom([...Array(queryCount)])),
            withLatestFrom(stepCount$),
            concatMap(([_, stepCount], n) => {
                const calculatedStep =
                    limit && limit > step ? limit % step : step;
                const calculatedOffset = (offset ? offset : 0) + n * step;

                const query = `
                    SELECT ${select ? select : '*'} FROM ${from}
                        ${joinStatement}
                        ${whereStatement}
                    LIMIT ${n + 1 === stepCount ? calculatedStep : step}
                    OFFSET ${calculatedOffset};
                `;

                return this.query(query, whereValues);
            }),
            share()
        );

        return query$;
    }

    public insert(query: InsertInterface): Observable<QueryResult> {
        const { into, data, returning = '*' } = query;

        const entries = Object.entries(data || {});

        if (!into) {
            throw new Error('table to insert data into not provided');
        }

        if (entries.length < 1) {
            throw new Error('data not provided');
        }

        const columnNames = entries.map(([column]) => column);
        const preparedValues = entries.map((_, i) => `$${i + 1}`);
        const values = entries.map(([, value]) => value);

        const queryString = `
            INSERT INTO ${into} (${columnNames}) VALUES (${preparedValues})
            RETURNING ${returning};
        `;

        return this.query(queryString, values);
    }

    public query(queryString: string, preparation: any[]) {
        return this.client.pipe(
            switchMap(async client => {
                const r = await client.query(queryString, preparation);

                await client.release();

                return r;
            })
        );
    }
}
