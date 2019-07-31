import { Observable, from as rxFrom, of } from 'rxjs';
import {
    switchMap,
    pluck,
    map,
    share,
    concatMap,
    withLatestFrom,
    mapTo,
} from 'rxjs/operators';
import { GetInterface } from './_types';
import { Pool, PoolConfig, QueryArrayResult, PoolClient } from 'pg';
import { processWhere, processJoins } from './utils';
import Transaction from './Transaction';

export default class RxPg {
    private _pool: Pool;
    private _transaction?: Transaction

    constructor(readonly poolConfig?: PoolConfig) {
        this._pool = new Pool(poolConfig);
    }

    /**
     * Close the current database connection
     */
    public close(): Observable<any> {
        return of(this._pool.end());
    }

    /**
     * get a client
     */
    get client(): Promise<PoolClient> {
        return this._pool.connect();
    }

    private isTransactionOpen(): Observable<boolean> {
        if (this._transaction) {
            return of(this._transaction._open);
        }

        return of(false);
    }

    public transaction(): Observable<this> {
        return this.isTransactionOpen()
            .pipe(
                switchMap(open => {
                    if (open) {
                        throw new Error('transaction is already open, please call one of the other methods');
                    }

                    return rxFrom(this._pool.connect())
                }),
                switchMap(client => {
                    this._transaction = new Transaction(client);

                    return this._transaction.open();
                }),
                mapTo(this)
            )
    }

    public get(query: GetInterface): Observable<QueryArrayResult> {
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

        const client$: Observable<PoolClient> = this.isTransactionOpen().pipe(
            switchMap((open) => open ? of(this._transaction!.client) : rxFrom(this._pool.connect()))
        );

        if (limit && limit < step) {
            return client$.pipe(
                switchMap(async client => {
                    const query = `
                        SELECT ${select ? select : '*'} FROM ${from} 
                            ${joinStatement}
                            ${whereStatement}
                        LIMIT ${limit}
                        OFFSET ${offset};`;
                    const q = await client.query(query, whereValues);

                    await client.release();

                    return q.rows;
                }),
                switchMap(o => o)
            );
        }

        const stepCount$ =
            limit && limit > step
                ? of(Math.ceil(limit / step))
                : client$.pipe(
                      switchMap(async client => {
                          const q = await client.query(
                              `select count(*) from ${query.from} ${whereStatement};`,
                              whereValues
                          );

                          await client.release();

                          return q;
                      }),
                      pluck('rows'),
                      map(([{ count }]) => parseInt(count, 10)),
                      map(count => Math.ceil(count / step)),
                      share()
                  );

        const query$ = stepCount$.pipe(
            switchMap(queryCount => rxFrom([...Array(queryCount)])),
            withLatestFrom(stepCount$),
            concatMap(async ([_, stepCount], n) => {
                const client = await this._pool.connect();
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

                const results = await client.query(query, whereValues);

                await client.release();

                return results.rows;
            }),
            switchMap(o => o),
            share()
        );

        return query$;
    }
}
