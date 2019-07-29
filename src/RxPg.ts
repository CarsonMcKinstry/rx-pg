import { Observable, from as rxFrom, empty, of } from 'rxjs';
import {
    switchMap,
    pluck,
    map,
    share,
    concatMap,
    tap,
    bufferCount,
} from 'rxjs/operators';
import { GetInterface } from './_types';
import { Pool, PoolConfig, QueryArrayResult } from 'pg';
import { processWhere, processJoins } from './utils';

export default class RxPg {
    protected _pool: Pool;
    constructor(readonly poolConfig?: PoolConfig) {
        this._pool = new Pool(poolConfig);
    }

    async close() {
        return this._pool.end();
    }

    get client() {
        return this._pool.connect();
    }

    get(query: GetInterface): Observable<QueryArrayResult> {
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

        const client$ = rxFrom(this._pool.connect());

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
                ? of(Math.ceil(limit / step)).pipe(tap(console.log))
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
            concatMap(async (_, n) => {
                const client = await this._pool.connect();
                // const calculatedStep =  - step;
                // figure out how to calculate a step amount...
                const calculatedOffset = (offset ? offset : 0) + n * step;

                const query = `
                    SELECT ${select ? select : '*'} FROM ${from} 
                        ${joinStatement}
                        ${whereStatement}
                    LIMIT ${step}
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
