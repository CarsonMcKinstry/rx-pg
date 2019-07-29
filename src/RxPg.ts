import { Observable, from as rxFrom } from 'rxjs';
import { switchMap, pluck, map, share, concatMap, tap } from 'rxjs/operators';
import { GetInterface } from './_types';
import { Pool, PoolConfig, QueryArrayResult } from "pg";
import { processWhere, processJoins } from './utils'


export default class RxPg {
    protected _pool: Pool
    constructor(readonly poolConfig?: PoolConfig) {
        this._pool = new Pool(poolConfig);

    }

    async close() {
        return this._pool.end();
    }

    get client () {
        return this._pool.connect();
    }

    get(query: GetInterface): Observable<QueryArrayResult> {
        const { where, select = '*', from, offset = 0, step = 1000, join } = query;
        const { statement: whereStatement, values: whereValues } = processWhere(where, 1);
        const joinStatement = processJoins(join);

        const client$ = rxFrom(this._pool.connect());

        const count$ = client$.pipe(
            switchMap(async client => {
                const q = await client.query(`select count(*) from ${query.from} ${whereStatement}`, whereValues);

                await client.release();

                return q;
            }),
            pluck('rows'),
            map(([ { count }]) => parseInt(count, 10)),
            map(count => Math.ceil(count / (query.step || 1000))),
            share()
        )

        const query$ = count$.pipe(
            tap(q => 
                console.log([...Array(q)].map((_, i) => i))
            ),
            switchMap(queryCount => rxFrom([...Array(queryCount)])),
            concatMap(async (_, n) => {
                console.log(n);
                const client = await this._pool.connect();
                const calculatedOffset = (offset ? offset : 0) + (n * step);
                const query = `
                    SELECT ${select ? select : '*'} FROM ${from} 
                        ${joinStatement}
                        ${whereStatement}
                    LIMIT ${step}
                    OFFSET ${calculatedOffset};
                `;

                console.log(query);

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
