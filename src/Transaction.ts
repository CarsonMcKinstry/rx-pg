import { Observable, of, from } from 'rxjs';
import { Client, PoolClient } from "pg";
import { mapTo, tap } from 'rxjs/operators';

export default class Transaction {
    public _open: boolean = false;
    public client: PoolClient;

    constructor(client: PoolClient) {
        this.client = client;
    }

    protected wrapQuery(queryString: string): Observable<any> {
        return from(this.client.query(queryString));
    }

    public open(): Observable<this> {
        return this.wrapQuery('BEGIN TRANSACTION;').pipe(
            tap(() => this._open = true),
            mapTo(this)
        );
    }

    public commit(): Observable<this> {
        return this.wrapQuery('COMMIT;').pipe(
            tap(() => this._open = false),
            mapTo(this)
        )
    }

    public rollback(): Observable<this> {
        return this.wrapQuery('ROLLBACK;').pipe(
            tap(() => this._open = false),
            mapTo(this)
        )
    }
}