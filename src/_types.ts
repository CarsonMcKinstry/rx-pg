
export type BooleanOperators = 'gt'|'gte'|'lt'|'lte'|'eq'|'neq';

export type WhereRecord = Record<BooleanOperators, any>

export interface WhereInterface {
    [key: string]: any | any[] | WhereRecord
}

export interface JoinInterface {
    type: string,
    source: string;
    target: string;
    on: {
        [sourceField: string]: any
    }
}

export interface GetInterface {
    from: string;
    select?: string;
    join?: JoinInterface|JoinInterface[]
    where?: WhereInterface|WhereInterface[]
    limit?: 'ALL'|number;
    offset?: 0|number;
    step?: number;
}