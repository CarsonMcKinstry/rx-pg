import { WhereInterface, JoinInterface, BooleanOperators } from './_types';

interface WhereStatement {
    statement: string;
    values: any[];
}

export function processWhere(
    where?: WhereInterface,
    startValuesAt: number = 1
): WhereStatement {
    if (!where) {
        return {
            statement: '',
            values: [],
        };
    }

    const operatorMap: { [key in BooleanOperators]: string } = {
        gt: '>',
        lt: '<',
        gte: '>=',
        lte: '<=',
        eq: '=',
        neq: '!=',
    };

    const whereEntries = Object.entries(where);

    let currentValueIndex = startValuesAt;

    const values = whereEntries.reduce((vals, [_, val]) => {
        if (Array.isArray(val) && typeof val !== 'object') {
            return vals.concat(val);
        }

        return vals.concat(Object.values(val));
    }, []);

    const statement = whereEntries
        .reduce((statements: string[], [field, val], i) => {
            if (Array.isArray(val)) {
                return statements.concat(
                    `${field} in ${val.map(() => {
                        const index = `$${currentValueIndex}`;
                        currentValueIndex++;
                        return index;
                    })}`
                );
            }

            if (typeof val === 'object') {
                const comparisonStatements = Object.entries(val).map(([op]) => {
                    const nextStatement = `${field} ${operatorMap[op as BooleanOperators]} $${currentValueIndex}`;

                    currentValueIndex++;

                    return nextStatement;
                });
                return statements.concat(comparisonStatements.join(' AND '));
            }

            const nextStatements = statements.concat(
                `${field} = $${currentValueIndex}`
            );

            currentValueIndex++;

            return nextStatements;
        }, [])
        .join(' AND ');

    return {
        statement: `WHERE ${statement}`,
        values,
    };
}

function formatJoin(join: JoinInterface) {
    const { type = '', source, target, on } = join;
    if (join && (!source || !target || !on)) {
        throw new TypeError('One property of source, target, on not included');
    }
    const onEntries = Object.entries(on);
    const onStatements = onEntries.map(([sourceField, targetField], i) => {
        const statement = `${source}.${sourceField} = ${target}.${targetField}`;

        return i > 0 && i < onEntries.length ? `AND ${statement}` : statement;
    });

    return `
            ${type} JOIN ${target} ON
                ${onStatements.join(' ')}
        `;
}

export function processJoins(joins?: JoinInterface | JoinInterface[]): string {
    if (Array.isArray(joins) && joins.length > 0) {
        return joins.map(formatJoin).join('\n');
    }

    if (!joins) return '';

    return formatJoin(joins as JoinInterface);
}
