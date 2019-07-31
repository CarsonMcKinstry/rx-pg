import RxPg from '../src/RxPg';
import { reduce, pluck, tap, map, switchMap, scan } from 'rxjs/operators';
import { from } from 'rxjs';

const db = new RxPg({
    host: 'localhost',
    port: 5432,
    user: 'postgres',
    password: 'postgres',
    database: 'postgres',
});

console.time('streamed pg');
db.get({
    from: 'posts',
    select: ['posts.id as post_id', 'users.id as user_id'],
    join: {
        source: 'posts',
        target: 'users',
        on: {
            posted_by: 'id',
        },
    },
    limit: 50000,
    step: 5000,
})
    .pipe(
        map(({ rows }) => rows),
        switchMap(rows => from(rows)),
        reduce(acc => acc + 1, 0)
    )
    .subscribe(console.log, null, async () => {
        console.log('closing');
        await db.close();
        console.timeEnd('streamed pg');
    });

// db.insert({
//     into: 'users',
//     data: {
//         name: 'Carson McKinstry',
//         age: 25
//     }
// })
//     .subscribe(console.log, null, async () => {
//         console.log('closing');
//         await db.close();
//         console.timeEnd('streamed pg');
//     });
