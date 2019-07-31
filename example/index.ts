import RxPg from '../src/RxPg';
import { reduce, pluck, tap } from 'rxjs/operators';

const db = new RxPg({
    host: 'localhost',
    port: 5432,
    user: 'postgres',
    password: 'docker',
    database: 'playground',
});

console.time('streamed pg')
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
    limit: 100
})
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
