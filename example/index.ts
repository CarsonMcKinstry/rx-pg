import RxPg from '../src/RxPg';

const db = new RxPg({
    host: 'localhost',
    port: 5432,
    user: 'postgres',
    password: 'docker',
    database: 'playground',
});

db.get({
    from: 'posts',
    select: ['posts.id as post_id', 'users.id as user_id'],
    join: {
        source: 'posts',
        target: 'users',
        on: {
            'posted_by': 'id'
        }
    },
    limit: 1001
}).subscribe(null, null, () => {
    db.close();
});