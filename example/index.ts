import RxPg from '../src/RxPg';

const db = new RxPg({
    host: 'localhost',
    port: 5432,
    user: 'postgres',
    password: 'docker',
    database: 'playground',
});

db.get({
    from: 'users'
}).subscribe(console.log, null, () => {
    db.close();
});