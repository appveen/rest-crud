const CRUD = require('./index').mysql;
// const utils = require('./utils');

const jsonSchema = require('./sample.json');


(async () => {
    try {
        const crud = new CRUD({
            host: 'localhost',
            user: 'root',
            password: 'itworks@123123123',
            database: 'test'
        });

        // await crud.connect();

        // const apis = crud.table('employee', jsonSchema);


        // const count = await apis.count({});
        // console.log('count', count);
    } catch (err) {
        console.log(err);
    }
})();

// const fields = utils.getFieldsFromSchema(jsonSchema);
// console.log(fields);

