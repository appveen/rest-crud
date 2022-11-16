const CRUD = require('./index').mssql;
// const utils = require('./utils');

const jsonSchema = require('./sample.json');


(async () => {
    try {
        const crud = await new CRUD({
            connectionString: ''
        });

        await crud.connect();

        const result = await crud.sqlQuery('SELECT 1 + 1 AS solution');


        // const count = await apis.count({});
        // const result = await apis.sqlQuery("");
        console.log('count', result.recordset[0].solution);
    } catch (err) {
        console.log(err);
    }
})();

// const fields = utils.getFieldsFromSchema(jsonSchema);
// console.log(fields);

