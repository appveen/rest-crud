const CRUD = require('../index').pgsql;
// const utils = require('./utils');

const jsonSchema = require('./sample.json');


(async () => {
    try {
        const msCrud = await new CRUD();
        await msCrud.connect();

        // Test custom SQL Query
        // const result = await msCrud.sqlQuery('SELECT 1 + 1 AS solution');
        // console.log('Result :: ', result.rows[0].solution);


        // Create table in MS SQL if it doesn't exist
        const apis = msCrud.table('employee', jsonSchema);
        // await apis.createTable();


        // Get total count of all records in table
        const totalCount = await apis.count({});
        console.log('Total records count :: ', totalCount);


        // Count records based on filter
        // const count = await apis.count({"name": "Kavi"});
        // console.log('Records count :: ', count);


        // List all records
        // const allRecords = await apis.list();
        // console.log('All Records :: ', allRecords);

        // List records with filter
        // const filterRecords = await apis.list({"filter": {"name": "Jugnu"}});
        // console.log('Filtered Records :: ', filterRecords);

        // List records with filter & select
        // const selectRecords = await apis.list({"filter": {"name": "Jugnu"}, "select": "name,email,password"});
        // console.log('Filter and Select Records :: ', selectRecords);

        // List records with orderBy/sort
        // const orderedRecords = await apis.list({"sort": "-_id", "select": "name,email,password"});
        // console.log('Ordered Records :: ', orderedRecords);

        // List records with orderBy, limit and offset
        // const limitRecords = await apis.list({"sort": "_id", "select": "_id,name,email,password", "count": 5, "page": 0, "filter": {"name": "Kavi"}});
        // console.log('Limit Records :: ', limitRecords);

        // Show record by ID
        // const showRecord = await apis.show("TES1001");
        // console.log('Show Record :: ', showRecord);

        // Show record by ID and select fields
        // const showSelectRecord = await apis.show("TES1002", {"select": "name,email,password,_id"});
        // console.log('Show Select Record :: ', showSelectRecord);

        // Delete record by ID
        // const deleteRecord = await apis.delete("TES1003");
        // console.log('Delete Record :: ', deleteRecord);

        // Delete many records by IDs
        // const deleteManyRecords = await apis.deleteMany("TES1003,TES1004");
        // console.log('Delete Many Records :: ', deleteManyRecords);

        // let data = {
        //     "name": "Kavi",
        //     "email": "kavi@appveen.com",
        //     "password": "abcdefgh",
        //     "contactNo": "1234567890",
        //     "pan": "Updated",
        //     "address.stOne": "updated",
        //     "address.stTwo": "updated",
        //     "address.city": "DED",
        //     "address.country": "IND",
        //     "address.pincode": 412411
        // };
        // let data2 = {
        //     "name": "Jugnu",
        //     "email": "jugnu@appveen.com",
        //     "password": "abcdefgh",
        //     "contactNo": "1234567890",
        //     "address.stOne": "updated",
        //     "address.stTwo": "updated",
        //     "address.city": "XYZ",
        //     "address.country": "IND",
        //     "address.pincode": 412412
        // };

        // Insert new record
        // let createdRecord = await apis.create(data);
        // console.log('Created Record :: ', createdRecord);

        // Insert multiple new records
        // let createdRecords = await apis.create([data, data2]);
        // console.log('Created Records :: ', createdRecords);

        // let id1 = createdRecords[0]._id;
        // let id2 = createdRecords[1]._id;

        // let d = {};
        // d.email = 'Test@appveen.com';
        // d.password = 'testing';

        // Update existing record
        // let updatedRecord = await apis.update(`${id1},${id2}`, d);
        // console.log('Updated Record :: ', updatedRecord);

    } catch (err) {
        console.log(err);
    }
})();

// const fields = utils.getFieldsFromSchema(jsonSchema);
// console.log(fields);

