const mssql = require('mssql');
let log4js = require('log4js');

const utils = require('../utils');
let version = require('../package.json').version;

const logLevel = process.env.LOG_LEVEL || 'trace';
const loggerName = process.env.HOSTNAME ? `[${process.env.DATA_STACK_NAMESPACE}] [${process.env.HOSTNAME}] [REST_CRUD MSSQL ${version}]` : `[REST_CRUD MSSQL ${version}]`;
log4js.configure({
    levels: {
        AUDIT: { value: Number.MAX_VALUE - 1, colour: 'yellow' }
    },
    appenders: { out: { type: 'stdout', layout: { type: 'basic' } } },
    categories: { default: { appenders: ['out'], level: logLevel.toUpperCase() } }
});
let logger = log4js.getLogger(loggerName);


/**
 * @param {object} options CRUD options
 * @param {string} options.host
 * @param {string} options.user
 * @param {string} options.password
 * @param {string} options.database
 * @param {boolean} options.customId
 * @param {string} options.idPattern
 */
function CRUD(options) {
    this.database = options.database;
    this.customId = options.customId || false;
    this.idPattern = options.idPattern || '';
    this.connectionString = options.connectionString;
}


CRUD.prototype.connect = async function () {
    try {
        logger.debug('Connecting to MSSQL');
        logger.trace(`Connection String :: ${this.connectionString}`);

        this.connection = await mssql.connect(this.connectionString);

        let result = await this.connection.query('SELECT 1 + 1 AS solution');

        logger.trace(`Query Soluton :: ${result.recordset[0].solution}`);
        logger.info('Connection Successfull!');
    } catch (err) {
        logger.error('Error connecting :: ', err);
        throw err;
    }
};

CRUD.prototype.disconnect = function () {
    try {
        this.connection.end();
        logger.info('Database Disconnected!');
    } catch (err) {
        logger.error('Error disconnecting :: ', err);
        throw err;
    }
};

CRUD.prototype.sqlQuery = function (sql) {
    return new Promise((resolve, reject) => {
        try {
            logger.debug(`Performing SQL Query`);
            logger.trace(`SQL Query :: ${sql}`);

            if (!sql) {
                logger.error('No sql query provided.');
                return reject(new Error('No sql query provided.'));
            }

            this.connection.query(sql, function (error, results, fields) {
                if (error) {
                    logger.error(`Error querying db :: ${error}`);
                    return reject(error);
                };

                logger.debug('Query successful');
                logger.trace(`Query result :: ${JSON.stringify(results)}`);
                resolve(results);
            });
        } catch (err) {
            logger.error(`Error querying :: ${err}`);
            reject(err);
        }
    });
};


/**
 * @param {string} table
 * @param {object} jsonSchema
 */
CRUD.prototype.table = function (table, jsonSchema) {
    const options = {};
    options.table = table;
    options.database = this.database;
    options.customId = this.customId;
    options.idPattern = this.idPattern;
    options.connection = this.connection;
    return new Table(options, jsonSchema);
};

function Table(options, jsonSchema) {
    this.database = options.database;
    this.customId = options.customId || false;
    this.idPattern = options.idPattern || '';
    this.table = options.table;
    this.connection = options.connection;
    this.fields = utils.getFieldsFromSchema(jsonSchema);
}

Table.prototype.createTable = async function () {
    try {
        logger.debug(`Creating Table :: ${this.table}`);

        let tableCheckSql = `SELECT * FROM sysobjects WHERE name='${this.table}' and xtype='U'`;
        logger.trace(`SQL query to check if table exists in db :: ${tableCheckSql}`);

        let exists = await this.connection.query(tableCheckSql);
        logger.debug(`Table exists? :: ${exists.recordset.length > 0 ? 'true' : 'false'}`);

        if (result.recordset.length <= 0) {
            let sql = utils.createTableStatement(this.fields);
            logger.trace(`SQL query to create table :: ${`CREATE TABLE ${this.table}(${sql})`}`);

            let tableResult = await this.connection.query(`CREATE TABLE ${this.table}(${sql})`);

            logger.debug(`Table created successfully`);
            logger.trace(`Table created :: ${JSON.stringify(tableResult)}`);
        }
    } catch (err) {
        logger.error(`Error creating table :: ${err}`);
        throw err;
    }
}

Table.prototype.count = function (filter) {
    return new Promise((resolve, reject) => {
        try {
            logger.debug('Counting rows in DB.');
            logger.trace(`Filter for count :: ${JSON.stringify(filter)}`);

            let sql = `SELECT count(*) AS count FROM ${this.table}`;
            const whereClause = utils.whereClause(this.fields, filter);
            if (whereClause && whereClause.trim()) {
                sql += whereClause;
            }
            logger.trace(`SQL Query to count :: ${sql}`);

            this.connection.query(sql, function (error, results, fields) {
                if (error) {
                    logger.error(`Error querying count :: ${error}`);
                    return reject(error);
                };

                logger.debug(`Total records count :: ${results.recordset[0].count}`);
                resolve(results.recordset[0].count);
            });
        } catch (err) {
            logger.error(`Error counting records :: ${err}`);
            reject(err);
        }
    });
};

Table.prototype.list = function (options) {
    return new Promise((resolve, reject) => {
        try {
            logger.debug('Listing rows in DB.');
            logger.trace(`Filters for listing :: ${JSON.stringify(options)}`);

            let sort = options?.sort || '_id';

            const selectClause = utils.selectClause(this.fields, options?.select) || '*';
            const whereClause = utils.whereClause(this.fields, options?.filter);
            const limitClause = utils.limitClauseMS(options?.count, options?.page);
            const orderByClause = utils.orderByClause(this.fields, sort);
            
            let sql = `SELECT ${selectClause} FROM ${this.table}`;
            if (whereClause) {
                sql += whereClause;
            }
            if (orderByClause) {
                sql += orderByClause;
            }
            if (limitClause) {
                sql += limitClause;
            }

            logger.trace(`SQL query to list :: ${sql}`);

            this.connection.query(sql, function (error, results, fields) {
                if (error) {
                    logger.error(`Error querying list :: ${error}`);
                    return reject(error);
                };

                logger.debug(`List query auccessful`);
                logger.trace(`List of records :: ${JSON.stringify(utils.unscapeData(results.recordset))}`);
                resolve(utils.unscapeData(results.recordset));
            });
        } catch (err) {
            logger.error(`Error listing records :: ${err}`);
            reject(err);
        }
    });
};

Table.prototype.show = function (id, options) {
    return new Promise((resolve, reject) => {
        try {
            logger.debug(`Fetching row from DB :: ${id}`);
            logger.trace(`Filters for show :: ${JSON.stringify(options)}`);

            const selectClause = utils.selectClause(this.fields, options?.select) || '*';
            let sql = `SELECT ${selectClause} FROM ${this.table} WHERE _id='${id}'`;

            logger.trace(`SQL query for show :: ${sql}`);

            this.connection.query(sql, function (error, results, fields) {
                if (error) {
                    logger.error(`Error fetching record :: ${error}`);
                    return reject(error);
                };

                logger.debug(`Show record query auccessful`);
                logger.trace(`Record details :: ${JSON.stringify(utils.unscapeData(results.recordset[0]))}`);
                resolve(utils.unscapeData(results.recordset[0]));
            });
        } catch (err) {
            logger.error(`Error fetching record :: ${err}`);
            reject(err);
        }
    });
};

// Table.prototype.create = function (data) {
//     return new Promise(async (resolve, reject) => {
//         try {
//             logger.debug(`Creating new row in DB`);
//             logger.trace(`Data to create :: ${JSON.stringify(data)}`);

//             if (!data._id) {
//                 data._id = utils.token();
//             }
//             const stmt = utils.insertStatement(this.fields, data);
//             if (!stmt) {
//                 return reject(new Error('No data to insert'));
//             }
//             let sql1 = `INSERT INTO ${this.table} ${stmt}`;

//             logger.trace(`SQL query for insert :: ${sql1}`);
//             await this.connection.query(sql1);
//             logger.debug(`Record created successfully`);


//             let sql2 = `SELECT * FROM ${this.table} WHERE _id='${data._id}'`;
            
//             logger.trace(`SQL query for show :: ${sql2}`);
//             let result2 = await this.connection.query(sql2);


//             logger.trace(`Record details :: ${JSON.stringify(utils.unscapeData(result2.recordset[0]))}`);
//             resolve(utils.unscapeData(result2.recordset[0]));
//         } catch (err) {
//             logger.error(`Error inserting/displaying record :: ${err}`);
//             reject(err);
//         }
//     });
// };

Table.prototype.create = function (data) {
    return new Promise(async (resolve, reject) => {
        try {
            logger.debug(`Creating new row in DB`);
            logger.trace(`Data to create :: ${JSON.stringify(data)}`);

            if (!data) {
                return reject(new Error('No data to insert'));
            }

            if (!Array.isArray(data)) {
                data = [data];
            }
            
            data.map(obj => {
                if (!obj._id) {
                    obj._id = utils.token();
                }
            });

            const stmt = utils.insertManyStatement(this.fields, data);
            if (!stmt) {
                return reject(new Error('No data to insert'));
            }
            let sql1 = `INSERT INTO ${this.table} ${stmt}`;

            logger.trace(`SQL query for insert :: ${sql1}`);
            await this.connection.query(sql1);
            logger.debug(`Records created successfully`);


            let sql2 = `SELECT * FROM ${this.table} WHERE _id IN (${data.map(obj => `'${obj._id}'`).join(`,`)})`;

            logger.trace(`SQL query for show :: ${sql2}`);
            let result2 = await this.connection.query(sql2);


            logger.trace(`Records details :: ${JSON.stringify(utils.unscapeData(result2.recordset))}`);
            resolve(utils.unscapeData(result2.recordset));
        } catch (err) {
            logger.error(`Error inserting/displaying records :: ${err}`);
            reject(err);
        }
    });
};

Table.prototype.update = function (id, data) {
    return new Promise((resolve, reject) => {
        try {
            logger.debug(`Updating row in DB :: ${id}`);
            logger.trace(`Data to create :: ${JSON.stringify(data)}`);

            if (!id) {
                return reject(new Error('No id provided to update record'));
            }

            const stmt = utils.updateStatement(this.fields, data);
            if (!stmt) {
                return reject(new Error('data has no matching field to update'));
            }
            let sql = `UPDATE ${this.table} ${stmt} WHERE _id='${id}'`;

            logger.trace(`SQL query for update :: ${sql}`);
            this.connection.query(sql, function (error, results, fields) {
                if (error) {
                    logger.error(`Error updating record :: ${error}`);
                    return reject(error);
                };

                logger.debug(`Record updated successfully`);
                logger.trace(`Updated record details :: ${JSON.stringify(results.recordset[0])}`);
                resolve(results);
            });
        } catch (err) {
            logger.error(`Error updating record :: ${err}`);
            reject(err);
        }
    });
};

Table.prototype.delete = function (id) {
    return new Promise((resolve, reject) => {
        try {
            logger.debug(`Deleting record in DB :: ${id}`);

            if (!id) {
                return reject(new Error('No id provided to delete record'));
            }
            let sql = `DELETE FROM ${this.table} WHERE _id='${id}'`;

            logger.trace(`SQL query for delete :: ${sql}`);
            this.connection.query(sql, function (error, results, fields) {
                if (error) {
                    logger.error(`Error deleting record :: ${error}`);
                    return reject(error);
                };

                logger.debug(`Record deleted successfully`);
                resolve(results.rowsAffected[0]);
            });
        } catch (err) {
            logger.error(`Error deleting record :: ${err}`);
            reject(err);
        }
    });
};

Table.prototype.deleteMany = function (ids) {
    return new Promise((resolve, reject) => {
        try {
            logger.debug(`Deleting multiple records from DB :: ${ids}`);

            if (!ids) {
                return reject(new Error('No id provided to delete record'));
            }
            let sql = `DELETE FROM ${this.table} WHERE _id IN (${ids.split(',').map(id => `'${id}'`).join(',')})`;
            
            logger.trace(`SQL query for deleting multiple records :: ${sql}`);
            this.connection.query(sql, function (error, results, fields) {
                if (error) {
                    logger.error(`Error deleting multiple records :: ${error}`);
                    return reject(error);
                };

                logger.debug(`Records deleted successfully :: ${results.rowsAffected[0]}`);
                resolve(results.rowsAffected[0]);
            });
        } catch (err) {
            logger.error(`Error deleting multiple records :: ${err}`);
            reject(err);
        }
    });
};


module.exports = CRUD;
