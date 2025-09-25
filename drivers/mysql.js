const mysql = require('mysql2/promise');
let log4js = require('log4js');
let _ = require('lodash');

const utils = require('../utils');
let version = require('../package.json').version;

const logLevel = process.env.LOG_LEVEL || 'trace';
const loggerName = process.env.HOSTNAME ? `[${process.env.DATA_STACK_NAMESPACE}] [${process.env.HOSTNAME}] [REST_CRUD MYSQL ${version}]` : `[REST_CRUD MYSQL ${version}]`;
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
    this.connectionDetails = {
        host: options.host,
        port: options.port,
        user: options.user,
        password: options.password,
        database: options.database
    };
}

CRUD.prototype.connect = async function () {
    try {
        logger.debug('Connecting to MYSQL');
        logger.trace(`Connection details :: ${JSON.stringify(this.connectionDetails)}`);

        this.connection = await mysql.createConnection(this.connectionDetails);

        this.connection.connect();

        let result = await this.connection.query('SELECT 1 + 1 AS solution');

        logger.trace(`Query Soluton :: ${result[0][0].solution}`);
        logger.info('Connection Successfull!');

        return 'Connection Successfull';
    } catch (err) {
        logger.error('Error connecting :: ', err);
        throw err;
    }
};

CRUD.prototype.disconnect = async function () {
    try {
        this.connection.end();
        console.log('Database Disconnected!');

        return 'Database Disconnected';
    } catch (err) {
        logger.error('Error disconnecting :: ', err);
        throw err;
    }
};

CRUD.prototype.sqlQuery = async function (sql, values) {
    try {
        logger.debug(`Performing SQL Query`);
        logger.trace(`SQL Query :: ${sql}`);

        if (!sql) {
            logger.error('No sql query provided.');
            throw new Error('No sql query provided.');
        }

        let result = await this.connection.query(sql, values);

        logger.trace(`Query result :: ${JSON.stringify(result)}`);
        return utils.unscapeData(result[0]);

    } catch (err) {
        logger.error(`Error querying :: ${err}`);
        throw err;
    }
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

        let sql = utils.createTableStatement(this.fields);
        logger.trace(`SQL query to create table :: ${`CREATE TABLE IF NOT EXISTS ${this.table}(${sql})`}`);

        let tableResult = await this.connection.query(`CREATE TABLE IF NOT EXISTS ${this.table}(${sql})`);

        logger.debug(`Table created successfully`);
        logger.trace(`Table created :: ${JSON.stringify(tableResult)}`);

        return 'Table created';
    } catch (err) {
        logger.error(`Error creating table :: ${err}`);
        throw err;
    }
}

Table.prototype.count = async function (filter) {
    try {
        logger.debug('Counting rows in DB.');
        logger.trace(`Filter for count :: ${JSON.stringify(filter)}`);

        let sql = `SELECT count(*) AS count FROM ${this.table}`;
        const whereClause = utils.whereClause(this.fields, filter);
        if (whereClause && whereClause.trim()) {
            sql += whereClause;
        }
        logger.trace(`SQL Query to count :: ${sql}`);

        let result = await this.connection.query(sql);

        logger.debug(`Total records count :: ${result[0][0].count}`);
        return result[0][0].count;

    } catch (err) {
        logger.error(`Error counting records :: ${err}`);
        throw err;
    }
};

Table.prototype.list = async function (options) {
    try {
        logger.debug('Listing rows in DB.');
        logger.trace(`Filters for listing :: ${JSON.stringify(options)}`);

        let whereClause;
        if (options?.filter && !_.isEmpty(options.filter)){
            whereClause = utils.whereClause(this.fields, options?.filter);
        }

        const selectClause = utils.selectClause(this.fields, options?.select) || '*';
        const limitClause = utils.limitClause(options?.count, options?.page);
        const orderByClause = utils.orderByClause(this.fields, options?.sort);

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

        let result = await this.connection.query(sql);

        logger.debug(`List query successful`);
        logger.trace(`List of records :: ${JSON.stringify(utils.unscapeData(result[0]))}`);

        return utils.unscapeData(result[0]);

    } catch (err) {
        logger.error(`Error listing records :: ${err}`);
        throw err;
    }
};

Table.prototype.show = async function (id, options) {
    try {
        logger.debug(`Fetching row from DB :: ${id}`);
        logger.trace(`Filters for show :: ${JSON.stringify(options)}`);

        const selectClause = utils.selectClause(this.fields, options?.select) || '*';
        let sql = `SELECT ${selectClause} FROM ${this.table} WHERE _id='${id}'`;

        logger.trace(`SQL query for show :: ${sql}`);

        let result = await this.connection.query(sql);

        logger.debug(`Show record query auccessful`);
        logger.trace(`Record details :: ${JSON.stringify(utils.unscapeData(result[0][0]))}`);

        return utils.unscapeData(result[0][0]);

    } catch (err) {
        logger.error(`Error fetching record :: ${err}`);
        throw err;
    }
};

Table.prototype.create = async function (data) {
    try {
        logger.debug(`Creating new row in DB`);
        logger.trace(`Data to create :: ${JSON.stringify(data)}`);

        if (!data) {
            throw new Error('No data to insert');
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
            throw new Error('No data to insert');
        }
        let sql1 = `INSERT INTO ${this.table} ${stmt}`;

        logger.trace(`SQL query for insert :: ${sql1}`);
        let result1 = await this.connection.query(sql1);
        logger.debug(`Records created successfully`);


        let sql2 = `SELECT * FROM ${this.table} WHERE _id IN (${data.map(obj => `'${obj._id}'`).join(`,`)})`;

        logger.trace(`SQL query for show :: ${sql2}`);
        let result2 = await this.connection.query(sql2);


        logger.trace(`Records details :: ${JSON.stringify(utils.unscapeData(result2[0]))}`);
        return utils.unscapeData(result2[0]);
    } catch (err) {
        logger.error(`Error inserting/displaying records :: ${err}`);
        throw err;
    }
};

Table.prototype.update = async function (id, data) {
    try {
        logger.debug(`Updating row in DB :: ${id}`);
        logger.trace(`Data to create :: ${JSON.stringify(data)}`);

        if (!id) {
            throw new Error('No id provided to update record');
        }

        const stmt = utils.updateStatement(this.fields, data);
        if (!stmt) {
            throw new Error('data has no matching field to update');
        }
        let sql = `UPDATE ${this.table} ${stmt} WHERE _id IN (${id.split(',').map(i => `'${i}'`).join(',')})`;

        logger.trace(`SQL query for update :: ${sql}`);
        let result = await this.connection.query(sql);

        logger.debug(`Record updated successfully`);
        logger.trace(`Updated record details :: ${JSON.stringify(result[0].affectedRows)}`);
        return result[0].affectedRows;

    } catch (err) {
        logger.error(`Error updating record :: ${err}`);
        throw err;
    }
};

Table.prototype.delete = async function (id) {
    try {
        logger.debug(`Deleting record in DB :: ${id}`);

        if (!id) {
            throw new Error('No id provided to delete record');
        }
        let sql = `DELETE FROM ${this.table} WHERE _id='${id}'`;

        logger.trace(`SQL query for delete :: ${sql}`);

        let result = await this.connection.query(sql);

        logger.debug(`Record deleted successfully`);
        return result[0].affectedRows;

    } catch (err) {
        logger.error(`Error deleting record :: ${err}`);
        throw err;
    }
};

Table.prototype.deleteMany = async function (ids) {
    try {
        logger.debug(`Deleting multiple records from DB :: ${ids}`);

        if (!ids) {
            return reject(new Error('No id provided to delete record'));
        }
        let sql = `DELETE FROM ${this.table} WHERE _id IN (${ids.split(',').map(id => `'${id}'`).join(',')})`;

        logger.trace(`SQL query for deleting multiple records :: ${sql}`);
        let result = await this.connection.query(sql);

        logger.debug(`Records deleted successfully :: ${result[0].affectedRows}`);
        return result[0].affectedRows;

    } catch (err) {
        logger.error(`Error deleting multiple records :: ${err}`);
        throw err;
    }
};


module.exports = CRUD;
