const postgres = require('postgres');
let log4js = require('log4js');

const utils = require('../utils');
let version = require('../package.json').version;

const logLevel = process.env.LOG_LEVEL || 'trace';
const loggerName = process.env.HOSTNAME ? `[${process.env.DATA_STACK_NAMESPACE}] [${process.env.HOSTNAME}] [REST_CRUD PGSQL ${version}]` : `[REST_CRUD PGSQL ${version}]`;
log4js.configure({
    levels: { AUDIT: { value: Number.MAX_VALUE - 1, colour: 'yellow' } },
    appenders: { out: { type: 'stdout', layout: { type: 'basic' } } },
    categories: { default: { appenders: ['out'], level: logLevel.toUpperCase() } }
});
let logger = log4js.getLogger(loggerName);


/**
 * @param {object} options CRUD options
 */
function CRUD(options) {
    this.database = options.database;
    this.customId = options.customId || false;
    this.idPattern = options.idPattern || '';

    const connectionProps = {
        user: options.user,
        password: options.password,
        host: options.host,
        port: options.port,
        database: options.database,
        connectionString: options.connectionString,
        ssl: options.ssl,
        idle_in_transaction_session_timeout: options.idle_in_transaction_session_timeout,
        connectionTimeoutMillis: options.connectionTimeoutMillis,
        client_encoding: options.client_encoding,
        application_name: options.application_name,
        fallback_application_name: options.fallback_application_name,
    };

    this.connectionDetails = Object.fromEntries(
      Object.entries(connectionProps).filter(([_, v]) => v !== null && v !== undefined)
    );
}


/**
 * Connect using postgres library
 */
CRUD.prototype.connect = async function () {
    try {
        logger.debug('Connecting to PostgreSQL (postgres lib)');
        logger.trace(`Connection details :: ${JSON.stringify(this.connectionDetails)}`);

        // Create postgres client
        this.connection = postgres(
          this.connectionDetails.connectionString ||
          {
              host: this.connectionDetails.host,
              port: this.connectionDetails.port,
              username: this.connectionDetails.user,
              password: this.connectionDetails.password,
              database: this.connectionDetails.database,
              ssl: this.connectionDetails.ssl,
              idle_timeout: this.connectionDetails.idle_in_transaction_session_timeout,
              connect_timeout: this.connectionDetails.connectionTimeoutMillis
          }
        );

        // Test connection
        const result = await this.connection`SELECT 1 + 1 AS solution`;
        console.log('The solution is:', result[0].solution);
        console.log('Connection Successful!');

        return 'Connection Successful';
    } catch (err) {
        logger.error('Error connecting :: ', err);
        throw err;
    }
};


/**
 * Disconnect postgres
 */
CRUD.prototype.disconnect = async function () {
    try {
        await this.connection.end();
        console.log('Database Disconnected!');
        return 'Database Disconnected';
    } catch (err) {
        logger.error('Error disconnecting :: ', err);
        throw err;
    }
};


/**
 * Raw SQL query handler
 */
CRUD.prototype.sqlQuery = async function (sql, values) {
    try {
        logger.debug('Performing SQL Query');
        logger.trace(`SQL Query :: ${sql}`);

        if (!sql) throw new Error('No sql query provided.');

        const result = await this.connection.unsafe(sql, values);

        logger.trace(`Query result :: ${JSON.stringify(result[0])}`);
        return utils.unscapeData(result);

    } catch (err) {
        logger.error(`Error querying :: ${err}`);
        throw err;
    }
};


/**
 * Table wrapper
 */
CRUD.prototype.table = function (table, jsonSchema) {
    return new Table({
        table,
        database: this.database,
        customId: this.customId,
        idPattern: this.idPattern,
        connection: this.connection
    }, jsonSchema);
};



/******************* TABLE CLASS ********************/

function Table(options, jsonSchema) {
    this.database = options.database;
    this.customId = options.customId;
    this.idPattern = options.idPattern;
    this.table = options.table;
    this.connection = options.connection;
    this.fields = utils.getFieldsFromSchema(jsonSchema);
}


/**
 * Create table
 */
Table.prototype.createTable = async function () {
    try {
        logger.debug(`Creating Table :: ${this.table}`);

        const sql = utils.createTableStatement(this.fields);
        const query = `CREATE TABLE IF NOT EXISTS ${this.table}(${sql})`;

        logger.trace(`SQL :: ${query}`);

        await this.connection.unsafe(query);

        logger.debug('Table created successfully');
        return 'Table created';
    } catch (err) {
        logger.error(`Error creating table :: ${err}`);
        throw err;
    }
};


/**
 * Count rows
 */
Table.prototype.count = async function (filter) {
    try {
        logger.debug('Counting rows in DB');
        let sql = `SELECT count(*) AS count FROM ${this.table}`;

        const whereClause = utils.whereClause(this.fields, filter);
        if (whereClause) sql += whereClause;

        logger.trace(`SQL :: ${sql}`);

        const result = await this.connection.unsafe(sql);
        return result[0].count;

    } catch (err) {
        logger.error(`Error counting records :: ${err}`);
        throw err;
    }
};


/**
 * List rows
 */
Table.prototype.list = async function (options) {
    try {
        logger.debug('Listing rows from DB');

        const whereClause = options?.filter ? utils.whereClause(this.fields, options.filter) : '';
        const selectClause = utils.selectClause(this.fields, options?.select) || '*';
        const limitClause = utils.limitClause(options?.count, options?.page);
        const orderByClause = utils.orderByClause(this.fields, options?.sort);

        let sql = `SELECT ${selectClause} FROM ${this.table}`;
        if (whereClause) sql += whereClause;
        if (orderByClause) sql += orderByClause;
        if (limitClause) sql += limitClause;

        logger.trace(`SQL :: ${sql}`);

        const result = await this.connection.unsafe(sql);
        return utils.unscapeData(result);

    } catch (err) {
        logger.error(`Error listing records :: ${err}`);
        throw err;
    }
};


/**
 * Show a record
 */
Table.prototype.show = async function (id, options) {
    try {
        logger.debug(`Fetching record :: ${id}`);

        const selectClause = utils.selectClause(this.fields, options?.select) || '*';
        const sql = `SELECT ${selectClause} FROM ${this.table} WHERE _id='${id}'`;

        logger.trace(`SQL :: ${sql}`);

        const result = await this.connection.unsafe(sql);
        return utils.unscapeData(result[0]);

    } catch (err) {
        logger.error(`Error fetching record :: ${err}`);
        throw err;
    }
};


/**
 * Create records
 */
Table.prototype.create = async function (data) {
    try {
        logger.debug('Creating new rows');

        if (!data) throw new Error('No data to insert');
        if (!Array.isArray(data)) data = [data];

        data.forEach(obj => { if (!obj._id) obj._id = utils.token(); });

        const stmt = utils.insertManyStatement(this.fields, data);
        if (!stmt) throw new Error('No data to insert');

        const insertSQL = `INSERT INTO ${this.table} ${stmt}`;
        logger.trace(`SQL Insert :: ${insertSQL}`);

        await this.connection.unsafe(insertSQL);

        const selectSQL =
          `SELECT * FROM ${this.table} WHERE _id IN (${data.map(o => `'${o._id}'`).join(',')})`;

        logger.trace(`SQL Select :: ${selectSQL}`);

        const result = await this.connection.unsafe(selectSQL);
        return utils.unscapeData(result);

    } catch (err) {
        logger.error(`Error in create :: ${err}`);
        throw err;
    }
};


/**
 * Update
 */
Table.prototype.update = async function (id, data) {
    try {
        logger.debug(`Updating record :: ${id}`);
        if (!id) throw new Error('No id provided');

        const stmt = utils.updateStatement(this.fields, data);
        if (!stmt) throw new Error('Invalid update payload');

        const sql =
          `UPDATE ${this.table} ${stmt} WHERE _id IN (${id.split(',').map(i => `'${i}'`).join(',')})`;

        logger.trace(`SQL :: ${sql}`);

        const result = await this.connection.unsafe(sql);
        return result.length;   // number of affected rows

    } catch (err) {
        logger.error(`Error updating :: ${err}`);
        throw err;
    }
};


/**
 * Delete
 */
Table.prototype.delete = async function (id) {
    try {
        logger.debug(`Deleting record :: ${id}`);
        if (!id) throw new Error('No id provided');

        const sql = `DELETE FROM ${this.table} WHERE _id='${id}'`;
        logger.trace(`SQL :: ${sql}`);

        const result = await this.connection.unsafe(sql);
        return result.length;

    } catch (err) {
        logger.error(`Error deleting :: ${err}`);
        throw err;
    }
};


/**
 * Delete Many
 */
Table.prototype.deleteMany = async function (ids) {
    try {
        logger.debug(`Deleting multiple :: ${ids}`);
        if (!ids) throw new Error('No ids provided');

        const sql =
          `DELETE FROM ${this.table} WHERE _id IN (${ids.split(',').map(id => `'${id}'`).join(',')})`;

        logger.trace(`SQL :: ${sql}`);

        const result = await this.connection.unsafe(sql);
        return result.length;

    } catch (err) {
        logger.error(`Error deleting multiple :: ${err}`);
        throw err;
    }
};


module.exports = CRUD;