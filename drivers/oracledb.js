const oracledb = require('oracledb');
const log4js = require('log4js');
const utils = require('../utils');
const version = require('../package.json').version;

const logLevel = process.env.LOG_LEVEL || 'trace';
const loggerName = process.env.HOSTNAME ? `[${process.env.DATA_STACK_NAMESPACE}] [${process.env.HOSTNAME}] [REST_CRUD ORACLE ${version}]` : `[REST_CRUD ORACLE ${version}]`;

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
    this.connection = oracledb.getConnection({
        connectString: options.host,
        user: options.user,
        password: options.password
    }).then((conn) => {
        this.connection = conn;
        this.connection.execute('SELECT 1 + 1 AS solution', function (error, results, fields) {
            if (error) throw error;
            console.log('The solution is: ', results.rows[0].solution);
            console.log('Connection Successfull!');
        });
    });
}

CRUD.prototype.disconnect = function () {
    this.connection.end();
    console.log('Database Disconnected!');
};

CRUD.prototype.sqlQuery = async function (sql, values) {
    if (!sql) {
        logger.error('No SQL query provided.');
        throw new Error('No SQL query provided.');
    }

    try {
        logger.debug(`Performing SQL Query`);
        logger.trace(`SQL Query :: ${sql}`);

        const result = await this.connection.execute(sql, values);

        logger.trace(`Query result :: ${JSON.stringify(result)}`);
        return result;
    } catch (err) {
        logger.error(`Error querying :: ${err}`);
        throw err;
    }
}

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
    let sql = utils.createTableStatement(this.fields);
    this.connection.execute(`CREATE TABLE IF NOT EXISTS ${this.table}(${sql})`, function (error, results, fields) {
        if (error) {
            throw error;
        };
        console.log(results);
    });
}

Table.prototype.count = function (filter) {
    return new Promise((resolve, reject) => {
        try {
            const whereClause = utils.whereClause(this.fields, filter);
            let sql = `SELECT count(*) AS count FROM ${this.table}`;
            if (whereClause && whereClause.trim()) {
                sql += whereClause;
            }
            this.connection.execute(sql, function (error, results, fields) {
                if (error) {
                    return reject(error);
                };
                resolve(results.rows[0].count);
            });
        } catch (err) {
            reject(err);
        }
    });
};


Table.prototype.list = function (options) {
    return new Promise((resolve, reject) => {
        try {
            const selectClause = utils.selectClause(this.fields, options.select) || '*';
            const whereClause = utils.whereClause(this.fields, options.filter);
            const limitClause = utils.limitClause(options.count, options.page);
            const orderByClause = utils.orderByClause(this.fields, options.sort);
            let sql = `SELECT ${selectClause} FROM ${this.table}`;
            if (whereClause) {
                sql += whereClause;
            }
            if (limitClause) {
                sql += limitClause;
            }
            if (orderByClause) {
                sql += orderByClause;
            }
            this.connection.execute(sql, function (error, results, fields) {
                if (error) {
                    return reject(error);
                };
                resolve(utils.unscapeData(results.rows));
            });
        } catch (err) {
            reject(err);
        }
    });
};


Table.prototype.show = function (id) {
    return new Promise((resolve, reject) => {
        try {
            const selectClause = utils.selectClause(this.fields, options.select) || '*';
            let sql = `SELECT ${selectClause} FROM ${this.table} WHERE _id='${id}'`;
            this.connection.execute(sql, function (error, results, fields) {
                if (error) {
                    return reject(error);
                };
                resolve(utils.unscapeData(results.rows[0]));
            });
        } catch (err) {
            reject(err);
        }
    });
};

Table.prototype.create = function (data) {
    return new Promise((resolve, reject) => {
        try {
            if (!data._id) {
                data._id = uniqueToken.token();
            }
            const stmt = utils.insertStatement(this.fields, data);
            if (!stmt) {
                return reject(new Error('No data to insert'));
            }
            let sql1 = `INSERT INTO ${this.table} ${stmt}`;
            this.connection.execute(sql1, function (error, results, fields) {
                if (error) {
                    return reject(error);
                };
                let sql2 = `SELECT * FROM ${this.table} WHERE _id='${data._id}'`;
                this.connection.execute(sql2, function (error, results, fields) {
                    if (error) {
                        return reject(error);
                    };
                    resolve(utils.unscapeData(results.rows[0]));
                });
            });
        } catch (err) {
            reject(err);
        }
    });
};

Table.prototype.update = function (id, data) {
    return new Promise((resolve, reject) => {
        try {
            const stmt = utils.updateStatement(this.fields, data);
            if (!id) {
                return reject(new Error('No id provided to update record'));
            }
            if (!stmt) {
                return reject(new Error('data has no matching field to update'));
            }
            let sql = `UPDATE ${this.table} ${stmt} WHERE _id='${id}'`;
            this.connection.execute(sql, function (error, results, fields) {
                if (error) {
                    return reject(error);
                };
                resolve(results);
            });
        } catch (err) {
            reject(err);
        }
    });
};

Table.prototype.delete = function (id) {
    return new Promise((resolve, reject) => {
        try {
            if (!id) {
                return reject(new Error('No id provided to delete record'));
            }
            let sql = `DELETE FROM ${this.table} WHERE _id='${id}'`;
            this.connection.execute(sql, function (error, results, fields) {
                if (error) {
                    return reject(error);
                };
                resolve(results);
            });
        } catch (err) {
            reject(err);
        }
    });
};

module.exports = CRUD;