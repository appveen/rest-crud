const mssql = require('mssql');
const utils = require('../utils');

/**
 * @param {object} options CRUD options
 * @param {string} options.host
 * @param {string} options.user
 * @param {string} options.password
 * @param {string} options.database
 * @param {boolean} options.customId
 * @param {string} options.idPattern
 */
async function CRUD(options) {
    this.database = options.database;
    this.customId = options.customId || false;
    this.idPattern = options.idPattern || '';
    this.connection = await mssql.connect({
        server: options.host,
        user: options.user,
        password: options.password,
        database: options.database
    });
    this.connection.query('SELECT 1 + 1 AS solution', function (error, results, fields) {
        if (error) throw error;
        console.log('The solution is: ', results[0].solution);
        console.log('Connection Successfull!');
    });
}

CRUD.prototype.disconnect = function () {
    this.connection.end();
    console.log('Database Disconnected!');
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
    let sql = utils.createTableStatement(this.fields);
    this.connection.query(`CREATE TABLE IF NOT EXISTS ${this.table}(${sql})`, function (error, results, fields) {
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
            this.connection.query(sql, function (error, results, fields) {
                if (error) {
                    return reject(error);
                };
                resolve(results[0].count);
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
            this.connection.query(sql, function (error, results, fields) {
                if (error) {
                    return reject(error);
                };
                resolve(utils.unscapeData(results));
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
            this.connection.query(sql, function (error, results, fields) {
                if (error) {
                    return reject(error);
                };
                resolve(utils.unscapeData(results[0]));
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
            this.connection.query(sql1, function (error, results, fields) {
                if (error) {
                    return reject(error);
                };
                let sql2 = `SELECT * FROM ${this.table} WHERE _id='${data._id}'`;
                this.connection.query(sql2, function (error, results, fields) {
                    if (error) {
                        return reject(error);
                    };
                    resolve(utils.unscapeData(results[0]));
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
            this.connection.query(sql, function (error, results, fields) {
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
            this.connection.query(sql, function (error, results, fields) {
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