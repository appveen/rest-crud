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
function CRUD(options) {
    this.database = options.database;
    this.customId = options.customId || false;
    this.idPattern = options.idPattern || '';
    this.connectionString = options.connectionString;
}


CRUD.prototype.connect = async function () {
    try {
        this.connection = await mssql.connect(this.connectionString)

        let result = await this.connection.query('SELECT 1 + 1 AS solution');

        console.log('The solution is: ', result.recordset[0].solution);
        console.log('Connection Successfull!');
    } catch (err) {
        console.log('Error Connecting :: ', err);
        throw err;
    }
}

CRUD.prototype.disconnect = function () {
    this.connection.end();
    console.log('Database Disconnected!');
};

CRUD.prototype.sqlQuery = function (sql) {
    return new Promise((resolve, reject) => {
        try {
            if (!sql) {
                return reject(new Error('No sql query provided.'));
            }
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
        let result = await this.connection.query(`SELECT * FROM sysobjects WHERE name='${this.table}' and xtype='U'`);
        console.log(`Table exists? :: ${result.recordset.length > 0 ? 'true' : 'false'}`);
    
        if (result.recordset.length <= 0) {
            let sql = utils.createTableStatement(this.fields);
            let tableResult = await this.connection.query(`CREATE TABLE ${this.table}(${sql})`);
            console.log('Table created :: ', tableResult);
        }
    } catch (err) {
        throw err;
    }
    
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
                resolve(results.recordset[0].count);
            });
        } catch (err) {
            reject(err);
        }
    });
};

Table.prototype.list = function (options) {
    return new Promise((resolve, reject) => {
        try {
            const selectClause = utils.selectClause(this.fields, options?.select) || '*';
            const whereClause = utils.whereClause(this.fields, options?.filter);
            let limitClause, orderByClause;
            if (options?.sort) {
                limitClause = utils.limitClauseMS(options?.count, options?.page);
                orderByClause = utils.orderByClause(this.fields, options?.sort);
            }
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
            this.connection.query(sql, function (error, results, fields) {
                if (error) {
                    return reject(error);
                };
                resolve(utils.unscapeData(results.recordset));
            });
        } catch (err) {
            reject(err);
        }
    });
};

Table.prototype.show = function (id, options) {
    return new Promise((resolve, reject) => {
        try {
            const selectClause = utils.selectClause(this.fields, options?.select) || '*';
            let sql = `SELECT ${selectClause} FROM ${this.table} WHERE _id='${id}'`;
            this.connection.query(sql, function (error, results, fields) {
                if (error) {
                    return reject(error);
                };
                resolve(utils.unscapeData(results.recordset[0]));
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
                resolve(results.rowsAffected[0]);
            });
        } catch (err) {
            reject(err);
        }
    });
};

Table.prototype.deleteMany = function (ids) {
    return new Promise((resolve, reject) => {
        try {
            if (!ids) {
                return reject(new Error('No id provided to delete record'));
            }
            let sql = `DELETE FROM ${this.table} WHERE _id IN (`;
            ids = ids.split(',');
            ids.forEach((id, i) => {
                sql += `'${id}'`
                if (i !== ids.length - 1) {
                    sql += ','
                }
            });
            sql += ')';
            this.connection.query(sql, function (error, results, fields) {
                if (error) {
                    return reject(error);
                };
                resolve(results.rowsAffected[0]);
            });
        } catch (err) {
            reject(err);
        }
    });
};


module.exports = CRUD;
