const crypto = require('crypto');
const parser = require('where-in-json');
const _ = require('lodash');


function token() {
    const x = Math.random();
    const y = Date.now();
    return crypto.createHash('RSA-SHA256').update(y + '$' + x).digest('hex').toUpperCase();
}


/**
 * 
 * @param {string} key 
 * @param {Array<{key:string,type:('TEXT'|'NUMBER'|'DOUBLE'|'BLOB'),primaryKey:boolean,unique:boolean,required:boolean}>} fields 
 */
function keyInFields(key, fields) {
    return fields.find(e => e.key === key);
}


/**
 * 
 * @param {Array<{key:string,type:('TEXT'|'NUMBER'|'DOUBLE'|'BLOB'),primaryKey:boolean,unique:boolean,required:boolean}>} fields 
 */
function createTableStatement(fields) {
    const temp = [];
    fields.forEach(field => {
        let str = '';
        str += field.key + ' ' + field.type;
        if (field.primaryKey) {
            str += ' PRIMARY KEY'
        } else if (field.unique) {
            str += ' UNIQUE'
        }
        if (field.required) {
            str += ' NOT NULL'
        }
        temp.push(str);
    });
    return temp.join(', ');
}


/**
 * 
 * @param {Array<{key:string,type:('TEXT'|'NUMBER'|'DOUBLE'|'BLOB'),primaryKey:boolean,unique:boolean,required:boolean}>} fields 
 * @param {any} data
 */
function insertStatement(fields, data) {
    const cols = [];
    const values = [];
    fields.forEach(item => {
        const key = item.key.split('___').join('.');
        const val = _.get(data, key);
        if (val) {
            cols.push(item.key);
            if (item.type === 'TEXT' || item.type.startsWith('VARCHAR') || item.type === 'BLOB') {
                values.push(`'${escape(val)}'`);
            } else {
                values.push(val);
            }
        }
    });
    if (values.length > 1) {
        return `(${cols.join(', ')}) VALUES(${values.join(', ')})`;
    }
    return null;
}


/**
 * 
 * @param {Array<{key:string,type:('TEXT'|'NUMBER'|'DOUBLE'|'BLOB'),primaryKey:boolean,unique:boolean,required:boolean}>} fields 
 * @param {any} data
 */
function updateStatement(fields, data) {
    const sets = [];
    Object.keys(data).forEach(dataKey => {
        let key = dataKey.split('.').join('___');
        const temp = keyInFields(key, fields);
        if (temp && key !== '_id') {
            if (temp.type === 'TEXT' || temp.type === 'VARCHAR(64)' || temp.type === 'BLOB') {
                sets.push(`${temp.key}='${escape(data[dataKey])}'`);
            } else {
                sets.push(`${temp.key}=${data[dataKey]}`);
            }
        }
    });
    if (sets.length > 0) {
        return 'SET ' + sets.join(', ');
    }
    return null;
}


/**
 * 
 * @param {Array<{key:string,type:('TEXT'|'NUMBER'|'DOUBLE'|'BLOB'),primaryKey:boolean,unique:boolean,required:boolean}>} fields 
 * @param {string} select
 */
function selectClause(fields, select) {
    if (!select) {
        return null;
    }
    const cols = select.split(',');
    const keys = [];
    cols.forEach(dataKey => {
        const temp = keyInFields(dataKey, fields);
        if (temp) {
            keys.push(temp.key);
        }
    });
    if (keys.length > 0) {
        return keys.join(', ');
    }
    return null;
}


/**
 * 
 * @param {Array<{key:string,type:('TEXT'|'NUMBER'|'DOUBLE'|'BLOB'),primaryKey:boolean,unique:boolean,required:boolean}>} fields 
 * @param {string} sort 
 */
function orderByClause(fields, sort) {
    if (!sort) {
        return null;
    }
    const cols = sort.split(',');
    const orderBy = [];
    cols.forEach(dataKey => {
        if (dataKey.startsWith('-')) {
            const temp = keyInFields(dataKey.split('-')[1], fields);
            if (temp) {
                orderBy.push(`${temp.key} DESC`);
            }
        } else {
            const temp = keyInFields(dataKey, fields);
            if (temp) {
                orderBy.push(`${temp.key} ASC`);
            }
        }
    });
    if (orderBy.length > 0) {
        return ' ORDER BY ' + orderBy.join(', ');
    }
    return null;
}


/**
 * 
 * @param {Array<{key:string,type:('TEXT'|'NUMBER'|'DOUBLE'|'BLOB'),primaryKey:boolean,unique:boolean,required:boolean}>} fields 
 * @param {*} filter 
 */
function whereClause(fields, filter) {
    if (!filter || _.isEmpty(filter)) {
        return null;
    }
    if (typeof filter === 'string') {
        filter = JSON.parse(filter);
    }

    return ' WHERE ' + parser.toWhereClause(filter);
}


/**
 * 
 * @param {number} count 
 * @param {number} page 
 */
function limitClause(count, page) {
    if (count == -1) {
        return null;
    }
    if (!count) {
        count = 30;
    }
    if (!page) {
        page = 1;
    }
    return ` LIMIT ${count} OFFSET ${(page - 1) * count}`;
}


/**
 * 
 * @param {number} count 
 * @param {number} page 
 */
function limitClauseMS(count, page) {
    if (count == -1) {
        return null;
    }
    if (!count) {
        count = 30;
    }
    if (!page) {
        page = 1;
    }
    return ` OFFSET ${(page - 1) * count} ROWS FETCH FIRST ${count} ROWS ONLY`;
}


/**
 * 
 * @param {*} data 
 */
function unscapeData(data) {
    if (Array.isArray(data)) {
        data.forEach(row => {
            Object.keys(row).forEach(key => row[key] = unescape(row[key]));
        });
    } else {
        Object.keys(data).forEach(key => data[key] = unescape(data[key]));
    }
    return data;
}


function getFieldsFromSchema(jsonSchema, parentKey) {
    let fields = [];
    if (!parentKey) {
        fields.push({
            key: '_id',
            type: 'VARCHAR(64)',
            primaryKey: true
        });
    }
    const typeMap = {
        string: 'VARCHAR(64)',
        number: 'FLOAT',
        boolean: 'BOOLEAN'
    }
    Object.keys(jsonSchema.properties).forEach(key => {
        let dataKey = parentKey ? parentKey + '___' + key : key;
        if (jsonSchema.properties[key].type === 'object') {
            fields = fields.concat(getFieldsFromSchema(jsonSchema.properties[key], key));
        } else {
            fields.push({
                key: dataKey,
                type: typeMap[jsonSchema.properties[key].type],
                primaryKey: false,
                unique: jsonSchema.properties[key].unique || false,
                required: jsonSchema.required.indexOf(key) > -1,
            });
        }
    });
    return fields;
}


module.exports.createTableStatement = createTableStatement;
module.exports.insertStatement = insertStatement;
module.exports.updateStatement = updateStatement;
module.exports.selectClause = selectClause;
module.exports.orderByClause = orderByClause;
module.exports.whereClause = whereClause;
module.exports.limitClause = limitClause;
module.exports.limitClauseMS = limitClauseMS;
module.exports.unscapeData = unscapeData;
module.exports.getFieldsFromSchema = getFieldsFromSchema;
module.exports.token = token;
