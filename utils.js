const parser = require('where-in-json');
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
    Object.keys(data).forEach(dataKey => {
        const temp = keyInFields(dataKey, fields);
        if (temp) {
            cols.push(temp.key);
            if (temp.type === 'TEXT' || temp.type === 'BLOB') {
                values.push(`'${escape(data[temp.key])}'`);
            } else {
                values.push(data[temp.key]);
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
        const temp = keyInFields(dataKey, fields);
        if (temp && dataKey !== '_id') {
            if (temp.type === 'TEXT' || temp.type === 'BLOB') {
                sets.push(`${temp.key}='${escape(data[temp.key])}'`);
            } else {
                sets.push(`${temp.key}=${data[temp.key]}`);
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
 * @param {any} data
 */
function updateStatement(fields, data) {
    const sets = [];
    Object.keys(data).forEach(dataKey => {
        const temp = keyInFields(dataKey, fields);
        if (temp && dataKey !== '_id') {
            if (temp.type === 'TEXT' || temp.type === 'BLOB') {
                sets.push(`${temp.key}='${escape(data[temp.key])}'`);
            } else {
                sets.push(`${temp.key}=${data[temp.key]}`);
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
        const temp = keyInFields(dataKey, fields);
        if (temp) {
            if (dataKey.startsWith('-')) {
                orderBy.push(`${temp.key} DESC`);
            } else {
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
    if (!filter) {
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
    Object.keys(jsonSchema.properties).forEach(key => {
        let dataKey = parentKey ? parentKey + '.' + key : key;
        if (jsonSchema.properties[key].type === 'object') {
            fields = fields.concat(getFieldsFromSchema(jsonSchema.properties[key], key));
        } else {
            fields.push({
                key: dataKey,
                type: jsonSchema.properties[key].type,
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
module.exports.unscapeData = unscapeData;
module.exports.getFieldsFromSchema = getFieldsFromSchema;