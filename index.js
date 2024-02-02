
module.exports = {
    mysql: require('./drivers/mysql'),
    pgsql: require('./drivers/pg'),
    mssql: require('./drivers/mssql'),
    oracledb: require('./drivers/oracledb')
};
