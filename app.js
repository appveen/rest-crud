const log4js = require('log4js');
const express = require('express');
const CRUD = require('./index').mysql;

const jsonSchema = require('./sample.json');

log4js.configure({
    appenders: { out: { type: 'stdout', layout: { type: 'basic' } } },
    categories: { default: { appenders: ['out'], level: 'info' } }
});


const logger = log4js.getLogger('Server');
const app = express();

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

(async () => {
    try {
        const crud = new CRUD({
            host: 'localhost',
            user: 'root',
            password: 'itworks@123123123',
            database: 'test'
        });

        const apis = crud.table('employee', jsonSchema);
        app.get('/data', async (req, res) => {
            try {
                let filter = {};
                try {
                    if (req.query.filter) {
                        filter = JSON.parse(req.query.filter);
                    }
                } catch (err) {
                    logger.error(err);
                    return res.status(400).json({
                        message: err
                    });
                }
                if (req.query.countOnly) {
                    const count = await apis.count(filter);
                    return res.status(200).json(count);
                }
                let skip = 0;
                let count = 30;
                let select = '';
                let sort = '';
                if (req.query.count && (+req.query.count) > 0) {
                    count = +req.query.count;
                }
                if (req.query.page && (+req.query.page) > 0) {
                    skip = count * ((+req.query.page) - 1);
                }
                if (req.query.select && req.query.select.trim()) {
                    select = req.query.select;
                }
                if (req.query.sort && req.query.sort.trim()) {
                    sort = req.query.sort;
                }
                const docs = await apis.list({
                    page,
                    count,
                    select,
                    sort,
                    filter
                });
                res.status(200).json(docs);
            } catch (err) {
                logger.error(err);
                res.status(500).json({
                    message: err.message
                });
            }
        });

        app.get('/data/:id', async (req, res) => {
            try {
                let doc = await apis.show(req.params.id);
                if (!doc) {
                    return res.status(404).json({
                        message: 'Data Model Not Found'
                    });
                }
                res.status(200).json(doc);
            } catch (err) {
                logger.error(err);
                res.status(500).json({
                    message: err.message
                });
            }
        });

        app.post('/data/', async (req, res) => {
            try {
                const payload = req.body;
                const status = await apis.create(payload);
                res.status(200).json(status);
            } catch (err) {
                logger.error(err);
                res.status(500).json({
                    message: err.message
                });
            }
        });

        app.put('/data/:id', async (req, res) => {
            try {
                const payload = req.body;
                const status = await apis.update(req.params.id, payload);
                res.status(200).json(status);
            } catch (err) {
                logger.error(err);
                res.status(500).json({
                    message: err.message
                });
            }
        });

        app.delete('/data/:id', async (req, res) => {
            try {
                const status = await apis.delete(req.params.id);
                res.status(200).json({
                    message: 'Document Deleted'
                });
            } catch (err) {
                logger.error(err);
                res.status(500).json({
                    message: err.message
                });
            }
        });

        app.listen(3000, () => {
            logger.info('Server is listening on port:', 3000);
        });
    } catch (err) {
        console.log(err);
    }
})();



