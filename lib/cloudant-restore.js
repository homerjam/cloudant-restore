var Cloudant = require('cloudant'),
    Promise = require("bluebird"),
    program = require('commander'),
    fs = Promise.promisifyAll(require("fs"));

program
    .version('0.0.1')
    .usage('[options] <file> <database>')
    .option('-u, --username <username>', 'Cloudant username')
    .option('-p, --password <password>', 'Cloudant password')
    .parse(process.argv);

if (program.args.length < 2) {
    program.help();
}

// Grab Cloudant username/password from .cloudant file or --username/--password args
var getCloudantCredentials = function() {
    return new Promise(function(resolve, reject) {
        if (program.username && program.password) {
            return resolve({
                username: program.username,
                password: program.password
            });
        }

        fs.readFileAsync('.cloudant').then(function(data) {
            resolve(JSON.parse(data));
        }, function(err) {
            switch (err.code) {
                case 'ENOENT':
                    reject(Error('.cloudant credentials not found'));
                    break;
            }
        });
    });
};

// Load backup
var loadBackup = function(creds) {
    return new Promise(function(resolve, reject) {
        fs.readFileAsync(program.args[0]).then(function(data) {
            resolve({
                creds: creds,
                backup: JSON.parse(data)
            });
        }, function(err) {
            switch (err.code) {
                case 'ENOENT':
                    reject(Error(program.args[0] + ' not found'));
                    break;
            }
        });
    });
};

var createDatabase = function(opts) {
    return new Promise(function(resolve, reject) {
        Cloudant({
            account: opts.creds.username,
            password: opts.creds.password
        }, function(err, cloudant) {
            if (err) {
                return reject(Error('Failed to connect to Cloudant using credentials ' + JSON.stringify(opts.creds)));
            }

            opts.db = cloudant.use(program.args[1]);

            cloudant.db.get(program.args[1], function(err, body) {
                if (!err && (body.doc_count > 0 || body.doc_del_count > 0)) {
                    return reject(Error('Database exists and is not empty'));
                }

                if (body && body.doc_count === 0 && body.doc_del_count === 0) {
                    return resolve(opts);
                }

                cloudant.db.create(program.args[1], function(err, body) {
                    if (err) {
                        return reject(Error('Couldn\'t create database'));
                    }


                    return resolve(opts);
                });
            });
        });
    });
};

// Restore docs in batches to database
var restoreDatabase = function(opts) {
    return new Promise(function(resolve, reject) {
        var docs = opts.backup.rows.map(function(row) {
            var doc = row.doc;
            delete doc._rev;
            return doc;
        });

        opts.db.bulk({
            docs: docs
        }, function(err, body) {
            if (err) {
                return reject(Error('Failed to restore database ' + err));
            }

            reject('Succesfully restored database');
        });
    });
};

// Perform task
getCloudantCredentials()
    .then(loadBackup)
    .then(createDatabase)
    .then(restoreDatabase)
    .then(function(result) {
        console.log(result);
    }, console.error);
