const {createLogger, format, transports} = require('winston');
const _ = require('lodash');
const fs = require('fs');
const path = require('path');

const logDir = 'logs';

// Create the log directory if it does not exist
if (!fs.existsSync(logDir)) {
    fs.mkdirSync(logDir);
}

const filename = path.join(logDir, 'connector.log');

// instantiate a new Winston Logger with the settings defined above
const logger = createLogger({
    level: 'info',
    // format: format.json(),
    format: format.combine(
        format.timestamp({
            format: 'YYYY-MM-DD HH:mm:ss'
        }),
        format.printf(
            info => `${info.timestamp} ${info.level}: ${info.message}`
        )
    ),
    transports: [
        new transports.Console({
            level: 'info',
            format: format.combine(
                format.colorize(),
                format.printf(
                    info => `${info.timestamp} ${info.level}: ${info.message}`
                )
            )
        }),
        new transports.File({filename})
    ],
    exitOnError: false, // do not exit on handled exceptions
});

// create a stream object with a 'write' function that will be used by `morgan`
logger.stream = {
    write: function (message, encoding) {
        logger.info(message);
    },
};

module.exports = logger;
