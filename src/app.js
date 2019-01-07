const {generateUid} = require('./uid');
const moment = require('moment');
const XLSX = require('xlsx');
const rq = require('request-promise');
const _ = require('lodash');
const fs = require("fs");
const URL = require('url').URL;
const isReachable = require('is-reachable');
const winston = require('./winston');
const scheduler = require('node-schedule');

const mapping = require('./mapping.json');

let {
    username,
    password,
    url,
    schedule,
    scheduleTime,
    dataType,
    dataUsername,
    dataPassword,
    dataURL,
    queryFile,
    minimum
} = require('./options');


const dhis2 = new URL(url);

dhis2.username = username;
dhis2.password = password;


const baseUrl = dhis2.toString() + 'api/';

const TRACKED_ENTITY_URL = baseUrl + 'trackedEntityInstances';
const EVENT_URL = baseUrl + 'events';
const ENROLLMENT_URL = baseUrl + 'enrollments';