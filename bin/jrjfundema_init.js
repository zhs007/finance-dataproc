"use strict";

const fs = require('fs');
const util = require('util');
const moment = require('moment');
const process = require('process');
const { startTask, initDailyRotateFileLog, log } = require('jarvis-task');
const { taskFactory } = require('../src/taskfactory');
require('../src/alltask');

initDailyRotateFileLog(util.format('./log/jrjfund_ema_init_%d.log', moment().format('x')), 'info');
// log('info', 'haha');

const cfg = JSON.parse(fs.readFileSync('./jrjfund_ema_init.json').toString());

startTask(cfg, taskFactory, () => {
    process.exit(0);
});