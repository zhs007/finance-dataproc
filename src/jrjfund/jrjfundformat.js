"use strict";

const util = require('util');
const moment = require('moment');
const { Task, log } = require('jarvis-task');
const { MysqlMgr } = require('../mysqlmgr');

const SQL_BATCH_NUMS = 2048;

async function delJRJFundFormat(dbid, ti, lst) {
    let conn = MysqlMgr.singleton.getMysqlConn(dbid);

    let fullsql = '';
    let sqlnums = 0;
    for (let ii = 0; ii < lst.length; ++ii) {
        let cf = lst[ii];

        let sql = util.format("delete from jrjfundformat_%d where code = '%s' and timed = '%s';", ti, cf.code, cf.timed);
        fullsql += sql;
        ++sqlnums;

        if (sqlnums >= SQL_BATCH_NUMS) {
            try{
                await conn.query(fullsql);
            }
            catch(err) {
                log('error', 'mysql err: ' + err);
                log('error', 'mysql sql: ' + fullsql);
            }

            fullsql = '';
            sqlnums = 0;
        }
    }

    if (sqlnums > 0) {
        try{
            await conn.query(fullsql);
        }
        catch(err) {
            log('error', 'mysql err: ' + err);
            log('error', 'mysql sql: ' + fullsql);
        }
    }
}

async function saveJRJFundFormat(dbid, ti, lst) {
    delJRJFundFormat(dbid, ti, lst);

    let conn = MysqlMgr.singleton.getMysqlConn(dbid);

    let fullsql = '';
    let sqlnums = 0;
    for (let ii = 0; ii < lst.length; ++ii) {
        let cf = lst[ii];
        let str0 = '';
        let str1 = '';

        let i = 0;
        for (let key in cf) {
            if (i != 0) {
                str0 += ', ';
                str1 += ', ';
            }

            str0 += '`' + key + '`';
            str1 += "'" + cf[key] + "'";

            ++i;
        }

        let sql = util.format("insert into jrjfundformat_%d(%s) values(%s);", ti, str0, str1);
        fullsql += sql;
        ++sqlnums;

        if (sqlnums >= SQL_BATCH_NUMS) {
            try{
                await conn.query(fullsql);
            }
            catch(err) {
                log('error', 'mysql err: ' + err);
                log('error', 'mysql sql: ' + fullsql);
            }

            fullsql = '';
            sqlnums = 0;
        }
    }

    if (sqlnums > 0) {
        try{
            await conn.query(fullsql);
        }
        catch(err) {
            log('error', 'mysql err: ' + err);
            log('error', 'mysql sql: ' + fullsql);
        }
    }
}

exports.saveJRJFundFormat = saveJRJFundFormat;