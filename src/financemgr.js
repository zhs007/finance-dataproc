"use strict";

const util = require('util');
const moment = require('moment');
const { log } = require('jarvis-task');
const { MysqlMgr } = require('./mysqlmgr');

const SQL_BATCH_NUMS = 2048;

class FinanceMgr {
    constructor() {
        this.mapDayOff = {};

        this.mysqlid = undefined;
    }

    init(mysqlid) {
        this.mysqlid = mysqlid;
    }

    async loadDayOff() {
        this.mapDayOff = {};

        let conn = MysqlMgr.singleton.getMysqlConn(this.mysqlid);

        let str = util.format("select * from dayoff");
        let [rows, fields] = await conn.query(str);
        for (let i = 0; i < rows.length; ++i) {
            let cd = moment(rows[i].dayoff).format('YYYY-MM-DD');

            this.mapDayOff[cd] = true;  // 表示数据库里有
        }
    }

    addDayOff(curday) {
        let cd = moment(curday).format('d');
        // 周末跳过
        if (cd == 0 || cd == 6) {
            return ;
        }

        this.mapDayOff[curday] = false;  // 表示数据库里还没有
    }

    async saveDayOff() {
        let conn = MysqlMgr.singleton.getMysqlConn(this.mysqlid);

        let fullsql = '';
        let sqlnums = 0;
        for (let curday in this.mapDayOff) {
            if (!this.mapDayOff[curday]) {
                let sql = util.format("insert into dayoff(dayoff) values('%s');", curday);
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
};

FinanceMgr.singleton = new FinanceMgr();

exports.FinanceMgr = FinanceMgr;