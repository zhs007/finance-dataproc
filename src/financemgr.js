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
            let cd = moment(rows[ii].dayoff).format('YYYYMMDD');
            
            this.mapDayOff[cd] = true;  // 表示数据库里有
        }
    }
};

FinanceMgr.singleton = new FinanceMgr();

exports.FinanceMgr = FinanceMgr;