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

    isDayOff(curday) {
        let cd = moment(curday).format('d');
        // 周末
        if (cd == 0 || cd == 6) {
            return true;
        }

        return this.mapDayOff.hasOwnProperty(curday);
    }

    subDays_DayOff(curday, days) {
        let mt = moment(curday);

        while (days >= 0) {
            if (!this.isDayOff(mt)) {
                --days;
            }

            mt = mt.subtract(1, 'days');
        }

        return mt.format('YYYY-MM-DD');
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
        return ;

        let conn = MysqlMgr.singleton.getMysqlConn(this.mysqlid);

        let fullsql = '';
        let sqlnums = 0;
        for (let curday in this.mapDayOff) {
            if (!this.mapDayOff[curday]) {
                let sql = util.format("insert into dayoff_jrj(dayoff) values('%s');", curday);
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

    async createFundFormat(tablename) {
        let conn = MysqlMgr.singleton.getMysqlConn(this.mysqlid);

        let dsql = "DROP TABLE " + tablename + ";";

        try{
            await conn.query(dsql);
        }
        catch(err) {
            log('error', 'mysql err: ' + err);
            log('error', 'mysql sql: ' + dsql);
        }

        let sql = "CREATE TABLE `" + tablename + "` (\n" +
            "  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,\n" +
            "  `code` char(6) NOT NULL,\n" +
            "  `timed` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',\n" +
            "  `accum_net` int(11) DEFAULT NULL,\n" +
            "  `unit_net` int(11) DEFAULT NULL,\n" +
            "  PRIMARY KEY (`id`),\n" +
            "  UNIQUE KEY `codetimed` (`code`,`timed`),\n" +
            "  KEY `code` (`code`),\n" +
            "  KEY `timed` (`timed`)\n" +
            ") ENGINE=MyISAM DEFAULT CHARSET=utf8;";
        try{
            await conn.query(sql);
        }
        catch(err) {
            log('error', 'mysql err: ' + err);
            log('error', 'mysql sql: ' + sql);
        }
    }

    async createFundFactor(tablename, factorname, factorstart, factorend) {
        let conn = MysqlMgr.singleton.getMysqlConn(this.mysqlid);

        let dsql = "DROP TABLE " + tablename + ";";

        try{
            await conn.query(dsql);
        }
        catch(err) {
            log('error', 'mysql err: ' + err);
            log('error', 'mysql sql: ' + dsql);
        }

        let sql0 = "CREATE TABLE `" + tablename + "` (\n" +
            "  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,\n" +
            "  `code` char(6) NOT NULL,\n" +
            "  `timed` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',";
        let sql1 = "  PRIMARY KEY (`id`),\n" +
            "  UNIQUE KEY `codetimed` (`code`,`timed`),\n" +
            "  KEY `code` (`code`),\n" +
            "  KEY `timed` (`timed`)\n" +
            ") ENGINE=MyISAM DEFAULT CHARSET=utf8;";

        let sql2 = '';
        for (let ii = factorstart; ii <= factorend; ++ii) {
            sql2 += "  `" + factorname + ii + "` int(11) DEFAULT NULL,";
        }

        let sql = sql0 + sql2 + sql1;
        try{
            await conn.query(sql);
        }
        catch(err) {
            log('error', 'mysql err: ' + err);
            log('error', 'mysql sql: ' + sql);
        }
    }

    async delJRJFundFactor(tablename, lst, batchnums) {
        let conn = MysqlMgr.singleton.getMysqlConn(this.mysqlid);

        let fullsql = '';
        let sqlnums = 0;
        for (let ii = 0; ii < lst.length; ++ii) {
            let cf = lst[ii];

            let sql = util.format("delete from %s where code = '%s' and timed = '%s';", tablename, cf.code, cf.timed);
            fullsql += sql;
            ++sqlnums;

            if (sqlnums >= batchnums) {
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

    async saveJRJFundFactor(tablename, lst, batchnums) {
        this.delJRJFundFactor(tablename, lst, batchnums);

        let conn = MysqlMgr.singleton.getMysqlConn(this.mysqlid);

        let fullsql = '';
        let sqlnums = 0;
        for (let ii = 0; ii < lst.length; ++ii) {
            let cf = lst[ii];
            let str0 = '';
            let str1 = '';

            let i = 0;
            for (let key in cf) {
                if (key != 'accum_net' && key != 'unit_net') {
                    if (i != 0) {
                        str0 += ', ';
                        str1 += ', ';
                    }

                    str0 += '`' + key + '`';
                    str1 += "'" + cf[key] + "'";

                    ++i;
                }
            }

            let sql = util.format("insert into %s(%s) values(%s);", tablename, str0, str1);
            fullsql += sql;
            ++sqlnums;

            if (sqlnums >= batchnums) {
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
};

FinanceMgr.singleton = new FinanceMgr();

exports.FinanceMgr = FinanceMgr;