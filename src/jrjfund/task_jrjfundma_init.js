"use strict";

const util = require('util');
const moment = require('moment');
const { Task, log, formatTimeMs, formatPer } = require('jarvis-task');
const { taskFactory } = require('../taskfactory');
const { TASK_NAMEID_JRJFUND_MA_INIT } = require('../taskdef');
const { FinanceMgr } = require('../financemgr');
const { MysqlMgr } = require('../mysqlmgr');
// const { saveJRJFundMa } = require('./jrjfundma');

// const SQL_BATCH_NUMS = 1024;

class TaskJRJFundMA_Init extends Task {
    constructor(taskfactory, cfg) {
        super(taskfactory, TASK_NAMEID_JRJFUND_MA_INIT, cfg);
    }

    async loadJRJCodeList(ti) {
        let conn = MysqlMgr.singleton.getMysqlConn(this.cfg.maindb);

        let str = util.format("select distinct(code) from jrjfundformat_%d;", ti);
        let [rows, fields] = await conn.query(str);
        return rows;
    }

    // async saveJRJFundFormat(ti, lst) {
    //     let conn = MysqlMgr.singleton.getMysqlConn(this.cfg.maindb);
    //
    //     let fullsql = '';
    //     let sqlnums = 0;
    //     for (let ii = 0; ii < lst.length; ++ii) {
    //         let cf = lst[ii];
    //         let str0 = '';
    //         let str1 = '';
    //
    //         let i = 0;
    //         for (let key in cf) {
    //             if (key != 'accum_net' && key != 'unit_net') {
    //                 if (i != 0) {
    //                     str0 += ', ';
    //                     str1 += ', ';
    //                 }
    //
    //                 str0 += '`' + key + '`';
    //                 str1 += "'" + cf[key] + "'";
    //
    //                 ++i;
    //             }
    //         }
    //
    //         let sql = util.format("insert into jrjfundma_%d(%s) values(%s);", ti, str0, str1);
    //         fullsql += sql;
    //         ++sqlnums;
    //
    //         if (sqlnums >= SQL_BATCH_NUMS) {
    //             try{
    //                 await conn.query(fullsql);
    //             }
    //             catch(err) {
    //                 log('error', 'mysql err: ' + err);
    //                 log('error', 'mysql sql: ' + fullsql);
    //             }
    //
    //             fullsql = '';
    //             sqlnums = 0;
    //         }
    //     }
    //
    //     if (sqlnums > 0) {
    //         try{
    //             await conn.query(fullsql);
    //         }
    //         catch(err) {
    //             log('error', 'mysql err: ' + err);
    //             log('error', 'mysql sql: ' + fullsql);
    //         }
    //     }
    // }

    procMA(lst, mai) {
        for (let ii = mai - 1; ii < lst.length; ++ii) {
            let ta = 0;
            for (let jj = 0; jj < mai; ++jj) {
                ta += lst[ii - jj].accum_net;
            }

            lst[ii]['ma' + mai] = Math.floor(ta / mai);
        }
    }

    async procFormat(ti, code) {
        let conn = MysqlMgr.singleton.getMysqlConn(this.cfg.maindb);

        let str = util.format("select * from jrjfundformat_%d where code = '%s' order by timed asc;", ti, code);
        let [rows, fields] = await conn.query(str);
        if (rows.length > 0) {
            let map = {};
            let bt = moment(rows[0].timed);
            let et = moment(rows[rows.length - 1].timed);
            for (let ii = 0; ii < rows.length; ++ii) {
                map[moment(rows[ii].timed).format('YYYY-MM-DD')] = {
                    accum_net: rows[ii].accum_net,
                    unit_net: rows[ii].unit_net
                };
            }

            let lst = [];
            let last_accum_net = 0;
            let last_unit_net = 0;
            while (parseInt(bt.format('YYYYMMDD')) <= parseInt(et.format('YYYYMMDD'))) {
                let ct = bt.format('YYYY-MM-DD');
                if (!FinanceMgr.singleton.isDayOff(ct)) {
                    let accum_net = 0;
                    let unit_net = 0;

                    if (map.hasOwnProperty(ct)) {
                        accum_net = map[ct].accum_net;
                        unit_net = map[ct].unit_net;

                        last_accum_net = accum_net;
                        last_unit_net = unit_net;
                    }
                    else {
                        accum_net = last_accum_net;
                        unit_net = last_unit_net;
                    }

                    lst.push({
                        code: code,
                        timed: ct,
                        accum_net: accum_net,
                        unit_net: unit_net
                    });
                }

                bt.add(1, 'days')
            }

            for (let ii = 2; ii <= 50; ++ii) {
                this.procMA(lst, ii);
            }

            await FinanceMgr.singleton.saveJRJFundFactor('jrjfundma_' + ti, lst, 512);
            // await saveJRJFundMa(this.cfg.maindb, ti, lst);
            // await this.saveJRJFundFormat(ti, lst);
        }
    }

    onStart() {
        super.onStart();

        for (let dbcfgname in this.cfg.mysqlcfg) {
            MysqlMgr.singleton.addCfg(dbcfgname, this.cfg.mysqlcfg[dbcfgname]);
        }

        MysqlMgr.singleton.start().then(async () => {
            FinanceMgr.singleton.init(this.cfg.maindb);

            await FinanceMgr.singleton.loadDayOff();

            for (let ii = 0; ii < 10; ++ii) {
                let per = ii / 10;
                await FinanceMgr.singleton.createFundFactor('jrjfundma_' + ii, 'ma', 2, 50);
                let lst = await this.loadJRJCodeList(ii);
                for (let jj = 0; jj < lst.length; ++jj) {
                    await this.procFormat(ii, lst[jj].code);

                    per += (1 / 10 / lst.length);
                    this.taskStatistics.onPer(per * 100);
                    let strper = formatPer(this.taskStatistics.per);
                    let strlastms = formatTimeMs(this.taskStatistics.lasttimems);
                    let strcurms = formatTimeMs(new Date().getTime() - this.taskStatistics.starttimems);
                    log('info', 'per ' + strper + ' curms ' + strcurms + ' lasttime ' + strlastms);
                }
            }

            this.onEnd();
        });
    }
};

taskFactory.regTask(TASK_NAMEID_JRJFUND_MA_INIT, (taskfactory, cfg) => {
    return new TaskJRJFundMA_Init(taskfactory, cfg);
});

exports.TaskJRJFundMA_Init = TaskJRJFundMA_Init;