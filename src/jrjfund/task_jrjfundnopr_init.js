"use strict";

const util = require('util');
const moment = require('moment');
const { Task, log, formatTimeMs, formatPer } = require('jarvis-task');
const { taskFactory } = require('../taskfactory');
const { TASK_NAMEID_JRJFUND_NOPR_INIT } = require('../taskdef');
const { FinanceMgr } = require('../financemgr');
const { MysqlMgr } = require('../mysqlmgr');
const { saveJRJFundNopr } = require('./jrjfundnopr');

class TaskJRJFundNOPR_Init extends Task {
    constructor(taskfactory, cfg) {
        super(taskfactory, TASK_NAMEID_JRJFUND_NOPR_INIT, cfg);
    }

    async loadJRJDayMap() {
        let conn = MysqlMgr.singleton.getMysqlConn(this.cfg.maindb);

        let mapDay = {};
        let daynums = 0;

        for (let ti = 0; ti < 10; ++ti) {
            let str = util.format("select distinct(timed) from jrjfundnop_%d;", ti);
            let [rows, fields] = await conn.query(str);
            for (let ii = 0; ii < rows.length; ++ii) {
                let ymd = moment(rows[ii].timed).format('YYYY-MM-DD');
                if (!mapDay.hasOwnProperty(ymd)) {
                    mapDay[ymd] = ymd;
                    ++daynums;
                }
            }
        }

        return {
            mapDay: mapDay,
            daynums: daynums
        };
    }

    async procDayRank(ymd) {
        let conn = MysqlMgr.singleton.getMysqlConn(this.cfg.maindb);

        let lstFund = [];
        let mapFund2 = {};

        for (let ti = 0; ti < 10; ++ti) {
            let str = util.format("select * from jrjfundnop_%d where timed = '%s';", ti, ymd);
            let [rows, fields] = await conn.query(str);
            for (let ii = 0; ii < rows.length; ++ii) {
                lstFund.push(rows[ii]);
                mapFund2[rows[ii].code] = {
                    code: rows[ii].code,
                    timed: ymd
                };
            }
        }

        for (let ii = 2; ii <= 50; ++ii) {
            lstFund.sort((a, b) => {
                if (a['nop' + ii] == null && b['nop' + ii] == null) {
                    return 0;
                }

                if (a['nop' + ii] == null) {
                    return 1;
                }

                if (b['nop' + ii] == null) {
                    return -1;
                }

                if (a['nop' + ii] > b['nop' + ii]) {
                    return -1;
                }
                else if (a['nop' + ii] < b['nop' + ii]) {
                    return 1;
                }

                return 0;
            });

            for (let jj = 0; jj < lstFund.length; ++jj) {
                if (lstFund[jj]['nop' + ii] != null) {
                    mapFund2[lstFund[jj].code]['nopr' + ii] = jj + 1;
                }
            }
        }

        let clst = [];
        for (let ti = 0; ti < 10; ++ti) {
            clst.push([]);
        }

        for (let code in mapFund2) {
            clst[parseInt(code.charAt(5))].push(mapFund2[code]);
        }

        for (let ti = 0; ti < 10; ++ti) {
            await saveJRJFundNopr(this.cfg.maindb, ti, clst[ti]);
        }
    }

    // procMA(lst, mai) {
    //     for (let ii = mai - 1; ii < lst.length; ++ii) {
    //         let ta = lst[ii - (mai - 1)].accum_net;
    //         let tb = lst[ii].accum_net;
    //
    //         lst[ii]['nop' + mai] = Math.floor((tb - ta) / ta * 10000);
    //     }
    // }
    //
    // async procFormat(ti, code) {
    //     let conn = MysqlMgr.singleton.getMysqlConn(this.cfg.maindb);
    //
    //     let str = util.format("select * from jrjfundformat_%d where code = '%s' order by timed asc;", ti, code);
    //     let [rows, fields] = await conn.query(str);
    //     if (rows.length > 0) {
    //         let map = {};
    //         let bt = moment(rows[0].timed);
    //         let et = moment(rows[rows.length - 1].timed);
    //         for (let ii = 0; ii < rows.length; ++ii) {
    //             map[moment(rows[ii].timed).format('YYYY-MM-DD')] = {
    //                 accum_net: rows[ii].accum_net,
    //                 unit_net: rows[ii].unit_net
    //             };
    //         }
    //
    //         let lst = [];
    //         let last_accum_net = 0;
    //         let last_unit_net = 0;
    //         while (parseInt(bt.format('YYYYMMDD')) <= parseInt(et.format('YYYYMMDD'))) {
    //             let ct = bt.format('YYYY-MM-DD');
    //             if (!FinanceMgr.singleton.isDayOff(ct)) {
    //                 let accum_net = 0;
    //                 let unit_net = 0;
    //
    //                 if (map.hasOwnProperty(ct)) {
    //                     accum_net = map[ct].accum_net;
    //                     unit_net = map[ct].unit_net;
    //
    //                     last_accum_net = accum_net;
    //                     last_unit_net = unit_net;
    //                 }
    //                 else {
    //                     accum_net = last_accum_net;
    //                     unit_net = last_unit_net;
    //                 }
    //
    //                 lst.push({
    //                     code: code,
    //                     timed: ct,
    //                     accum_net: accum_net,
    //                     unit_net: unit_net
    //                 });
    //             }
    //
    //             bt.add(1, 'days')
    //         }
    //
    //         for (let ii = 2; ii <= 50; ++ii) {
    //             this.procMA(lst, ii);
    //         }
    //
    //         await saveJRJFundNop(this.cfg.maindb, ti, lst);
    //         // await this.saveJRJFundFormat(ti, lst);
    //     }
    // }

    onStart() {
        super.onStart();

        for (let dbcfgname in this.cfg.mysqlcfg) {
            MysqlMgr.singleton.addCfg(dbcfgname, this.cfg.mysqlcfg[dbcfgname]);
        }

        MysqlMgr.singleton.start().then(async () => {
            FinanceMgr.singleton.init(this.cfg.maindb);

            await FinanceMgr.singleton.loadDayOff();

            for (let ii = 0; ii < 10; ++ii) {
                await FinanceMgr.singleton.createFundFactor('jrjfundnopr_' + ii, 'nopr', 2, 50);
            }

            let ret = await this.loadJRJDayMap();
            let per = 0;
            for (let ymd in ret.mapDay) {
                await this.procDayRank(ymd);

                per += (1 / ret.daynums);
                this.taskStatistics.onPer(per * 100);
                let strper = formatPer(this.taskStatistics.per);
                let strlastms = formatTimeMs(this.taskStatistics.lasttimems);
                let strcurms = formatTimeMs(new Date().getTime() - this.taskStatistics.starttimems);
                log('info', 'per ' + strper + ' curms ' + strcurms + ' lasttime ' + strlastms);
            }

            // for (let ii = 0; ii < 10; ++ii) {
            //     let per = ii / 10;
            //     let lst = await this.loadJRJCodeList(ii);
            //     for (let jj = 0; jj < lst.length; ++jj) {
            //         await this.procFormat(ii, lst[jj].code);
            //
            //         per += (1 / 10 / lst.length);
            //         this.taskStatistics.onPer(per * 100);
            //         let strper = formatPer(this.taskStatistics.per);
            //         let strlastms = formatTimeMs(this.taskStatistics.lasttimems);
            //         let strcurms = formatTimeMs(new Date().getTime() - this.taskStatistics.starttimems);
            //         log('info', 'per ' + strper + ' curms ' + strcurms + ' lasttime ' + strlastms);
            //     }
            // }

            this.onEnd();
        });
    }
};

taskFactory.regTask(TASK_NAMEID_JRJFUND_NOPR_INIT, (taskfactory, cfg) => {
    return new TaskJRJFundNOPR_Init(taskfactory, cfg);
});

exports.TaskJRJFundNOPR_Init = TaskJRJFundNOPR_Init;