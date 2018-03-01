"use strict";

const util = require('util');
const moment = require('moment');
const { Task, log, formatTimeMs, formatPer } = require('jarvis-task');
const { taskFactory } = require('../taskfactory');
const { TASK_NAMEID_JRJFUND_RSI } = require('../taskdef');
const { FinanceMgr } = require('../financemgr');
const { MysqlMgr } = require('../mysqlmgr');
// const { saveJRJFundMa } = require('./jrjfundma');

// const SQL_BATCH_NUMS = 1024;

class TaskJRJFundRSI extends Task {
    constructor(taskfactory, cfg) {
        super(taskfactory, TASK_NAMEID_JRJFUND_RSI, cfg);
    }

    async loadJRJCodeList(ti) {
        let conn = MysqlMgr.singleton.getMysqlConn(this.cfg.maindb);

        let str = util.format("select distinct(code) from jrjfundformat_%d;", ti);
        let [rows, fields] = await conn.query(str);
        return rows;
    }

    procRSI(lst, mai) {
        for (let ii = mai - 1; ii < lst.length; ++ii) {
            let ap = 0;
            let as = 0;

            // let bi = ii - (mai - 1);
            // let bv = lst[bi].accum_net;

            for (let jj = 0; jj < mai - 1; ++jj) {
                // let ci = ii - (mai - 1 - jj);

                let ca = lst[ii - jj - 1].accum_net - lst[ii - jj].accum_net;
                if (ca < 0) {
                    as += Math.abs(ca);
                }
                else {
                    ap += ca;
                }
            }

            ap /= (mai - 1);
            as /= (mai - 1);

            lst[ii]['rsi' + mai] = Math.floor(ap / (ap + as) * 10000);
        }
    }

    async procFormat(ti, code, mst, mbt, met) {
        let conn = MysqlMgr.singleton.getMysqlConn(this.cfg.maindb);

        let str = util.format("select * from jrjfundformat_%d where code = '%s' and timed >= '%s' and timed <= '%s' order by timed asc;", ti, code, mst, met);
        let [rows, fields] = await conn.query(str);
        // log('info', 'select ' + rows.length);
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

            for (let ii = 3; ii <= 50; ++ii) {
                this.procRSI(lst, ii);
            }

            let lst1 = [];
            for (let ii = 0; ii < lst.length; ++ii) {
                if (moment(lst[ii].timed).isBetween(mbt, met, null, '[]')) {
                    lst1.push(lst[ii]);
                }
            }

            await FinanceMgr.singleton.saveJRJFundFactor2('jrjfundrsi_' + ti, lst1, 512);
            // log('info', 'saveJRJFundMa ' + code);
            // await saveJRJFundMa(this.cfg.maindb, ti, lst1);
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

            let et = moment().format('YYYY-MM-DD');
            let bt = FinanceMgr.singleton.subDays_DayOff(et, this.cfg.daynums);
            let st = FinanceMgr.singleton.subDays_DayOff(et, this.cfg.daynums + 60);

            for (let ii = 0; ii < 10; ++ii) {
                let per = ii / 10;
                // await FinanceMgr.singleton.createFundFactor('jrjfundma_' + ii, 'ma', 2, 50);
                let lst = await this.loadJRJCodeList(ii);
                for (let jj = 0; jj < lst.length; ++jj) {
                    await this.procFormat(ii, lst[jj].code, st, bt, et);

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

taskFactory.regTask(TASK_NAMEID_JRJFUND_RSI, (taskfactory, cfg) => {
    return new TaskJRJFundRSI(taskfactory, cfg);
});

exports.TaskJRJFundRSI = TaskJRJFundRSI;