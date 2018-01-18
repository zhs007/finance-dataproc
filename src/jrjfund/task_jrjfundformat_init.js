"use strict";

const util = require('util');
const moment = require('moment');
const { Task, log, formatTimeMs, formatPer } = require('jarvis-task');
const { taskFactory } = require('../taskfactory');
const { TASK_NAMEID_JRJFUND_FORMAT_INIT } = require('../taskdef');
const { FinanceMgr } = require('../financemgr');
const { MysqlMgr } = require('../mysqlmgr');
const { saveJRJFundFormat } = require('./jrjfundformat');

class TaskJRJFundFormat_Init extends Task {
    constructor(taskfactory, cfg) {
        super(taskfactory, TASK_NAMEID_JRJFUND_FORMAT_INIT, cfg);
    }

    async loadJRJCodeList(ti) {
        let conn = MysqlMgr.singleton.getMysqlConn(this.cfg.maindb);

        let str = util.format("select distinct(fundcode) from jrjfundnet_%d;", ti);
        let [rows, fields] = await conn.query(str);
        return rows;
    }

    async procFormat(ti, code) {
        let conn = MysqlMgr.singleton.getMysqlConn(this.cfg.maindb);

        let str = util.format("select * from jrjfundnet_%d where fundcode = '%s' order by enddate asc;", ti, code);
        let [rows, fields] = await conn.query(str);
        if (rows.length > 0) {
            let map = {};
            let bt = moment(rows[0].enddate);
            let et = moment(rows[rows.length - 1].enddate);
            for (let ii = 0; ii < rows.length; ++ii) {
                map[moment(rows[ii].enddate).format('YYYY-MM-DD')] = {
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

                    if (accum_net == 0 && unit_net > 0) {
                        accum_net = unit_net;
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

            await saveJRJFundFormat(this.cfg.maindb, ti, lst);
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
                await FinanceMgr.singleton.createFundFormat('jrjfundformat_' + ii);
                let lst = await this.loadJRJCodeList(ii);
                for (let jj = 0; jj < lst.length; ++jj) {
                    await this.procFormat(ii, lst[jj].fundcode);

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

taskFactory.regTask(TASK_NAMEID_JRJFUND_FORMAT_INIT, (taskfactory, cfg) => {
    return new TaskJRJFundFormat_Init(taskfactory, cfg);
});

exports.TaskJRJFundFormat_Init = TaskJRJFundFormat_Init;