"use strict";

const util = require('util');
const moment = require('moment');
const { Task, log } = require('jarvis-task');
const { taskFactory } = require('../taskfactory');
const { TASK_NAMEID_JRJFUND_MA_INIT } = require('../taskdef');
const { FinanceMgr } = require('../financemgr');
const { MysqlMgr } = require('../mysqlmgr');

class TaskJRJFundMA_Init extends Task {
    constructor(taskfactory, cfg) {
        super(taskfactory, TASK_NAMEID_JRJFUND_MA_INIT, cfg);
    }

    findStartDayInYear(y) {
        let sd = y + '-01-01';
        for (let cd = moment(sd); cd.format('YYYY') == y; cd.add(1, 'days')) {
            if (this.mapBusinessDay.hasOwnProperty(cd.format('YYYY-MM-DD'))) {
                return cd.format('YYYY-MM-DD');
            }
        }

        return undefined;
    }

    procCurYear(y) {
        let sd = y + '-01-01';
        for (let cd = moment(sd); cd.format('YYYY') == y; cd.add(1, 'days')) {
            if (!this.mapBusinessDay.hasOwnProperty(cd.format('YYYY-MM-DD'))) {
                FinanceMgr.singleton.addDayOff(cd.format('YYYY-MM-DD'));
            }
        }
    }

    procDayOff() {
        let by = moment(this.minday, 'YYYY-MM-DD').format('YYYY');
        for (; by <= this.cfg.endyear; ++by) {
            let sd = this.findStartDayInYear(by);
            if (sd != undefined) {
                log('info', 'startday is ' + sd);

                if (moment(sd).isBefore(by + '-01-06')) {
                    this.procCurYear(by);
                }
            }
        }
    }

    async loadSinaDay() {
        let conn = MysqlMgr.singleton.getMysqlConn(this.cfg.maindb);

        for (let ii = 0; ii < 10; ++ii) {
            let str = util.format("select distinct(timed) from sinastockprice_d_%d order by timed;", ii);
            let [rows, fields] = await conn.query(str);
            for (let i = 0; i < rows.length; ++i) {
                let cd = moment(rows[i].timed).format('YYYY-MM-DD');

                this.mapBusinessDay[cd] = true;

                if (cd > this.maxday) {
                    this.maxday = cd;
                }

                if (cd < this.minday) {
                    this.minday = cd;
                }
            }
        }
    }

    async loadJRJDay() {
        let conn = MysqlMgr.singleton.getMysqlConn(this.cfg.maindb);

        for (let ii = 0; ii < 10; ++ii) {
            let str = util.format("select distinct(enddate) from jrjfundnet_%d order by enddate;", ii);
            let [rows, fields] = await conn.query(str);
            for (let i = 0; i < rows.length; ++i) {
                let cd = moment(rows[i].enddate).format('YYYY-MM-DD');

                this.mapBusinessDay[cd] = true;

                if (cd > this.maxday) {
                    this.maxday = cd;
                }

                if (cd < this.minday) {
                    this.minday = cd;
                }
            }
        }
    }

    onStart() {
        super.onStart();

        for (let dbcfgname in this.cfg.mysqlcfg) {
            MysqlMgr.singleton.addCfg(dbcfgname, this.cfg.mysqlcfg[dbcfgname]);
        }

        MysqlMgr.singleton.start().then(async () => {
            FinanceMgr.singleton.init(this.cfg.maindb);

            await FinanceMgr.singleton.createFundFactor('jrjfundma_0', 'ma', 2, 50);
            await FinanceMgr.singleton.createFundFactor('jrjfundma_1', 'ma', 2, 50);
            await FinanceMgr.singleton.createFundFactor('jrjfundma_2', 'ma', 2, 50);
            await FinanceMgr.singleton.createFundFactor('jrjfundma_3', 'ma', 2, 50);
            await FinanceMgr.singleton.createFundFactor('jrjfundma_4', 'ma', 2, 50);
            await FinanceMgr.singleton.createFundFactor('jrjfundma_5', 'ma', 2, 50);
            await FinanceMgr.singleton.createFundFactor('jrjfundma_6', 'ma', 2, 50);
            await FinanceMgr.singleton.createFundFactor('jrjfundma_7', 'ma', 2, 50);
            await FinanceMgr.singleton.createFundFactor('jrjfundma_8', 'ma', 2, 50);
            await FinanceMgr.singleton.createFundFactor('jrjfundma_9', 'ma', 2, 50);

            this.onEnd();
        });
    }
};

taskFactory.regTask(TASK_NAMEID_JRJFUND_MA_INIT, (taskfactory, cfg) => {
    return new TaskJRJFundMA_Init(taskfactory, cfg);
});

exports.TaskJRJFundMA_Init = TaskJRJFundMA_Init;