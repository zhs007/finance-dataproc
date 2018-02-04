"use strict";

const util = require('util');
const moment = require('moment');
const { Task, log, formatTimeMs, formatPer } = require('jarvis-task');
const { taskFactory } = require('../taskfactory');
const { TASK_NAMEID_STRATEGY_FUNDCN } = require('../taskdef');
const { FinanceMgr } = require('../financemgr');
const { MysqlMgr } = require('../mysqlmgr');
const { StrategyFactory } = require('../strategyfactory');
// const { StrategyMA } = require('./strategyma');

class TaskStrategyFund extends Task {
    constructor(taskfactory, cfg) {
        super(taskfactory, TASK_NAMEID_STRATEGY_FUNDCN, cfg);
    }

    async loadData(code, bt, et) {
        let conn = MysqlMgr.singleton.getMysqlConn(this.cfg.maindb);
        let ti = parseInt(code.charAt(5));

        let str = util.format("select * from jrjfundformat_%d inner join jrjfundma_%d on " +
            "jrjfundformat_%d.code=jrjfundma_%d.code and jrjfundformat_%d.timed=jrjfundma_%d.timed " +
            "where jrjfundformat_%d.code = '%s' and jrjfundformat_%d.timed >= '%s' and jrjfundformat_%d.timed <= '%s' " +
            "order by jrjfundformat_%d.timed asc;", ti, ti, ti, ti, ti, ti, ti, code, ti, bt, ti, et, ti);
        let [rows, fields] = await conn.query(str);
        return rows;
    }

    onStart() {
        super.onStart();

        for (let dbcfgname in this.cfg.mysqlcfg) {
            MysqlMgr.singleton.addCfg(dbcfgname, this.cfg.mysqlcfg[dbcfgname]);
        }

        MysqlMgr.singleton.start().then(async () => {
            FinanceMgr.singleton.init(this.cfg.maindb);

            let strategy = StrategyFactory.singleton.newStrategy(this.cfg.typename, this.cfg);

            let lstData = await this.loadData(this.cfg.code, this.cfg.begintime, this.cfg.endtime);
            strategy.runSimulation(lstData);

            let conn = MysqlMgr.singleton.getMysqlConn(this.cfg.maindb);
            await strategy.saveDB(conn);
            log('info', JSON.stringify(strategy.result));

            this.onEnd();
        });
    }
};

taskFactory.regTask(TASK_NAMEID_STRATEGY_FUNDCN, (taskfactory, cfg) => {
    return new TaskStrategyFund(taskfactory, cfg);
});

exports.TaskStrategyFund = TaskStrategyFund;