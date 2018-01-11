"use strict";

const { Task } = require('jarvis-task');
const { taskFactory } = require('../taskfactory');
const { TASK_NAMEID_INITDAYOFF } = require('../taskdef');
const { FinanceMgr } = require('../financemgr');
const { MysqlMgr } = require('../mysqlmgr');

class TaskInitDayOff extends Task {
    constructor(taskfactory, cfg) {
        super(taskfactory, TASK_NAMEID_INITDAYOFF, cfg);

        this.mapBusinessDay = {};
    }

    onStart() {
        super.onStart();

        FinanceMgr.singleton.init(this.cfg.maindb);

        FinanceMgr.singleton.loadJRJFund().then(() => {
            startTotalFundCrawler(async (crawler) => {
                let lstfund = FinanceMgr.singleton.getNewJRJFund();
                if (lstfund.length > 0) {
                    startNewFundArchCrawler(lstfund, async () => {});
                }
            });

            CrawlerMgr.singleton.start(true, false, async () => {
                await FinanceMgr.singleton.saveJRJFund();

                this.onEnd();
            }, true);
        });
    }
};

taskFactory.regTask(TASK_NAMEID_INITDAYOFF, (taskfactory, cfg) => {
    return new TaskInitDayOff(taskfactory, cfg);
});

exports.TaskInitDayOff = TaskInitDayOff;