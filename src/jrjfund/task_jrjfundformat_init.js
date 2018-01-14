"use strict";

const util = require('util');
const moment = require('moment');
const { Task, log } = require('jarvis-task');
const { taskFactory } = require('../taskfactory');
const { TASK_NAMEID_JRJFUND_FORMAT_INIT } = require('../taskdef');
const { FinanceMgr } = require('../financemgr');
const { MysqlMgr } = require('../mysqlmgr');

const SQL_BATCH_NUMS = 2048;

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

    async saveJRJFundFormat(ti, lst) {
        let conn = MysqlMgr.singleton.getMysqlConn(this.cfg.maindb);

        let fullsql = '';
        let sqlnums = 0;
        for (let ii = 0; ii < lst.length; ++ii) {
            let cf = lst[ii];
            let str0 = '';
            let str1 = '';

            let i = 0;
            for (let key in cf) {
                if (i != 0) {
                    str0 += ', ';
                    str1 += ', ';
                }

                str0 += '`' + key + '`';
                str1 += "'" + cf[key] + "'";

                ++i;
            }

            let sql = util.format("insert into jrjfundformat_%d(%s) values(%s);", ti, str0, str1);
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

                    lst.push({
                        code: code,
                        timed: ct,
                        accum_net: accum_net,
                        unit_net: unit_net
                    });
                }

                bt.add(1, 'days')
            }

            await this.saveJRJFundFormat(ti, lst);
        }
    }

    onStart() {
        super.onStart();

        for (let dbcfgname in this.cfg.mysqlcfg) {
            MysqlMgr.singleton.addCfg(dbcfgname, this.cfg.mysqlcfg[dbcfgname]);
        }

        MysqlMgr.singleton.start().then(async () => {
            FinanceMgr.singleton.init(this.cfg.maindb);

            for (let ii = 0; ii < 10; ++ii) {
                await FinanceMgr.singleton.createFundFormat('jrjfundformat_' + ii);
                let lst = await this.loadJRJCodeList(ii);
                for (let jj = 0; jj < lst.length; ++jj) {
                    await this.procFormat(ii, lst[jj].fundcode);
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