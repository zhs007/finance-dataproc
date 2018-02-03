"use strict";

const util = require('util');
const moment = require('moment');
const { Task, log } = require('jarvis-task');
const { Strategy, Trader } = require('../strategy');
const { STRATEGY_TYPENAME_FUNDMA } = require('../strategydef');

// 思路
//  1. 2根均线，maval0短
//  2. 在 maval0 - maval1 反转时买入或卖出
class StrategyMA extends Strategy {
    // params:
    //      typecode - fundcn
    //      code - fund code
    //      money - money
    //      maval0 - min ma
    //      maval1 - max ma
    constructor(params, starttime) {
        super(STRATEGY_TYPENAME_FUNDMA, params, starttime);

        this.lastmaoff = 0;
    }

    onTick_CurData(curdata, lstHistory) {
        if (lstHistory.length == 0) {
            this.lastmaoff = 0;
        }

        if (curdata[this.params.maval0] != null && curdata[this.params.maval1] != null) {
            let maoff = curdata[this.params.maval0] - curdata[this.params.maval1];
            let curmaoff = (maoff > 0 ? 1 : (maoff < 0 ? -1 : 0));

            curdata.curmaoff = curmaoff;

            if (this.lastmaoff == 0 && curmaoff != 0) {
                this.lastmaoff = curmaoff;
            }
        }

        return curdata;
    }

    onTick(curdata, lstHistory) {
        if (curdata[this.params.maval0] != null && curdata[this.params.maval1] != null && curdata.hasOwnProperty('curmaoff')) {
            if (this.lastmaoff != 0) {
                if (this.lastmaoff != curdata.curmaoff) {
                    if (curdata.curmaoff >= 0 && this.lastmaoff < 0) {
                        this.buy(curdata.unit_net, this.countPosition(1, curdata.unit_net), moment(curdata.timed).format('YYYY-MM-DD'));
                    }
                }

                if (curdata.curmaoff != 0) {
                    this.lastmaoff = curdata.curmaoff;
                }
            }
        }
    }

    onTick_Trader(trader, curdata, lstHistory) {
        if (curdata.hasOwnProperty(this.params.maval0) && curdata.hasOwnProperty(this.params.maval1) && curdata.hasOwnProperty('curmaoff')) {
            if (trader.islong && curdata.curmaoff < 0 && this.lastmaoff != 0 && trader.buyprice != curdata.unit_net) {
                this.sellTrader(trader, curdata.unit_net, trader.curvolume, moment(curdata.timed).format('YYYY-MM-DD'));
            }
        }
    }

    onEnd_Trader(trader, curdata, lstHistory) {
        this.sellTrader(trader, curdata.unit_net, trader.curvolume, moment(curdata.timed).format('YYYY-MM-DD'));
    }

    onTick_Net(curdata, lstHistory) {
        this.lstNet.push({
            lastmoney: this.lastMoney,
            lastvolume: this.curVolume,
            net: this.lastMoney + this.curVolume * curdata.unit_net,
        });
    }
};

exports.StrategyMA = StrategyMA;