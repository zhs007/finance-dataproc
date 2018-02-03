"use strict";

const util = require('util');
const moment = require('moment');
const { log } = require('jarvis-task');

// 暂不考虑可以做空的交易
class Trader {
    constructor(typecode, code, islong) {
        this.typecode = typecode;
        this.code = code;
        this.islong = islong;

        this.issuccess = false;

        this.buytime = undefined;
        this.buyprice = 0;
        this.buyvolume = 0;

        this.selltime = undefined;
        this.sellprice = 0;
        this.sellvolume = 0;

        this.winper = 0;
        this.isend = false;

        this.curvolume = 0;
    }

    buy(price, volume, curtime) {
        this.buytime = curtime;
        this.buyprice = price;
        this.buyvolume = volume;

        this.curvolume += volume;

        if (this.curvolume == 0) {
            this.onEnd();
        }

        return true;
    }

    sell(price, volume, curtime) {
        if (this.curvolume < volume) {
            return false;
        }

        this.selltime = curtime;
        this.sellprice = price;
        this.sellvolume = volume;

        this.curvolume -= volume;

        if (this.curvolume == 0) {
            this.onEnd();
        }

        if (this.sellprice > this.buyprice) {
            this.issuccess = true;
        }

        return true;
    }

    onEnd() {
        this.isend = true;

        if (this.islong) {
            let ret = this.sellprice - this.buyprice;
            this.winper = Math.floor(ret * 10000 / this.buyprice);
        }
        else {
            let ret = this.buyprice - this.sellprice;
            this.winper = Math.floor(ret * 10000 / this.sellprice);
        }
    }
}

class Strategy {
    constructor(typename, params, starttime) {
        this.typecode = params.typecode;   // 交易大类
        this.code = params.code;           // 交易大类下的编号

        this.typename = typename;
        this.params = params;
        this.starttime = starttime;

        this.poolMoney = params.money;
        this.lastMoney = params.money;
        this.curVolume = 0;

        // 交易队列
        this.lstTrader = [];
        // 净值
        this.lstNet = [];
        // 历史数据
        this.lstHistory = [];

        this.result = {
            inMoney: 0,                 // 总投入
            outMoney: 0,                // 总回报
            winPer: 0,                  // 回报比
            days: 0,                    // 交易天数
            traders: 0,                 // 交易次数
            traderWinPer: 0,            // 交易胜率

            maxDrawdown: 0,             // 最大回撤
            annualizedReturns: 0,       // 年化
            benchmarkReturns: 0,        // 参考年化
            alpha: 0,                   // alpha
            beta: 0,                    // beta
            sharpeRatio: 0,             // 夏普比率
            volatility: 0,              // 波动率
            informationRatio: 0         // 信息比率
        };
    }

    resetResult() {
        this.result.inMoney = 0;
        this.result.outMoney = 0;
        this.result.winPer = 0;
        this.result.days = 0;
        this.result.traders = 0;
        this.result.traderWinPer = 0;

        this.result.maxDrawdown = 0;
        this.result.annualizedReturns = 0;
        this.result.benchmarkReturns = 0;
        this.result.alpha = 0;
        this.result.beta = 0;
        this.result.sharpeRatio = 0;
        this.result.volatility = 0;
        this.result.informationRatio = 0;
    }

    runSimulation(lstData) {
        this.resetResult();

        this.lstTrader = [];
        this.lstHistory = [];
        this.lstNet = [];

        this.lastMoney = this.poolMoney;
        this.curVolume = 0;

        for (let ii = 0; ii < lstData.length; ++ii) {
            let curdata = lstData[ii];

            // 先处理curdata，免得下面重复运算
            curdata = this.onTick_CurData(curdata, this.lstHistory);

            // 先处理已有的trader
            this.onTick_TraderList(curdata, this.lstHistory);
            this.onTick(curdata, this.lstHistory);

            if (ii == lstData.length - 1) {
                this.onEnd_TraderList(curdata, this.lstHistory);
            }

            this.onTick_Net(curdata, this.lstHistory);

            this.lstHistory.push(curdata);
        }

        this.countResult();
    }

    buy(curprice, curvolume, curtime) {
        let cm = curprice * curvolume;
        if (cm > this.lastMoney) {
            return false;
        }

        let curtrader = new Trader(this.typecode, this.code, true);
        if (curtrader.buy(curprice, curvolume, curtime)) {
            this.lstTrader.push(curtrader);

            this.lastMoney -= cm;
            this.curVolume += curvolume;

            return true;
        }

        return false;
    }

    sellTrader(trader, curprice, curvolume, curtime) {
        if (curvolume > this.curVolume) {
            return false;
        }

        if (trader.sell(curprice, curvolume, curtime)) {
            let cm = curprice * curvolume;

            this.lastMoney += cm;
            this.curVolume -= curvolume;

            return true;
        }

        return false;
    }

    // 根据当前剩余现金决定购买量，整数
    // per is [0, 1]
    countPosition(per, curprice) {
        return Math.floor(this.lastMoney * per / curprice);
    }

    onTick_TraderList(curdata, lstHistory) {
        for (let ii = 0; ii < this.lstTrader.length; ++ii) {
            let trader = this.lstTrader[ii];
            if (!trader.isend) {
                this.onTick_Trader(trader, curdata, lstHistory);
            }
        }
    }

    onEnd_TraderList(curdata, lstHistory) {
        for (let ii = 0; ii < this.lstTrader.length; ++ii) {
            let trader = this.lstTrader[ii];
            if (!trader.isend) {
                this.onEnd_Trader(trader, curdata, lstHistory);
            }
        }
    }

    countResult() {
        this.result.inMoney = this.poolMoney;
        this.result.outMoney = this.lastMoney;
        this.result.winPer = (this.result.outMoney / this.result.inMoney).toFixed(4);
        this.result.days = this.lstNet.length;
        this.result.traders = this.lstTrader.length;

        let wtnums = 0;
        for (let ii = 0; ii < this.lstTrader.length; ++ii) {
            if (this.lstTrader[ii].issuccess) {
                wtnums++;
            }
        }
        this.result.traderWinPer = (wtnums / this.result.traders).toFixed(4);

        this.result.maxDrawdown = 0;
        this.result.annualizedReturns = ((this.result.winPer - 1) / this.result.days * 250).toFixed(4);
        this.result.benchmarkReturns = 0;
        this.result.alpha = 0;
        this.result.beta = 0;
        this.result.sharpeRatio = 0;
        this.result.volatility = 0;
        this.result.informationRatio = 0;
    }

    onTick(curdata, lstHistory) {
    }

    onTick_Net(curdata, lstHistory) {
    }

    onTick_Trader(trader, curdata, lstHistory) {
    }

    onTick_CurData(curdata, lstHistory) {
        return curdata;
    }

    onEnd_Trader(trader, curdata, lstHistory) {
    }
};

exports.Strategy = Strategy;
exports.Trader = Trader;