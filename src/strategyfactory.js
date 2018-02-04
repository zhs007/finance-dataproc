"use strict";

class StrategyFactory {

    constructor() {
        this.mapStrategy = {};
    }

    regStrategy(typename, funcNew) {
        this.mapStrategy[typename] = {
            funcNew: funcNew
        };
    }

    newStrategy(typename, params) {
        if (this.mapStrategy.hasOwnProperty(typename)) {
            return this.mapStrategy[typename].funcNew(params);
        }

        return undefined;
    }
};

StrategyFactory.singleton = new StrategyFactory();

exports.StrategyFactory = StrategyFactory;
