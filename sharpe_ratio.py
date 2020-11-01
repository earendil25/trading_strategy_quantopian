import quantopian.algorithm as algo

from quantopian.pipeline import Pipeline
from quantopian.pipeline import CustomFactor
from quantopian.pipeline import factors
from quantopian.pipeline.filters import QTradableStocksUS

from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.data import morningstar
import quantopian.optimize as opt

import numpy as np

def initialize(context):
    schedule_function(
        rebalance,
        date_rules.month_start(),
        time_rules.market_open()
    )

    my_pipe = make_pipeline()
    algo.attach_pipeline(my_pipe, 'pipeline')

class volatility(CustomFactor):
    
    inputs = [USEquityPricing.close] 
    outputs = ['sharpe']
    window_length = 252
 
    def compute(self, today, assets, out, close):        
        for i in range(len(assets)):
            p = close[:, i]
            r = np.diff(p)/p[1:]
            ret = np.mean(r)
            sigma = np.std(r)
            out.sharpe[i]=ret/sigma

def make_pipeline():
        
    base_universe = morningstar.Fundamentals.market_cap.latest.top(100, mask = QTradableStocksUS())
    vol = volatility(window_length=22).sharpe
    
    longs = vol.top(10, mask = base_universe)
    shorts = vol.bottom(10, mask = base_universe)
    
    sec = longs | shorts
    
    return Pipeline(
        columns={
            'long': longs,
            'short': shorts
        },screen = sec)


def before_trading_start(context, data):
    context.output = algo.pipeline_output('pipeline')
    context.longs = context.output[context.output['long']].index
    context.shorts = context.output[context.output['short']].index


def rebalance(context, data):
    # Calculate target weights to rebalance
    target_weights = compute_target_weights(context, data)

    # If we have target weights, rebalance our portfolio
    if target_weights:
        algo.order_optimal_portfolio(
            objective=opt.TargetWeights(target_weights),
            constraints=[],
        )

def compute_target_weights(context, data):
    weights = {}

    for security in context.longs:
        weights[security] = 1.0/len(context.longs)
        
    for security in context.shorts:
        weights[security] = 0/len(context.shorts)

    return weights
