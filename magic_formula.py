  
from quantopian.algorithm import order_optimal_portfolio
from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline import Pipeline, CustomFactor, experimental
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.factors import SimpleMovingAverage
from quantopian.pipeline.filters import QTradableStocksUS, StaticAssets, StaticSids
import quantopian.optimize as opt

from quantopian.pipeline.data import morningstar, Fundamentals

def initialize(context):
    schedule_function(
        my_rebalance,
        date_rules.week_start(),
        time_rules.market_open()
    )

    my_pipe = make_pipeline()
    attach_pipeline(my_pipe, 'my_pipeline')
    
def make_pipeline():

    base_universe = QTradableStocksUS()

    value = -Fundamentals.pe_ratio.latest.rank()
    quality = Fundamentals.roe.latest.rank()
    
    longs = (value+quality).bottom(30,mask=base_universe)
    
    shorts = (value+quality).top(30,mask=base_universe)

    return Pipeline(
        columns={
            'longs':longs,
            'shorts': shorts
        },
        screen=(longs|shorts),
    )

def compute_target_weights(context, data):

    weights = {}

    for security in context.longs:
        weights[security] = 0.55/len(context.longs)

    for security in context.shorts:
        weights[security] = -0.45/len(context.shorts)

    return weights

def before_trading_start(context, data):
    pipe_results = pipeline_output('my_pipeline')
    
    context.longs = []
    
    for sec in pipe_results[pipe_results['longs']].index.tolist():
        if data.can_trade(sec):
            context.longs.append(sec)

    context.shorts = []
    for sec in pipe_results[pipe_results['shorts']].index.tolist():
        if data.can_trade(sec):
            context.shorts.append(sec)

def my_rebalance(context, data):
    target_weights = compute_target_weights(context, data)

    if target_weights:
        order_optimal_portfolio(
            objective=opt.TargetWeights(target_weights),
            constraints=[],
        )
