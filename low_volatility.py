import quantopian.algorithm as algo

from quantopian.pipeline import Pipeline
from quantopian.pipeline import CustomFactor
from quantopian.pipeline import factors
from quantopian.pipeline.filters import QTradableStocksUS

from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.data import morningstar
import quantopian.optimize as opt

def initialize(context):
    schedule_function(
        rebalance,
        date_rules.month_start(),
        time_rules.market_open()
    )

    my_pipe = make_pipeline()
    algo.attach_pipeline(my_pipe, 'pipeline')


def make_pipeline():
        
    base_universe = morningstar.Fundamentals.market_cap.latest.top(100, mask = QTradableStocksUS())
    vol = factors.AnnualizedVolatility()
    
    sec = vol.bottom(25, mask = base_universe)
    
    return Pipeline(
        columns={
            'long': sec
        },screen = sec)


def before_trading_start(context, data):
    context.output = algo.pipeline_output('pipeline')
    context.security_list = context.output.index


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

    for security in context.security_list:
        weights[security] = 1.0/len(context.security_list)

    return weights
