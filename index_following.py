from quantopian.algorithm import order_optimal_portfolio
from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline import Pipeline
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.factors import SimpleMovingAverage
from quantopian.pipeline.filters import QTradableStocksUS
import quantopian.optimize as opt
 
from quantopian.pipeline.data import morningstar
 
def initialize(context):
    # Schedule our rebalance function to run at the start of
    # each week, when the market opens.
    schedule_function(
        my_rebalance,
        date_rules.week_start(),
        time_rules.market_open()
    )
 
    # Create our pipeline and attach it to our algorithm.
    my_pipe = make_pipeline()
    attach_pipeline(my_pipe, 'my_pipeline')
 
def make_pipeline():
    """
    Create our pipeline.
    """
 
    # Base universe set to the QTradableStocksUS.
    base_universe = QTradableStocksUS()
 
    Market_cap = morningstar.valuation.market_cap.latest
 
    # Filter to select securities to short.
    shorts = Market_cap.bottom(0)
 
    # Filter to select securities to long.
    longs = Market_cap.top(5,mask=base_universe)
 
    # Filter for all securities that we want to trade.
    securities_to_trade = (shorts | longs) & base_universe
 
    return Pipeline(
        columns={
            'longs': longs,
            'shorts': shorts,
            'cap': Market_cap,
        },
        screen=(securities_to_trade),
    )
 
def compute_target_weights(context, data):
    """
    Compute ordering weights.
    """
 
    # Initialize empty target weights dictionary.
    # This will map securities to their target weight.
    weights = {}
 
    for security in context.longs:
        weights[security] = context.ratio.loc[security]
 
    for security in context.shorts:
        weights[security] = 0
 
    return weights
 
def before_trading_start(context, data):
    """
    Get pipeline results.
    """
 
    # Gets our pipeline output every day.
    pipe_results = pipeline_output('my_pipeline')
 
    # Go long in securities for which the 'longs' value is True,
    # and check if they can be traded.
    context.longs = []
    
    context.ratio = pipe_results['cap']/(pipe_results['cap'].sum())
    
    for sec in pipe_results[pipe_results['longs']].index.tolist():
        if data.can_trade(sec):
            context.longs.append(sec)
 
    # Go short in securities for which the 'shorts' value is True,
    # and check if they can be traded.
    context.shorts = []
    for sec in pipe_results[pipe_results['shorts']].index.tolist():
        if data.can_trade(sec):
            context.shorts.append(sec)
 
def my_rebalance(context, data):
    """
    Rebalance weekly.
    """
 
    # Calculate target weights to rebalance
    target_weights = compute_target_weights(context, data)
 
    # If we have target weights, rebalance our portfolio
    if target_weights:
        order_optimal_portfolio(
            objective=opt.TargetWeights(target_weights),
            constraints=[],
        )
