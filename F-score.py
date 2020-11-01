from quantopian.algorithm import order_optimal_portfolio
from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline import Pipeline, CustomFactor, experimental
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.factors import SimpleMovingAverage
from quantopian.pipeline.filters import QTradableStocksUS, StaticAssets, StaticSids
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
    
class Fscore(CustomFactor):
    inputs = [
        morningstar.operation_ratios.roa,
        morningstar.cash_flow_statement.operating_cash_flow,
        morningstar.cash_flow_statement.cash_flow_from_continuing_operating_activities,
        
        morningstar.operation_ratios.long_term_debt_equity_ratio,
        morningstar.operation_ratios.current_ratio,
        morningstar.valuation.shares_outstanding,
        
        morningstar.operation_ratios.gross_margin,
        morningstar.operation_ratios.assets_turnover,
    ]
    window_length = 22
    
    def compute(self, today, assets, out,
                roa, cash_flow, cash_flow_from_ops,
                long_term_debt_ratio, current_ratio, shares_outstanding,
                gross_margin, assets_turnover):
        profit = (
            (roa[-1] > 0).astype(int) +
            (cash_flow[-1] > 0).astype(int) +
            (roa[-1] > roa[0]).astype(int) +
            (cash_flow_from_ops[-1] > roa[-1]).astype(int)
        )
        
        leverage = (
            (long_term_debt_ratio[-1] < long_term_debt_ratio[0]).astype(int) +
            (current_ratio[-1] > current_ratio[0]).astype(int) + 
            (shares_outstanding[-1] <= shares_outstanding[0]).astype(int)
        )
        
        operating = (
            (gross_margin[-1] > gross_margin[0]).astype(int) +
            (assets_turnover[-1] > assets_turnover[0]).astype(int)
        )
        
        out[:] = profit + leverage + operating

class ROA(CustomFactor):
    window_length = 1
    inputs = [morningstar.operation_ratios.roa]
    
    def compute(self, today, assets, out, roa):
        out[:] = (roa[-1] > 0).astype(int)
        
class ROAChange(CustomFactor):
    window_length = 22
    inputs = [morningstar.operation_ratios.roa]
    
    def compute(self, today, assets, out, roa):
        out[:] = (roa[-1] > roa[0]).astype(int)
        
class CashFlow(CustomFactor):
    window_length = 1
    inputs = [morningstar.cash_flow_statement.operating_cash_flow]
    
    def compute(self, today, assets, out, cash_flow):
        out[:] = (cash_flow[-1] > 0).astype(int)
        
class CashFlowFromOps(CustomFactor):
    window_length = 1
    inputs = [morningstar.cash_flow_statement.cash_flow_from_continuing_operating_activities, morningstar.operation_ratios.roa]
    
    def compute(self, today, assets, out, cash_flow_from_ops, roa):
        out[:] = (cash_flow_from_ops[-1] > roa[-1]).astype(int)
        
class LongTermDebtRatioChange(CustomFactor):
    window_length = 22
    inputs = [morningstar.operation_ratios.long_term_debt_equity_ratio]
    
    def compute(self, today, assets, out, long_term_debt_ratio):
        out[:] = (long_term_debt_ratio[-1] < long_term_debt_ratio[0]).astype(int)
        
class CurrentDebtRatioChange(CustomFactor):
    window_length = 22
    inputs = [morningstar.operation_ratios.current_ratio]
    
    def compute(self, today, assets, out, current_ratio):
        out[:] = (current_ratio[-1] > current_ratio[0]).astype(int)
        
class SharesOutstandingChange(CustomFactor):
    window_length = 22
    inputs = [morningstar.valuation.shares_outstanding]
    
    def compute(self, today, assets, out, shares_outstanding):
        out[:] = (shares_outstanding[-1] <= shares_outstanding[0]).astype(int)
        
class GrossMarginChange(CustomFactor):
    window_length = 22
    inputs = [morningstar.operation_ratios.gross_margin]
    
    def compute(self, today, assets, out, gross_margin):
        out[:] = (gross_margin[-1] > gross_margin[0]).astype(int)
        
class AssetsTurnoverChange(CustomFactor):
    window_length = 22
    inputs = [morningstar.operation_ratios.assets_turnover]
    
    def compute(self, today, assets, out, assets_turnover):
        out[:] = (assets_turnover[-1] > assets_turnover[0]).astype(int) 
    
    

def make_pipeline():

    Market_cap = morningstar.valuation.market_cap.latest
    
    base_universe = Market_cap.top(500,mask=QTradableStocksUS())

    shorts =  Fscore(window_length=60).bottom(100,mask=base_universe)

    long_universe = Market_cap.top(20,mask=base_universe)
    
    longs = Fscore(window_length=60).top(10,mask=long_universe)

    securities_to_trade = (shorts | longs)

    return Pipeline(
        columns={
            'longs': longs,
            'shorts': shorts,
            'cap': Market_cap
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
        weights[security] = 2.05*context.long_weight.loc[security]

    for security in context.shorts:
        weights[security] = -1.95/len(context.shorts)

    return weights

def before_trading_start(context, data):
    """
    Get pipeline results.
    """

    # Gets our pipeline output every day.
    pipe_results = pipeline_output('my_pipeline')
    
    pipe_longs = pipe_results[pipe_results['longs']]
    context.long_weight = pipe_longs['cap']/(pipe_longs['cap'].sum())
    
    pipe_shorts = pipe_results[pipe_results['shorts']]
    context.short_weight = pipe_shorts['cap']/(pipe_shorts['cap'].sum())

    # Go long in securities for which the 'longs' value is True,
    # and check if they can be traded.
    context.longs = []
    
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
