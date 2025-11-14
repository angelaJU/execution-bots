#########################################################################################
# Date: 2018.08
# Author: min.li
# Email: min.li@altonomy.io
# Github: https://github.com/happyshiller/Altonomy
# Function: a simple market making strategy for liquidity enhancing
#########################################################################################

import os
import csv
import time
import random
import pandas as pd # to manipulate dataframes
import numpy as np # to manipulate arrays
from . import config
import json
import datetime
import requests
from bisect import bisect
from .ServiceBot import ServiceBot
from .alphas import mean_reversion_ema
from .LiquidityEnhancerBot import LiquidityEnhancerBot
from altonomy.core import client

class LiquidityEnhancerPlusBot(LiquidityEnhancerBot):
    """LiquidityEnhancerPlusBot is a generic class for a market making strategy - aiming for enhancing the liquidity"""

    def __init__(self, brokers, clerk, altcoin, quotecoin, logger=None, liquidEx=None, conversion_pair=None):
        """Initialize attributes of the ServiceBot, then attributes specific to DepthBot"""
        super().__init__(brokers, clerk, altcoin, quotecoin, logger)
        
        self.terminal = client()
        self.update_aux_exchange(liquidEx)
        self.update_aux_pair(self.tradingpair)

        self.currentdepthask = None
        self.currentdepthbid = None

        self.currentdepthasklocal = None
        self.currentdepthbidlocal = None

    def clear_variables_for_pause(self):
        """ clear variables after pause """
        super().clear_variables_for_pause()
        self.currentdepthasklocal = None
        self.currentdepthbidlocal = None

    def update_aux_exchange(self, exstr):
        if exstr is not None:
            self.auxExchange = exstr
            # self.terminal.set_defaults({
            #     "_exchange": self.auxExchange
            # })
        else:
            self.auxExchange = None

    def update_conv_exchange(self, exstr):
        if exstr is not None:
            self.convExchange = exstr
        else:
            self.convExchange = None

    def update_aux_pair(self, pairstr):
        if pairstr is not None and pairstr.upper() != 'DEFAULT':
            self.auxPair = pairstr
        else:
            self.auxPair = None

    def update_conv_pair(self, pairstr):
        if pairstr is not None and pairstr.upper() != 'DEFAULT':
            self.convPair = pairstr
        else:
            self.convPair = None

    def update_depthbook(self):
        try:
            results = self.terminal.orderbook(self.auxPair, self.auxExchange, self.convPair, self.convExchange)
            self.logger.debug(f"RESULTS = {results}")
        except Exception as e:
            self.logger.warning(f"{e}")
            return

        source_pair = list(results.keys())[0]
        
        currentdepthbid = results[source_pair]['bids']
        currentdepthask = results[source_pair]['asks']
        if currentdepthbid is not None:
            self.currentdepthbid = currentdepthbid
        if currentdepthask is not None:
            self.currentdepthask = currentdepthask

    def build_initial_sample(self):
        """build up the sample set of mid price"""
        self.logger.debug('[Function]: Build initial sample')
        midprice = 0.0
        # bidaskspread = 0.0
        # self.midpxsamplelist = []
        while len(self.midpxsample) < self.samplesize:
            beginningtime = time.time()
            if isinstance(self.brokers, list) and len(self.brokers) == 1:
                # update orderbook
                self.brokers[0].clear_broker()
                self.brokers[0].update_orderbook(self.tradingpair)
            else:
                raise Exception

            self.update_depthbook()
            depthbid = self.currentdepthbid
            depthask = self.currentdepthask
            
            if isinstance(depthbid, list) and isinstance(depthask, list):
                if depthask[0]['price'] <= 0 or depthbid[0]['price'] <= 0:
                    continue
                midprice = round(float((depthask[0]['price'] + depthbid[0]['price'])/2), self.orderpricerounding)
                spread = float(depthask[0]['price']  - depthbid[0]['price'])
                self.midpxsample.append(midprice)
                self.spreadsample.append(spread)
                #if midprice > 0 and spread > 0: ## update current depth bid/ask as well
                #    self.currentdepthbid = depthbid
                #    self.currentdepthask = depthask
            else:
                continue

            timeinterval = time.time() - beginningtime
            if self.sampleinterval > timeinterval:
                time.sleep(self.sampleinterval-timeinterval)

        self.retsample = self.ret_convert(self.midpxsample)
        self.retsample = self.winsorize(self.retsample)    
        self.logger.debug('samples built(midpx): %s' % (str(self.midpxsample)) )
        self.logger.debug('samples built(retrn): %s' % (str(self.retsample)) )

    def update_depthbook_local(self):
        currentdepthbidlocal = self.brokers[0].get_depth_bid_and_size(self.tradingpair, self.maxquotinglevels)
        currentdepthasklocal = self.brokers[0].get_depth_ask_and_size(self.tradingpair, self.maxquotinglevels)
        if currentdepthbidlocal is not None:
            self.currentdepthbidlocal = currentdepthbidlocal
        if currentdepthasklocal is not None:
            self.currentdepthasklocal = currentdepthasklocal
    
    def current_mid_local(self):
        current_mid_local = float(self.currentdepthasklocal[0]['price'] + self.currentdepthbidlocal[0]['price'])/2.0
        return current_mid_local

    def current_spread(self):
        current_spread = float(self.currentdepthasklocal[0]['price'] - self.currentdepthbidlocal[0]['price'])
        current_spread = round(current_spread, self.orderpricerounding)
        return current_spread

    def do_posting_min_spread(self):
        ##min spread improvment
        if self.minimum_spread_final > self.epsilon and self.current_spread() > self.minimum_spread_final + self.epsilon:
            ## when min spread is not satisfied, improving current bid/ask is necessary
            
            min_spread = self.minimum_spread_final
            price_buy0 = self.current_mid_local() - min_spread / 2.0
            price_sell0= self.current_mid_local() + min_spread / 2.0
            price_buy = min(price_buy0, self.currentdepthbidlocal[0]['price'] + self.tierdepth)
            if len(self.icebergOrderbuylist) > 0:
                price_buy = min(price_buy, self.icebergOrderbuylist[0]['orderprice'] + self.tierdepth)
            price_sell= max(price_sell0, self.currentdepthasklocal[0]['price'] - self.tierdepth)
            if len(self.icebergOrderselllist) > 0:
                price_sell= max(price_sell, self.icebergOrderselllist[0]['orderprice'] - self.tierdepth)
            price_buy = round(price_buy, self.orderpricerounding)
            price_sell= round(price_sell,self.orderpricerounding)

            self.logger.debug('[do posting]:: current_spread %s, is less than min spread %s / triggering price buy %s, price sell %s' % (str(self.current_spread()), str(min_spread), str(price_buy), str(price_sell) ))
            ordersize = self.generate_buyorder_size(price_buy)
            ordersize = min(ordersize, self.open_max_amount - self.open_buy_orders_amount)
            minsize = self.get_minsize_from_minvalue(price_buy)
            # if ordersize <= 0 or (minsize > 0 and ordersize < minsize): ##or self.should_cancel(orderprice, ordersize, 'BUY'):
            if ordersize <= 0 or (minsize > 0 and ordersize < minsize) or self.should_cancel_by_risk(price_buy, ordersize, 'BUY'):
                self.logger.info('[do_posting]:: ordersize invalid %s, minsize = %s (min spread)' % (str(ordersize), str(minsize)) )
            elif ( not self.cancel_for_chase_buy_flag and \
            (len(self.icebergOrderbuylist) == 0 or price_buy - self.icebergOrderbuylist[0]['orderprice'] >= self.min_sep or
            (abs(price_buy - price_buy0) < self.epsilon and price_buy - self.icebergOrderbuylist[0]['orderprice'] > self.epsilon)) ): 
                sendorderid = self.generate_order(price_buy, ordersize, sellbroker=None, buybroker=self.brokers[0])
                self.logger.debug('[do_posting]:: pair %s prepare to send buy order price=%s, size=%s, orderid=%s for min spread' % (self.tradingpair, str(price_buy), str(ordersize), str(sendorderid)))
                if sendorderid is not None:
                    icebergorder = {"tiernumber": 0,
                                    "ordersize": ordersize,
                                    "orderprice":price_buy,
                                    "orderside":"BUY",
                                    "orderid":sendorderid,
                                    "accumFilledAmount":0,
                                    "lastAccumFilledAmout":0,
                                    "inactive": 0,
                                    "orderTime": time.time()
                                    }
                    self.icebergOrderbuylist.insert(0, icebergorder)
                    self.open_buy_orders_amount += ordersize
                    self.logger.info('[do_posting]:: %s buy order sent: %s for min spread' % (self.tradingpair, str(icebergorder)))

            ordersize = self.generate_sellorder_size(price_sell)
            ordersize = min(ordersize, self.open_max_amount - self.open_sell_orders_amount)
            minsize = self.get_minsize_from_minvalue(price_sell)
            # if ordersize <= 0 or (minsize > 0 and ordersize < minsize): ##or self.should_cancel(orderprice, ordersize, 'BUY'):
            if ordersize <= 0 or (minsize > 0 and ordersize < minsize) or self.should_cancel_by_risk(price_sell, ordersize, 'SELL'):
                self.logger.info('[do_posting]:: ordersize invalid %s, minsize=%s (min spread)' % (str(ordersize), str(minsize)) )
            elif ( not self.cancel_for_chase_sell_flag and \
            (len(self.icebergOrderselllist) == 0 or price_sell - self.icebergOrderselllist[0]['orderprice'] <= -self.min_sep or 
            (abs(price_sell - price_sell0) < self.epsilon and price_sell - self.icebergOrderselllist[0]['orderprice'] < -self.epsilon)) ):    
                sendorderid = self.generate_order(price_sell, ordersize, sellbroker=self.brokers[0], buybroker=None)
                self.logger.debug('[do_posting]:: pair %s prepare to send sell order price=%s, size=%s, orderid=%s for min spread' % (self.tradingpair, str(price_sell), str(ordersize), str(sendorderid)))
                if sendorderid is not None:
                    icebergorder = {"tiernumber": 0,
                                    "ordersize": ordersize,
                                    "orderprice":price_sell,
                                    "orderside":"SELL",
                                    "orderid":sendorderid,
                                    "accumFilledAmount":0,
                                    "lastAccumFilledAmout":0,
                                    "inactive": 0,
                                    "orderTime": time.time()
                                    }
                    self.icebergOrderselllist.insert(0, icebergorder)
                    self.open_sell_orders_amount += ordersize
                    self.logger.info('[do_posting]:: %s sell order sent: %s for min spread' % (self.tradingpair, str(icebergorder)))



    def tick(self):
        
        """Update orderbook in every tick and update account balance in every 5 minutes"""
        # setup condition to update account balance in every 300s
        do_update_accout = False
        if int(time.time() - self.account_ts) > config.ACCOUNT_TS: # 300s
            self.account_ts = time.time()
            do_update_accout = True
        # setup condition to update mid price sample in every 5s
        do_update_midpxsample = False
        if time.time() - self.sample_ts > self.sampleinterval:
            do_update_midpxsample = True
        
        if isinstance(self.brokers, list) and len(self.brokers) == 1:
            # self.logger.debug('[tick] - brokers validated')
            # update account balance
            if do_update_accout:
                self.brokers[0].update_account_balance(self.brokers[0].get_account_id())
                self.monitor_until_canceled()
                if self.clerk and config.RECORD_TO_DB:
                    self.update_order_history()

            if do_update_midpxsample != True: return False

            # update orderbook
            self.brokers[0].clear_broker()
            self.brokers[0].update_orderbook(self.tradingpair)
            
            self.update_depthbook()
            self.update_depthbook_local()
            # update sample 
            if do_update_midpxsample:
                self.sample_ts = time.time()
                self.update_ema_quantities(self.sample_ts)
                self.create_midpx_sample()
                self.update_movingaverage()
            
            return True
            
        else:
            # TODO logging 
            self.logger.debug('no broker')
            return False