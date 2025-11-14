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

class LiquidityEnhancerBot(ServiceBot):
    """LiquidityEnhancerBot is a generic class for a market making strategy - aiming for enhancing the liquidity"""

    def __init__(self, brokers, clerk, altcoin, quotecoin, logger=None):
        """Initialize attributes of the ServiceBot, then attributes specific to DepthBot"""
        super().__init__(brokers, clerk, altcoin, quotecoin, logger)
        
        ###################################
        ### static parameters and related variables
        ###################################
        # sample size
        self.samplesize = 0
        # take 10 minutes moving average, input in minutes from UI
        self.movingaveragewindow = 10
        # take a slice (bid ask - mid) every 5s
        self.sampleinterval = 5
        self.ema_half_life = 0.0
        self.vol_scale = 0.0

        # quoting edge, which add to moving average price
        self.quotingedge = 0.0
        # how many levels Algo quotes in the market
        # 5 level at each side
        self.quotinglevels = 3
        self.maxquotinglevels = 5
        self.taskordersize = 0
        self.ordersize_scaler = 2.0
        # the depth interval between each tier
        self.tierdepth = 0.0
        self.min_sep_frac = 0.2
        self.min_sep = 0.0
        self.initial_imbalance = 0.0
        self.initial_imbalance_alt = 0.0    # ABOTS-105
        self.imbalance_thresh = 0.0
        self.imbalance_thresh_alt = 0.0     # ABOTS-105
        self.maxlonguantity = 0.0
        self.maxshortquantity = 0.0
        self.open_max_amount = 0.0
        self.alpha_weights_str = ''
        self.alpha_weights = [0.0]
        self.minimum_spread = 0.0
        self.minimum_spread_bps = 0.0
        self.minimum_spread_final = 0.0
        self.epsilon = 1e-10
        self.min_ordervalue = 0
        self.min_size_increment = 0
        self.loop_interval = 0.05
        if self.ordersizerounding > 0:
            self.min_size_increment = round(float(10 ** -self.ordersizerounding), self.ordersizerounding)
        if self.ordervaluerounding > 0:
            self.min_ordervalue = round(float(10 ** -self.ordervaluerounding), self.ordervaluerounding)
        
        ###################################
        ### internal variables
        ###################################
        # a list of historical mid price
        self.midpxsample = []
        self.retsample = []
        self.spreadsample = []
        # the rolling moving average price
        self.movingaverage = 0.0
        self.ema_vol = 0.0
        self.ema_vol_1m = 0.0
        self.ema_price = 0.0
        self.ema_spread = 0.0
        # sample updating time stamp
        self.sample_ts = 0
        #order list of all quoting levels
        self.icebergOrderbuylist = []
        self.icebergOrderselllist = []
        #store moving average
        self.currentmovingaverage = 0.0
        #current depth book
        self.currentdepthbid = 0.0
        self.currentdepthask = 0.0
        self.current_alpha = 0.0
        self.open_buy_orders_num = 0
        self.open_buy_orders_amount = 0.0
        self.open_sell_orders_num = 0
        self.open_sell_orders_amount = 0.0
        self.imbalance_btc = 0.0
        self.imbalance_alt = 0.0    # ABOTS-105
        self.total_orders = 0
        self.total_cancels = 0
        self.altcoin_balance = 0.0
        self.quotecoin_balance = 0.0
        self.starting_balance = {}
        self.cancel_for_chase_buy_flag = False
        self.cancel_for_chase_sell_flag = False
        
        if self.clerk and config.RECORD_TO_DB:
            self.register_bot()

    def clear_variables_for_pause(self):
        self.logger.info(f"clearing variables after resume from pause")
        self.midpxsample = []
        self.retsample = []
        self.spreadsample = []
        self.movingaverage = 0.0
        self.ema_vol = 0.0
        self.ema_vol_1m = 0.0
        self.ema_price = 0.0
        self.ema_spread = 0.0

        # sample updating time stamp
        self.sample_ts = 0
        self.currentmovingaverage = 0.0

        # current depth book
        self.currentdepthbid = None
        self.currentdepthask = None
        self.currentdepthasklocal = None
        self.currentdepthbidlocal = None
        self.cancel_for_chase_buy_flag = False
        self.cancel_for_chase_sell_flag = False

    def update_sample_interval(self, sampleinterval=5):
        """update how many levels will be quoted in the market"""
        try:
            isampleinterval = int(float(sampleinterval))
            if isampleinterval > 0:
                self.sampleinterval = isampleinterval
            else:
                self.passanitycheck = False
                self.logger.error("The sample interval {lvl} is not a positive number, updating failed!".format(lvl=sampleinterval))
                return False
        except ValueError:
            self.passanitycheck = False
            self.logger.error("The sample interval {lvl} is not integer, updating failed!\n".format(lvl=sampleinterval))
            return False


    def update_moving_average_window(self,num=10):
        """update the moving average window, default 10 minutes MA"""
        try:
            inum = int(float(num))
            if inum > 0:
                self.movingaverage = inum
                self.samplesize = inum * 60 / self.sampleinterval
            else:
                self.passanitycheck = False
                self.logger.error("The moving average window {size} is not a positive number, updating failed!".format(size=num))
                return False
        except ValueError:
            self.passanitycheck = False
            self.logger.error("The sample size {size} is not integer, updating failed!\n".format(size=num))
            return False

    def update_ema_half_life(self, ema_half_life=30.0):
        """update the ema half life"""
        try:
            fema_half_life = float(ema_half_life)
            if fema_half_life > 0.0:
                self.ema_half_life = fema_half_life
            else:
                self.passanitycheck = False
                self.logger.error("The ema_half_life {half_life} is not a positive number, updating failed!".format(half_life=ema_half_life))
                return False
        except ValueError:
            self.passanitycheck = False
            self.logger.error("The ema_half_life {half_life} is not float, updating failed!\n".format(half_life=ema_half_life))
            return False


    def update_posting_volscale(self, vol_scale=1.0):
        """"""
        try:
            fvol_scale = float(vol_scale)
            if fvol_scale > 0.0:
                self.vol_scale = fvol_scale
            else:
                self.passanitycheck = False
                self.logger.error("The vol scale {eg} is not a positive number, updating failed!".format(eg=vol_scale))
                return False
        except ValueError:
            self.logger.error("The vol scale {eg} is not float, updating failed!\n".format(eg=vol_scale))
            return False

    def update_ordersize_scaler(self, ordersize_scaler=2.5):
        """"""
        try:
            fordersize_scaler = float(ordersize_scaler)
            if ordersize_scaler > 0.0:
                self.ordersize_scaler = fordersize_scaler
            else:
                self.passanitycheck = False
                self.logger.error("The ordersize scaler {eg} is not a positive number, updating failed!".format(eg=ordersize_scaler))
                return False
        except ValueError:
            self.logger.error("The ordersize scaler {eg} is not float, updating failed!\n".format(eg=ordersize_scaler))
            return False

    def update_min_separation_fraction(self, min_sep_frac=0.2):
        """"""
        try:
            fmin_sep_frac = float(min_sep_frac)
            if fmin_sep_frac > 0.0:
                self.min_sep_frac = fmin_sep_frac 
            else:
                self.passanitycheck = False
                self.logger.error("The min separation fraction {eg} is not a positive number, updating failed!".format(eg=min_sep_frac))
                return False
        except ValueError:
            self.logger.error("The min separation fraction {eg} is not float, updating failed!\n".format(eg=min_sep_frac))
            return False

    def update_quoting_edge(self, edge=0.0):
        """"""
        try:
            fedge = float(edge)
            if fedge > 0.0:
                self.quotingedge = round(fedge, self.orderpricerounding)
            else:
                self.passanitycheck = False
                self.logger.error("The quoting edge {eg} is not a positive number, updating failed!".format(eg=edge))
                return False
        except ValueError:
            self.logger.error("The quoting edge {eg} is not float, updating failed!\n".format(eg=edge))
            return False


    def update_quoting_levels(self, quotinglevels=3):
        """update how many levels will be quoted in the market"""
        try:
            iquotinglevels = int(float(quotinglevels))
            if iquotinglevels > 0:
                self.quotinglevels = iquotinglevels
            else:
                self.passanitycheck = False
                self.logger.error("The quoting level {lvl} is not a positive number, updating failed!".format(lvl=quotinglevels))
                return False
        except ValueError:
            self.passanitycheck = False
            self.logger.error("The quoting level {lvl} is not integer, updating failed!\n".format(lvl=quotinglevels))
            return False

    def update_max_quoting_levels(self, maxquotinglevels=5):
        """update how many levels will be quoted in the market"""
        try:
            imaxquotinglevels = int(float(maxquotinglevels))
            if imaxquotinglevels > 0:
                self.maxquotinglevels = imaxquotinglevels
            else:
                self.passanitycheck = False
                self.logger.error("The max quoting level {lvl} is not a positive number, updating failed!".format(lvl=maxquotinglevels))
                return False
        except ValueError:
            self.passanitycheck = False
            self.logger.error("The max quoting level {lvl} is not integer, updating failed!\n".format(lvl=maxquotinglevels))
            return False

    ## not used for now
    def update_quoting_pieces(self, quotingpieces=5):
        """update how many levels will be quoted in the market"""
        try:
            iquotingpieces = int(float(quotingpieces))
            if iquotingpieces > 0:
                self.quotingpieces = iquotingpieces
            else:
                self.passanitycheck = False
                self.logger.error("The quoting pieces {lvl} is not a positive number, updating failed!".format(lvl=quotingpieces))
                return False
        except ValueError:
            self.passanitycheck = False
            self.logger.error("The quoting pieces {lvl} is not integer, updating failed!\n".format(lvl=quotingpieces))
            return False


    def update_tier_depth(self, tierdepth):
        """update the depth interval between each tier"""
        try:
            ftierdepth = float(tierdepth)
            if ftierdepth > 0.0:
                self.tierdepth = round(ftierdepth, self.orderpricerounding)
            else:
                self.passanitycheck = False
                self.logger.error("The tier depth {depth} is not a positive number, updating failed!".format(depth=tierdepth))
                return False
        except ValueError:
            self.passanitycheck = False
            self.logger.error("The tier depth {depth} is not float, updating failed!\n".format(depth=tierdepth))
            return False
    
    
    def update_max_long_quantity(self, max_long_quantity):
        """
        :max_long_quantity: the max net long position
        :
        """
        try:
            fquantity = float(max_long_quantity)
            if fquantity > 0.0:
                self.maxlonguantity = fquantity
            else:
                self.logger.error("The max long quantity %s is not positive number, updating failed!\n" % str(max_long_quantity))
                self.passanitycheck = False
                return False
        except ValueError:
            self.logger.error("The max long quantity %s is not float, updating failed!\n" % str(max_long_quantity))
            self.passanitycheck = False
            return False

    def update_max_short_quantity(self, max_short_quantity):
        """
        :max_short_quantity: the max net short position 
        :
        """
        try:
            fquantity = float(max_short_quantity)
            if fquantity > 0.0:
                self.maxshortquantity = fquantity
            else:
                self.logger.error("The max short quantity %s is not positive number, updating failed!\n" % str(max_short_quantity))
                self.passanitycheck = False
                return False
        except ValueError:
            self.logger.error("The max short quantity %s is not float, updating failed!\n" % str(max_short_quantity))
            self.passanitycheck = False
            return False

    def update_open_max_amount(self, open_max_amount):
        """
        :open_max_amount: the max open buy or sell order amount in the market 
        :
        """
        try:
            fopen_max_amount = float(open_max_amount)
            if fopen_max_amount > 0.0:
                self.open_max_amount = fopen_max_amount
            else:
                self.logger.error("The open max amount %s is not positive number, updating failed!\n" % str(open_max_amount))
                self.passanitycheck = False
                return False
        except ValueError:
            self.logger.error("The open max amount %s is not float, updating failed!\n" % str(open_max_amount))
            self.passanitycheck = False
            return False
        
    def update_max_imbalance_btc(self, max_imbalance_btc):
        """
        :max_imbalance_btc: the max imbalance we accumulated in btc term 
        :
        """
        try:
            fmax_imbalance_btc = float(max_imbalance_btc)
            if fmax_imbalance_btc > 0.0:
                self.imbalance_thresh = fmax_imbalance_btc
            else:
                self.logger.error("The max imbalance btc %s is not positive number, updating failed!\n" % str(max_imbalance_btc))
                self.passanitycheck = False
                return False
        except ValueError:
            self.logger.error("The max imbalance btc %s is not float, updating failed!\n" % str(max_imbalance_btc))
            self.passanitycheck = False
            return False

    # ABOTS-105: added for altcoin
    def update_max_imbalance_alt(self, max_imbalance_alt):
        """
        :max_imbalance_alt: the max imbalance we accumulated in alt term 
        :
        """
        try:
            fmax_imbalance_alt = float(max_imbalance_alt)
            if fmax_imbalance_alt > 0.0:
                self.imbalance_thresh_alt = fmax_imbalance_alt
            else:
                self.logger.error("The max imbalance alt %s is not positive number, updating failed!\n" % str(max_imbalance_alt))
                self.passanitycheck = False
                return False
        except ValueError:
            self.logger.error("The max imbalance alt %s is not float, updating failed!\n" % str(max_imbalance_alt))
            self.passanitycheck = False
            return False

    def update_initial_imbalance(self, initial_imbalance_btc):
        """
        :initial_imbalance: the initial imbalance (btc/eth term) accumulated from before 
        :
        """
        try:
            finitial_imbalance_btc = float(initial_imbalance_btc)
            self.initial_imbalance = finitial_imbalance_btc
            self.imbalance_btc = self.initial_imbalance  ## assign initial balance to imbalance_btc
        except ValueError:
            self.logger.error("The initial imbalance btc %s is not float, updating failed!\n" % str(initial_imbalance_btc))
            self.passanitycheck = False
            return False

    # ABOTS-105: added for altcoin
    def update_initial_imbalance_alt(self, initial_imbalance_alt):
        """
        :initial_imbalance: the initial imbalance (alt term) accumulated from before 
        :
        """
        try:
            finitial_imbalance_alt = float(initial_imbalance_alt)
            self.initial_imbalance_alt = finitial_imbalance_alt
            self.imbalance_alt = self.initial_imbalance_alt  ## assign initial balance to imbalance_alt
        except ValueError:
            self.logger.error("The initial imbalance alt %s is not float, updating failed!\n" % str(initial_imbalance_alt))
            self.passanitycheck = False
            return False

    def update_alpha_weights(self, alpha_weights_str):
        """
        :alpha_weights_str: the alpha weights for signals seperated by comma
        """
        try:
            weights_list = alpha_weights_str.split(',')
            self.alpha_weights_str = alpha_weights_str
            self.alpha_weights = [float(item) for item in weights_list]
            if any([w < 0 for w in self.alpha_weights]):
                self.logger.error("The alpha weights %s are not all positive numbers, updating failed\n" % str(alpha_weights_str))
                self.passanitycheck = False
                return False
        except ValueError:
            self.logger.error("The alpha weight string %s is not float list delimitered by comma!")
            self.passanitycheck = False
            return False

    def update_minimum_spread(self, minimum_spread):
        """update the depth interval between each tier"""
        try:
            fminimum_spread = float(minimum_spread)
            if fminimum_spread >= 0.0:
                self.minimum_spread = round(fminimum_spread, self.orderpricerounding)
            else:
                self.passanitycheck = False
                self.logger.error("The mininum spread {mspread} is not a positive number, updating failed!".format(mspread=minimum_spread))
                return False
        except ValueError:
            self.passanitycheck = False
            self.logger.error("The minimum spread {mspread} is not float, updating failed!\n".format(mspread=minimum_spread))
            return False

    def update_minimum_spread_bps(self, minimum_spread_bps):
        """update the depth interval between each tier"""
        try:
            fminimum_spread_bps = float(minimum_spread_bps)
            if fminimum_spread_bps >= 0.0:
                self.minimum_spread_bps = fminimum_spread_bps
            else:
                self.passanitycheck = False
                self.logger.error("The mininum spread bps {mspread} is not a positive number, updating failed!".format(mspread=minimum_spread_bps))
                return False
        except ValueError:
            self.passanitycheck = False
            self.logger.error("The minimum spread bps {mspread} is not float, updating failed!\n".format(mspread=minimum_spread_bps))
            return False

    def update_task_order_size(self, ordersize):
        """ordersize: the normal order size for each order"""
        try:
            fordersize = float(ordersize)
            if fordersize > 0.0:
                self.taskordersize = fordersize
            else:
                self.logger.error("The order size %s is not positive number, updating failed!\n" % str(ordersize))
                self.passanitycheck = False
                return False
        except ValueError:
            self.logger.error("The order size %s is not float, updating failed!\n" % str(ordersize))
            self.passanitycheck = False
            return False

    def update_min_separation(self):
        self.min_sep = max(round(self.tierdepth * self.min_sep_frac, self.orderpricerounding), self.minimumticksize)

    def get_minsize_from_minvalue(self, price):
        minsize = 0
        if self.ordervaluerounding > 0:
            minsize = round(self.min_ordervalue / price + self.min_size_increment, self.ordersizerounding)
        return minsize

    def calculate_starting_balance(self):
        self.starting_balance = {}
        altcoinfrozenbalance = self.get_altcoin_frozen_balance()
        altcointradablebalance = self.get_altcoin_tradable_balance()
        quotecoinfrozenbalance = self.get_quotecoin_frozen_balance()
        quotecointradablebalance = self.get_quotecoin_tradable_balance()
        for broker in self.brokers:
            self.starting_balance.update({
                            str(broker.get_account_id()): {'altfree': altcointradablebalance[broker.get_account_id()],
                                                            'altfrozen': altcoinfrozenbalance[broker.get_account_id()],
                                                            'quotefree': quotecointradablebalance[broker.get_account_id()],
                                                            'quotefrozen': quotecoinfrozenbalance[
                                                                broker.get_account_id()]}
                                                                })

    def base_spread_unit(self):
        return self.ema_spread * 2.5
    
    def generate_buyorder_size(self, orderprice):
        """"""
        randomordersize = self.taskordersize * random.uniform(1.0-self.orderandomrange, 1.0+self.orderandomrange)
        sfactor = 1.0 +  max(0.0, (self.current_mid() - orderprice) / self.base_spread_unit() - 1.0) * self.ordersize_scaler
        randomordersize = randomordersize * sfactor

        if randomordersize > 0:
            self.logger.info("The buy order size is %s" % str(round(randomordersize, self.ordersizerounding)))
            return round(randomordersize, self.ordersizerounding)
        else:
            self.logger.info("The buy order size is %s" % str(round(self.taskordersize, self.ordersizerounding)))
            return round(self.taskordersize, self.ordersizerounding) 


    def generate_sellorder_size(self, orderprice):
        """"""
        randomordersize = self.taskordersize * random.uniform(1.0-self.orderandomrange, 1.0+self.orderandomrange)
        sfactor = 1.0 +  max(0.0, (orderprice - self.current_mid()) / self.base_spread_unit() - 1.0) * self.ordersize_scaler
        randomordersize = randomordersize * sfactor

        if randomordersize > 0:
            self.logger.info("The sell order size is %s" % str(round(randomordersize, self.ordersizerounding)))
            return round(randomordersize, self.ordersizerounding)
        else:
            self.logger.info("The sell order size is %s" % str(round(self.taskordersize, self.ordersizerounding)))
            return round(self.taskordersize, self.ordersizerounding) 

    def winsorize(self, datalist, sigma=5):
        """clean sample data, remove outliers from historical mid price list"""
        if isinstance(datalist, list) and len(datalist) > 0:
            datarray = np.array(datalist)
            datacap = np.std(datarray) * sigma
            return list([ min(max(x,-datacap),datacap) for x in datarray ])
        else:
            return None

    def ret_convert(self, pxlist):
        if isinstance(pxlist, list) and len(pxlist) > 0:
            start = pxlist[:-1]
            end = pxlist[1:]
            return list(np.log(end) - np.log(start))
        else:
            return None
            
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

            depthbid = self.brokers[0].get_depth_bid_and_size(self.tradingpair, self.maxquotinglevels)
            depthask = self.brokers[0].get_depth_ask_and_size(self.tradingpair, self.maxquotinglevels)
            
            if isinstance(depthbid, list) and isinstance(depthask, list):
                if depthask[0]['price'] <= 0 or depthbid[0]['price'] <= 0:
                    continue
                midprice = round(float((depthask[0]['price'] + depthbid[0]['price'])/2), self.orderpricerounding)
                spread = float(depthask[0]['price']  - depthbid[0]['price'])
                self.midpxsample.append(midprice)
                self.spreadsample.append(spread)
                if midprice > 0 and spread > 0: ## update current depth bid/ask as well
                    self.currentdepthbid = depthbid
                    self.currentdepthask = depthask
            else:
                continue

            timeinterval = time.time() - beginningtime
            if self.sampleinterval > timeinterval:
                time.sleep(self.sampleinterval-timeinterval)

        self.retsample = self.ret_convert(self.midpxsample)
        self.retsample = self.winsorize(self.retsample)    
        self.logger.debug('samples built(midpx): %s' % (str(self.midpxsample)) )
        self.logger.debug('samples built(retrn): %s' % (str(self.retsample)) )
        
    def create_midpx_sample(self):
        """add a new slice to the sample, delete an old one in every 5s"""
        self.logger.debug('[Function]: create_midpx_sample')
        midprice = 0.0
        # bidaskspread = 0.0

        depthbid = self.currentdepthbid
        depthask = self.currentdepthask
        # no update if there is no meaningful orderbook data    
        if isinstance(depthbid, list) and isinstance(depthask, list):
            midprice = round(float((depthask[0]['price'] + depthbid[0]['price'])/2), self.orderpricerounding)
            self.midpxsample.append(midprice)
            del self.midpxsample[0]
        return self.midpxsample      


    def generate_moving_average_price(self, sampledata):
        """generate rolling moving average price based on sample"""
        movingaverage = 0.0
        midpxlist = sampledata
        if isinstance(midpxlist, list) and len(midpxlist) >= self.samplesize:
            ##midpxlist = self.reject_outliers(midpxlist)
            if midpxlist is not None:
                #midpxlist = midpxlist.tolist()
                movingaverage = sum(midpxlist) / len(midpxlist)
        else:
            return False   
        return movingaverage    


    def initialize_ema_quantities(self, pxlist, retlist, spreadlist):
        self.logger.debug('[Function]: initialize_ema_quantities')
        if isinstance(pxlist, list):
            self.ema_price = np.mean(pxlist)
            self.logger.debug('ema_price initialized: %s' % str(self.ema_price))
        if isinstance(retlist, list):
            self.ema_vol = np.std(retlist)
            self.ema_vol_1m = self.ema_vol * np.sqrt(60.0 / self.sampleinterval)
            self.logger.debug('ema_vol initialized: %s, ema_vol_1m initialized: %s' % (str(self.ema_vol), str(self.ema_vol_1m)))
        if isinstance(spreadlist, list):
            self.ema_spread = np.mean(spreadlist)
            self.logger.debug('ema_spread initialized: %s' % str(self.ema_spread))


    def update_ema_quantities(self, last_update_ts):
        self.logger.debug('[Function]: update_ema_quantities')
        time_lapsed = time.time() - last_update_ts
        if (isinstance(self.currentdepthask, list) and isinstance(self.currentdepthbid, list)):
            curr_mid = self.current_mid()
            curr_spread = float(self.currentdepthask[0]['price'] - self.currentdepthbid[0]['price'])
            ## make sure to call this before update midpxsample list, as we asusmed midpxsample[-1] is from previous cycle
            curr_ret = np.log(curr_mid) - np.log(self.midpxsample[-1])
            decay = np.power(0.5, time_lapsed / self.ema_half_life)
            self.ema_vol = np.sqrt(curr_ret * curr_ret * (1 - decay) + self.ema_vol * self.ema_vol * decay)
            self.ema_vol_1m = self.ema_vol * np.sqrt(60.0 / self.sampleinterval)
            self.ema_price = curr_mid * (1 - decay) + self.ema_price * decay
            self.ema_spread = curr_spread* (1 - decay) + self.ema_spread * decay
            self.logger.info("tradingpair=%s, ema_vol=%s, ema_vol_1m=%s, ema_price=%s, ema_spread=%s" % (self.tradingpair, str(self.ema_vol), str(self.ema_vol_1m), str(self.ema_price), str(self.ema_spread)))

    def update_movingaverage(self):
        self.currentmovingaverage = self.generate_moving_average_price(self.midpxsample)

    def update_depthbook(self):
        currentdepthbid = self.brokers[0].get_depth_bid_and_size(self.tradingpair, self.maxquotinglevels)
        currentdepthask = self.brokers[0].get_depth_ask_and_size(self.tradingpair, self.maxquotinglevels)
        if currentdepthbid is not None:
            self.currentdepthbid = currentdepthbid
        if currentdepthask is not None:
            self.currentdepthask = currentdepthask

    def current_mid(self):
        current_mid = float(self.currentdepthask[0]['price'] + self.currentdepthbid[0]['price'])/2.0
        return current_mid

    def current_spread(self):
        current_spread = float(self.currentdepthask[0]['price'] - self.currentdepthbid[0]['price'])
        current_spread = round(current_spread, self.orderpricerounding)
        return current_spread

    def calculate_consolidated_alpha(self):
        ## consolidate all the alphas 
        midprice = self.current_mid()
        consolidated = mean_reversion_ema(midprice, self.ema_price) * self.alpha_weights[0] 
        self.logger.debug("alpha_weight=%s, consolidated=%s" % (str(self.alpha_weights), str(consolidated)) )
        return consolidated
    
    def should_cancel_back(self, orderprice, ordersize, orderside):
        self.logger.debug('[should_cancel_back]:: orderprice %s, ordersize %s, orderside %s' % (str(orderprice), str(ordersize), orderside))
        if orderside != 'BUY' and orderside != 'SELL':
            return True
        
        ## if order price falls in the signal indicated moving range, cancel and replace
        consolidated = self.calculate_consolidated_alpha()
        self.logger.debug('consolidated_alpha = %s' % (str(consolidated)) )
        self.logger.debug('[CANCEL_S?]: side=' + orderside + ',alpha=' + str(consolidated) + ',mid=' + str(self.current_mid()) + ',predprice=' + str((1+consolidated)*self.current_mid()) + ', orderprice=' + str(orderprice) )
        if orderside == 'BUY':
            if ((1 + consolidated) * self.current_mid() < orderprice):
                self.logger.info('[should_cancel_back]: CANCEL buy alpha=%s, mid=%s, < orderprice=%s' % (str(consolidated), str(self.current_mid()), str(orderprice)) )
                self.logger.debug('buy order CANCELLING from signal')
                return True
        else:
            if ((1 + consolidated) * self.current_mid() > orderprice):
                self.logger.info('[should_cancel_back]: CANCEL sell alpha=%s, mid=%s, > orderprice=%s' % (str(consolidated), str(self.current_mid()), str(orderprice)) )
                self.logger.debug('sell order CANCELLING from signal')
                return True
        
        return False

    def should_cancel_by_risk(self, orderprice, ordersize, orderside='BUY'):
        ## if reached reserved amount restriction limit or orther risk limit, cancel back
        if orderside == 'SELL':
            
            if self.imbalance_btc + ordersize * orderprice > self.imbalance_thresh:   ## no more sells
                self.logger.info("[should_cancel]:: The btc imbalance for %s%s is %s, exceeding the max threshold %s."
                                 % (self.altcoin, self.quotecoin, str(self.imbalance_btc), str(self.imbalance_thresh)))
                return True

            # ABOTS-105
            if self.imbalance_alt - ordersize < -self.imbalance_thresh_alt:   ## no more sells
                self.logger.info("[should_cancel]:: The alt imbalance for %s%s is %s, exceeding the max threshold %s."
                                 % (self.altcoin, self.quotecoin, str(self.imbalance_alt), str(self.imbalance_thresh_alt)))
                return True

            if (self.altcoin_balance - ordersize) < self.reservedaltcoin:
                self.logger.info("[should_cancel]:: The %s (- ordersize %s) tradable balance of altcoin %s is less than the minimum reserved altcoin balance of %s."
                                 % (str(self.altcoin_balance), str(ordersize), self.altcoin, str(self.reservedaltcoin)))
                return True
    
        else:
        
            if self.imbalance_btc - ordersize * orderprice < - self.imbalance_thresh:
                self.logger.info("[should_cancel]:: The btc imbalance for %s%s is %s, exceeding the max threshold %s."
                                 % (self.altcoin, self.quotecoin, str(self.imbalance_btc), str(self.imbalance_thresh)))
                return True

            # ABOTS-105
            if self.imbalance_alt + ordersize > self.imbalance_thresh_alt:
                self.logger.info("[should_cancel]:: The alt imbalance for %s%s is %s, exceeding the max threshold %s."
                                 % (self.altcoin, self.quotecoin, str(self.imbalance_alt), str(self.imbalance_thresh_alt)))
                return True

            if (self.quotecoin_balance - ordersize * orderprice) < self.reservedquotecoin:
                self.logger.info("[should_cancel]:: The %s (- ordervalue %s) tradable balance of quote coin %s is less than the minimum reserved quote coin balance of %s."
                                 % (str(self.quotecoin_balance), str(ordersize * orderprice), self.quotecoin, str(self.reservedquotecoin)))
                return True

        return False
    
    def should_cancel(self, orderprice, ordersize, orderside='BUY'):
        self.logger.debug('[should_cancel]:: orderprice %s, ordersize %s, orderside %s' % (str(orderprice), str(ordersize), orderside))
        
        if self.should_cancel_back(orderprice, ordersize, orderside) or self.should_cancel_by_risk(orderprice, ordersize, orderside):
            return True

        return False

    def update_coin_balances(self):
        self.quotecoin_balance = self.brokers[0].get_coin_tradable_balance(self.quotecoin)
        self.altcoin_balance = self.brokers[0].get_coin_tradable_balance(self.altcoin)

    def update_imbalance(self, side, executed_qty, executed_price):
        ## account imbalance due to trading activity (in BTC term?)
        # ABOTS-105: added altcoin imbalance
        self.logger.debug(f"update imbalance :: before :: {self.imbalance_btc} {self.quotecoin}")
        self.logger.debug(f"update imbalance :: before :: {self.imbalance_alt} {self.altcoin}")
        if side == "BUY":
            self.imbalance_btc = self.imbalance_btc - executed_qty * executed_price
            self.imbalance_alt = self.imbalance_alt + executed_qty
        elif side == "SELL":
            self.imbalance_btc = self.imbalance_btc + executed_qty * executed_price
            self.imbalance_alt = self.imbalance_alt - executed_qty
        self.logger.debug(f"update imbalance :: after :: {self.imbalance_btc} {self.quotecoin}")
        self.logger.debug(f"update imbalance :: after :: {self.imbalance_alt} {self.altcoin}")
        
    def update_open_orders(self, side, open_qty, limit_price):
        ## already hanled in update_status_for_layers
        pass

    def calculate_posting_distance(self):
        ## 
        self.logger.debug('calculate posting distance: vol_scale=%s, ema_vol_1m=%s, quoteedege=%s' % (str(self.vol_scale), str(self.ema_vol_1m), str(self.quotingedge)) )
        posting_distance = self.current_mid() * self.vol_scale * self.ema_vol_1m + self.quotingedge
        return posting_distance

    def get_posting_price(self, orderside, numtierdepth = 0):
        self.logger.debug('Function: get_posting_price:')
        posting_distance = self.calculate_posting_distance()
        if (self.current_mid() <= 0.0 or self.currentdepthbid[0]['price'] <=0.0 or self.currentdepthask[0]['price'] <= 0.0):
            self.logger.error('invalid bid/ask/mid - orderprice set to 0')
            return 0.0
        if orderside == 'BUY':
            price = round( self.current_mid() - posting_distance - numtierdepth * self.tierdepth, self.orderpricerounding)
            self.logger.debug('BUY - orderprice=%s, mid=%s, posting_distance=%s, tierdepth=%s' % (str(price), str(self.current_mid()), str(posting_distance), str(self.tierdepth)) )
        elif orderside == 'SELL':
            price = round( self.current_mid() + posting_distance + numtierdepth * self.tierdepth, self.orderpricerounding)
            self.logger.debug('SELL - orderprice=%s, mid=%s, posting_distance=%s, tierdepth=%s' % (str(price), str(self.current_mid()), str(posting_distance), str(self.tierdepth)) )
        else:
            self.logger.error('order side is invalid, orderpice set to 0')
            return 0.0
        if price >= self.minorderprice and price <= self.maxorderprice:
            self.logger.debug('orderprice=%s is in between %s and %s' % (str(price), str(self.minorderprice), str(self.maxorderprice)))
            return price
        else:
            self.logger.debug('orderprice=%s is in outside range (%s, %s)' % (str(price), str(self.minorderprice), str(self.maxorderprice)))
            return 0.0

    def generate_order(self, orderprice, ordersize, sellbroker=None, buybroker=None):
        """
        :bestprice: the beginning quoting price
        :sellbroker: the broker takes our sell order
        :buybroker: the broker takes our buy order
        """
        limitbuyorderID = None
        limitsellorderID = None
        # Commission discount has been take out
        if not self.meets_minimum_size_and_notional_thresholds(orderprice, ordersize):
            return None

        self.logger.debug('start generating order!')
        if sellbroker:
            limitsellorderID = sellbroker.client.sell_wait(self.tradingpair, orderprice, ordersize)[0]
            self.total_orders += 1
            self.logger.debug('generating sell orders %s, w price %s, size %s' % (str(limitsellorderID), str(orderprice), str(ordersize)))
        if buybroker:
            limitbuyorderID = buybroker.client.buy_wait(self.tradingpair, orderprice, ordersize)[0]
            self.total_orders += 1
            self.logger.debug('generating buy orders %s, w price %s, size %s' % (str(limitbuyorderID), str(orderprice), str(ordersize)))
        # TODO add recording functions
        # time.sleep(0.2)
        if limitsellorderID:
            sellbroker.sellorderlist.append(limitsellorderID)
            self.logger.debug('sell order generated %s' % str(limitsellorderID))
            try:
                if self.clerk and config.RECORD_TO_DB:
                    self.record_order_to_db(sellbroker, self.tradingpair, 'SELL_LIMIT', limitsellorderID)
            except:
                pass  
            return limitsellorderID
            # self.record_order_to_csv(sellbroker, self.tradingpair, 'SELL_LIMIT', limitsellorderID)
            # self.record_order_to_db(sellbroker, self.tradingpair, 'SELL_LIMIT', limitsellorderID)
        if limitbuyorderID:
            buybroker.buyorderlist.append(limitbuyorderID)
            self.logger.debug('buy order generated %s' % str(limitbuyorderID))
            try:
                if self.clerk and config.RECORD_TO_DB:
                    self.record_order_to_db(buybroker, self.tradingpair, 'BUY_LIMIT', limitbuyorderID)
            except:
                pass
            return limitbuyorderID
            # self.record_order_to_csv(buybroker, self.tradingpair, 'BUY_LIMIT', limitbuyorderID)
            # self.record_order_to_db(buybroker, self.tradingpair, 'BUY_LIMIT', limitbuyorderID)
        ###
    
    def create_single_tier(self,tiernumber,side,order_size=-1):
        """create a single tier and generating order,each tier generate an order with details of ordersize,total quantity for this tier,orderprice,quoting side, orderID"""

        if side == "BUY":
            orderprice = self.get_posting_price("BUY", tiernumber)
            if orderprice <= 0.0:
                return False
            if order_size > 0:
                ordersize = order_size
            else:
                ordersize = self.generate_buyorder_size(orderprice)
            orderID = self.generate_order(orderprice,ordersize,None,self.brokers[0])
        elif side == "SELL":
            orderprice = self.get_posting_price("SELL", tiernumber)
            if orderprice <= 0.0:
                return False
            if order_size > 0:
                ordersize = order_size
            else:
                ordersize = self.generate_sellorder_size(orderprice)
            orderID = self.generate_order(orderprice,ordersize,self.brokers[0],None)
        else:
            self.logger.error("The side parameter side (%s) is neither a BUY nor a SELL!" % str(side))
            return None

        ordertime = time.time()
        if orderID:
            icebergorder = {"tiernumber": tiernumber,
                            "ordersize": ordersize,
                            "orderprice":orderprice,
                            "orderside":side,
                            "orderid":orderID,
                            "accumFilledAmount":0,
                            "lastAccumFilledAmout":0,
                            "inactive": 0,
                            "orderTime": ordertime
                            }
            return icebergorder
        else:
            return False


    def create_initial_tier(self,quotinglevels,side='BUY'):
        """create the intial tier list，one order per tier"""  
        iceburgOrderlist = []
        for i in range(quotinglevels):
            icebergorder = self.create_single_tier(i, side)
            if isinstance(icebergorder, dict) and len(icebergorder) > 0 and icebergorder['orderid'] is not None:
                iceburgOrderlist.append(icebergorder)
        return iceburgOrderlist


    def is_orderdetails_valid(self, orderdetails):
        return isinstance(orderdetails, dict) and len(orderdetails) > 0

    ## update fill amount and imbalance
    def update_status_for_layers(self):
        self.logger.debug('[update_status_for_layer]::')
        open_buy_orders_num = 0
        open_buy_orders_amt = 0.0
        open_sell_orders_num = 0
        open_sell_orders_amt = 0.0
        for border in self.icebergOrderbuylist:
            currentbuyorderid = border['orderid']
            if currentbuyorderid:
                time.sleep(self.loop_interval)
                buyorderdetails = self.brokers[0].client.get_order_details(self.tradingpair, 'BUY_LIMIT', currentbuyorderid, data_format='legacy')
                if self.is_orderdetails_valid(buyorderdetails):
                    border['lastAccumFilledAmount'] = border['accumFilledAmount']
                    border['accumFilledAmount'] = float(buyorderdetails.get('FilledAmount',0))
                    if border['accumFilledAmount'] == border['ordersize']:
                        border['inactive'] = 1
                    executed_qty = border['accumFilledAmount'] - border['lastAccumFilledAmount']
                    self.update_imbalance("BUY",executed_qty,border['orderprice'])
                open_buy_orders_num += 1
                open_buy_orders_amt += border['ordersize'] - border['accumFilledAmount']
        for sorder in self.icebergOrderselllist:
            currentsellorderid = sorder['orderid'] 
            if currentsellorderid:
                time.sleep(self.loop_interval)
                sellorderdetails = self.brokers[0].client.get_order_details(self.tradingpair, 'SELL_LIMIT', currentsellorderid, data_format='legacy')
                if self.is_orderdetails_valid(sellorderdetails):
                    sorder['lastAccumFilledAmount'] = sorder['accumFilledAmount']
                    sorder['accumFilledAmount'] = float(sellorderdetails.get('FilledAmount',0))
                    if sorder['accumFilledAmount'] == sorder['ordersize']:
                        sorder['inactive'] = 1
                    executed_qty = sorder['accumFilledAmount'] - sorder['lastAccumFilledAmount']
                    self.update_imbalance("SELL",executed_qty,sorder['orderprice']) 
                open_sell_orders_num += 1
                open_sell_orders_amt += sorder['ordersize'] - sorder['accumFilledAmount']
        
        self.open_buy_orders_num = open_buy_orders_num
        self.open_buy_orders_amount = open_buy_orders_amt
        self.open_sell_orders_num = open_sell_orders_num
        self.open_sell_orders_amount = open_sell_orders_amt

    def cancel_on_side(self, side='BUY'):
        self.logger.info('[cancel_on_side]:: %s' % side)
        if side == 'BUY':
            for border in self.icebergOrderbuylist:
                borderid = border['orderid']
                time.sleep(self.loop_interval)
                borderdetails = self.brokers[0].client.get_order_details(self.tradingpair, 'BUY_LIMIT', borderid, data_format='legacy')
                if self.is_orderdetails_valid(borderdetails):
                    self.controlled_cancel(self.brokers[0],self.tradingpair, 'BUY_LIMIT', borderid)
                    self.total_cancels += 1
                    ## assume success
                    border['inactive'] = 1
            #self.icebergOrderbuylist = [border for border in self.icebergOrderbuylist if border['inactive'] == 0]
        elif side == 'SELL':
            for sorder in self.icebergOrderselllist:
                sorderid = sorder['orderid']
                time.sleep(self.loop_interval)
                sorderdetails = self.brokers[0].client.get_order_details(self.tradingpair, 'SELL_LIMIT', sorderid, data_format='legacy')
                if self.is_orderdetails_valid(sorderdetails):
                    self.controlled_cancel(self.brokers[0],self.tradingpair, 'SELL_LIMIT', sorderid)
                    self.total_cancels += 1
                    sorder['inactive'] = 1
            #self.icebergOrderselllist= [sorder for sorder in self.icebergOrderselllist if sorder['inactive']== 0]
        self.clear_order_list()

    def clear_order_list(self):
        for order in self.icebergOrderbuylist + self.icebergOrderselllist:
            if order['inactive'] == 1:
                self._move_to_cancel_list(order)
        self.icebergOrderbuylist = [border for border in self.icebergOrderbuylist if border['inactive'] == 0]
        self.icebergOrderselllist = [sorder for sorder in self.icebergOrderselllist if sorder['inactive'] == 0]      

    def clear_and_cancel_by_alpha(self):
        for border in self.icebergOrderbuylist:
            if border['inactive'] == 1:
                continue
            buyorderid = border['orderid']
            if buyorderid:
                time.sleep(self.loop_interval)
                buyorderdetails = self.brokers[0].client.get_order_details(self.tradingpair,'BUY_LIMIT', buyorderid, data_format='legacy')
                if self.is_orderdetails_valid(buyorderdetails):
                    left_amount = border['ordersize'] - float(buyorderdetails.get('FilledAmount',0))
                    if buyorderdetails.get('Status',None) == 'Completed' or buyorderdetails.get('Status',None) == 'Canceled':
                        self.logger.info("[clear_and_cancel_by_alpha]:: order completed or canceled, orderid=%s" % buyorderid)
                        self.open_buy_orders_amount = max(0, self.open_buy_orders_amount - left_amount)
                        self.open_buy_orders_num = max(0, self.open_buy_orders_num - 1)
                        border['inactive'] = 1
                        continue
                    buyorderprice = border['orderprice']
                    need_cancel = self.should_cancel_back(buyorderprice, left_amount, 'BUY')
                    if need_cancel:
                        self.logger.info("[clear_and_cancel_by_alpha]:: cancel - buy avoid, orderid=%s" % buyorderid)
                        self.controlled_cancel(self.brokers[0],self.tradingpair, 'BUY_LIMIT', buyorderid)
                        self.total_cancels += 1
                        ## TO-DO: need to confirm it's canceled successfully
                        self.open_buy_orders_amount = max(0, self.open_buy_orders_amount - left_amount)
                        self.open_buy_orders_num = max(0, self.open_buy_orders_num - 1)
                        border['inactive'] = 1
        
        for sorder in self.icebergOrderselllist:
            if sorder['inactive'] == 1:
                continue
            sellorderid = sorder['orderid']
            if sellorderid:
                time.sleep(self.loop_interval)
                sellorderdetails = self.brokers[0].client.get_order_details(self.tradingpair, 'SELL_LIMIT', sellorderid, data_format='legacy')
                if self.is_orderdetails_valid(sellorderdetails):
                    left_amount = sorder['ordersize'] - float(sellorderdetails.get('FilledAmount',0))
                    if sellorderdetails.get('Status',None) == 'Completed' or sellorderdetails.get('Status',None) == 'Canceled':
                        self.logger.info("[clear_and_cancel_by_alpha]:: order completed or canceled, orderid=%s" % sellorderid)
                        self.open_sell_orders_amount = max(0, self.open_sell_orders_amount - left_amount)
                        self.open_sell_orders_num = max(0, self.open_sell_orders_num - 1)
                        sorder['inactive'] = 1
                        continue
                    sellorderprice = sorder['orderprice']
                    need_cancel = self.should_cancel_back(sellorderprice, left_amount, 'SELL')
                    if need_cancel:
                        self.logger.info("[clear_and_cancel_by_alpha]:: cancel - sell avoid, orderid=%s" % sellorderid)
                        self.controlled_cancel(self.brokers[0],self.tradingpair, 'SELL_LIMIT', sellorderid)
                        self.total_cancels += 1
                        self.open_sell_orders_amount = max(0, self.open_sell_orders_amount - left_amount)
                        self.open_sell_orders_num = max(0, self.open_sell_orders_num - 1)
                        sorder['inactive'] = 1       

        self.clear_order_list()        

    def cancel_for_chasing(self):
        min_sep = self.min_sep
        if len(self.icebergOrderbuylist) == self.maxquotinglevels:
            new_buy_order_price = self.get_posting_price('BUY')
            border0 = self.icebergOrderbuylist[0]
            border1 = self.icebergOrderbuylist[-1]
            cancel_for_chase = False
            cancel_reason = 'cancel_for_chase:: BUY (min separation)'
            if (new_buy_order_price - border0['orderprice']) > min_sep:
                cancel_for_chase = True
                self.cancel_for_chase_buy_flag = True
            elif (self.minimum_spread_final > self.epsilon and self.current_spread() > self.minimum_spread_final + self.epsilon):
                cancel_for_chase = True
                cancel_reason = 'cancel_for_chase:: BUY (min_spread)'
            if cancel_for_chase:
                self.logger.info('[cancel_for_chasing]:: new_orderprice=%s, border0_price=%s' % (str(new_buy_order_price),str(border0['orderprice'])))
                borderid = border1['orderid']
                borderdetails = self.brokers[0].client.get_order_details(self.tradingpair, 'BUY_LIMIT', borderid, data_format='legacy')
                if self.is_orderdetails_valid(borderdetails):
                    self.logger.info("[cancel_for_chasing]:: cancel - buy chase, orderid=%s, cancel_reason=%s" % (borderid, cancel_reason))
                    self.controlled_cancel(self.brokers[0],self.tradingpair, 'BUY_LIMIT', borderid)
                    self.total_cancels += 1
                    left_amount = border1['ordersize'] - float(borderdetails.get('FilledAmount',0))
                    self.open_buy_orders_amount = max(0, self.open_buy_orders_amount - left_amount)
                    self.open_buy_orders_num = max(0, self.open_buy_orders_num - 1)
                    border1['inactive'] = 1

        if len(self.icebergOrderselllist) == self.maxquotinglevels:
            new_sell_order_price = self.get_posting_price('SELL')
            sorder0 = self.icebergOrderselllist[0]
            sorder1 = self.icebergOrderselllist[-1]
            cancel_for_chase = False
            cancel_reason = 'cancel_for_chase:: SELL (min separation)'
            if (new_sell_order_price - sorder0['orderprice']) < -min_sep:
                cancel_for_chase = True
                self.cancel_for_chase_sell_flag = True
            elif (self.minimum_spread_final > self.epsilon and self.current_spread() > self.minimum_spread_final + self.epsilon):
                cancel_for_chase = True
                cancel_reason = 'cancel_for_chase:: SELL (min spread)'
            if cancel_for_chase:
                self.logger.info('[cancel_for_chasing]:: new_orderprice=%s, border0_price=%s' % (str(new_sell_order_price),str(sorder0['orderprice'])))
                sorderid = sorder1['orderid']
                sorderdetails = self.brokers[0].client.get_order_details(self.tradingpair, 'SELL_LIMIT', sorderid, data_format='legacy')
                if self.is_orderdetails_valid(sorderdetails):
                    self.logger.info("[cancel_for_chasing]:: cancel - sell chase, orderid=%s" % sorderid)
                    self.controlled_cancel(self.brokers[0],self.tradingpair, 'SELL_LIMIT', sorderid)
                    self.total_cancels += 1
                    left_amount = sorder1['ordersize'] - float(sorderdetails.get('FilledAmount',0))
                    self.open_sell_orders_amount = max(0, self.open_sell_orders_amount - left_amount)
                    self.open_sell_orders_num = max(0, self.open_sell_orders_num - 1)
                    sorder1['inactive'] = 1 

        self.clear_order_list()

    # ABOTS-105: common for cancelling sells
    def cancel_sell_for_risk(self):
        self.cancel_on_side('SELL')
        self.open_sell_orders_amount = 0.0
        self.open_sell_orders_num = 0

    # ABOTS-105: common for cancelling buys
    def cancel_buy_for_risk(self):
        self.cancel_on_side('BUY')
        self.open_buy_orders_amount = 0.0
        self.open_buy_orders_num = 0

    # ABOTS-105: refactored to support both altcoin and quotecoin
    def cancel_for_risk(self):
        ## cancel due to risk //
        if self.imbalance_btc > self.imbalance_thresh:
            ## stop one side posting
            self.logger.info(f'imbalance {self.quotecoin} threshold {self.imbalance_thresh} reached (at {self.imbalance_btc}), cancel all sell orders.')
            self.cancel_sell_for_risk()
        if self.imbalance_alt < -self.imbalance_thresh_alt:
            self.logger.info(f'imbalance {self.altcoin} threshold {self.imbalance_thresh_alt} reached (at {self.imbalance_alt}), cancel all sell orders.')
            self.cancel_sell_for_risk()
        if self.imbalance_btc < -self.imbalance_thresh:
            self.logger.info(f'imbalance {self.quotecoin} threshold {self.imbalance_thresh} reached (at {self.imbalance_btc}), cancel all buy orders.')
            self.cancel_buy_for_risk()
        if self.imbalance_alt > self.imbalance_thresh_alt:
            self.logger.info(f'imbalance {self.altcoin} threshold {self.imbalance_thresh_alt} reached (at {self.imbalance_alt}), cancel all buy orders.')
            self.cancel_buy_for_risk()
        
        ## open amount exceeded, cancel from furthest order till within risk
        if self.open_buy_orders_amount > self.open_max_amount:
            numOrders = len(self.icebergOrderbuylist)
            for idx in list(reversed(range(numOrders))):
                border = self.icebergOrderbuylist[idx]
                borderid = border['orderid']
                borderdetails = self.brokers[0].client.get_order_details(self.tradingpair, 'BUY_LIMIT', borderid, data_format='legacy')
                if self.is_orderdetails_valid(borderdetails):
                    self.controlled_cancel(self.brokers[0],self.tradingpair,'BUY_LIMIT',borderid)
                    self.total_cancels += 1
                    border['inactive'] = 1
                    ## assume cancel successfully
                    amt = border['ordersize'] - border['accumFilledAmount']
                    self.open_buy_orders_amount -= amt
                if self.open_buy_orders_amount <= self.open_max_amount:
                    break
                 
        if self.open_sell_orders_amount > self.open_max_amount:
            ## cancel from the farest prices
            numOrders = len(self.icebergOrderselllist)
            for idx in list(reversed(range(numOrders))):
                sorder = self.icebergOrderselllist[idx]
                sorderid = sorder['orderid']
                sorderdetails = self.brokers[0].client.get_order_details(self.tradingpair, 'SELL_LIMIT', sorderid, data_format='legacy')
                if self.is_orderdetails_valid(sorderdetails):
                    self.controlled_cancel(self.brokers[0],self.tradingpair, 'SELL_LIMIT', sorderid)
                    self.total_cancels += 1
                    sorder['inactive'] = 1
                    ## assume cancel success
                    self.open_sell_orders_amount -= sorder['ordersize'] - sorder['accumFilledAmount']
                if self.open_sell_orders_amount <= self.open_max_amount:
                    break
 
        ## clean inactive orders:
        self.clear_order_list()   

    def do_market_making(self):
        self.logger.debug('[do_market_making]::')
        if self.min_sep <= self.epsilon:
            self.update_min_separation()
            self.logger.info('[do_market_making]:: minimum separation for pair %s is %s, min order value is %s' % (self.tradingpair, str(self.min_sep), str(self.min_ordervalue)))
        
        if self.minimum_spread_final <= self.epsilon:
            self.minimum_spread_final = self.minimum_spread
            if self.minimum_spread_bps > 0:
                min_spread = self.minimum_spread_bps / 10000.0 * self.current_mid() ##bps need to convert to real decimal
                self.minimum_spread_final = min(min_spread, self.minimum_spread_final)
            self.minimum_spread_final = round(self.minimum_spread_final, self.orderpricerounding)
            self.logger.info('[do_market_making]:: minimum spread for pair %s is %s' % (self.tradingpair, str(self.minimum_spread_final)))

        self.logger.debug('[do_market_making]:: update_status_for_layers()')
        self.update_status_for_layers()
        self.update_coin_balances()
        # ABOTS-105: updated message
        self.logger.info('[do_market_making]:: %s%s imbalance_btc=%s, imbalance_alt=%s' % (self.altcoin,self.quotecoin,str(self.imbalance_btc),str(self.imbalance_alt)))
        ## check if any orders needs to be cancelled (due to alpha/risk etc), and update record accordingly
        ## check if we need to cancel furthest posted order to save room for closer post
        self.logger.debug('[do_market_making]:: clear_and_cancel_by_alpha()')
        self.clear_and_cancel_by_alpha()
        self.logger.debug('[do_market_making]:: cancel_for_chasing()')
        self.cancel_for_chasing()      
        self.logger.debug('[do_market_making]:: cancel_for_risk()')
        self.cancel_for_risk()
        
        ## check if any orders needs to be posted, and update active orders list accordingly
        self.logger.debug('[do_market_making]:: do_posting()')
        self.do_posting()

    # ABOTS-110: check prices for le bot
    def prices_are_out_of_range(self, price: float):
        if price >= self.minorderprice and price <= self.maxorderprice:
            self.logger.debug('price=%s is in between %s and %s' % (str(price), str(self.minorderprice), str(self.maxorderprice)))
            return False
        else:
            self.logger.debug('price=%s is in outside range (%s, %s)' % (str(price), str(self.minorderprice), str(self.maxorderprice)))
            return True

    def do_posting_min_spread(self):
        ##min spread improvment
        if self.minimum_spread_final > self.epsilon and self.current_spread() > self.minimum_spread_final + self.epsilon:
            ## when min spread is not satisfied, improving current bid/ask is necessary
            
            min_spread = self.minimum_spread_final
            price_buy0 = self.current_mid() - min_spread / 2.0
            price_sell0= self.current_mid() + min_spread / 2.0
            price_buy = min(price_buy0, self.currentdepthbid[0]['price'] + self.tierdepth)
            if len(self.icebergOrderbuylist) > 0:
                price_buy = min(price_buy, self.icebergOrderbuylist[0]['orderprice'] + self.tierdepth)
            price_sell= max(price_sell0, self.currentdepthask[0]['price'] - self.tierdepth)
            if len(self.icebergOrderselllist) > 0:
                price_sell= max(price_sell, self.icebergOrderselllist[0]['orderprice'] - self.tierdepth)
            price_buy = round(price_buy, self.orderpricerounding)
            price_sell= round(price_sell,self.orderpricerounding)

            self.logger.debug('[do posting]:: current_spread %s, is less than min spread %s / triggering price buy %s, price sell %s' % (str(self.current_spread()), str(min_spread), str(price_buy), str(price_sell) ))
            ordersize = self.generate_buyorder_size(price_buy)
            ordersize = min(ordersize, self.open_max_amount - self.open_buy_orders_amount)
            minsize = self.get_minsize_from_minvalue(price_buy)
            if ordersize <= 0 or (minsize > 0 and ordersize < minsize) or self.should_cancel_by_risk(price_buy, ordersize, 'BUY'):
                self.logger.info('[do_posting]:: ordersize invalid %s, minsize = %s (min spread)' % (str(ordersize), str(minsize)) )
            elif ( not self.cancel_for_chase_buy_flag and \
            (len(self.icebergOrderbuylist) == 0 or price_buy - self.icebergOrderbuylist[0]['orderprice'] >= self.min_sep or
            (abs(price_buy - price_buy0) < self.epsilon and price_buy - self.icebergOrderbuylist[0]['orderprice'] > self.epsilon)) ):
                # ABOTS-110: check prices for le bot
                if self.prices_are_out_of_range(price_buy):
                    return
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
            if ordersize <= 0 or (minsize > 0 and ordersize < minsize) or self.should_cancel_by_risk(price_sell, ordersize, 'SELL'):
                self.logger.info('[do_posting]:: ordersize invalid %s, minsize=%s (min spread)' % (str(ordersize), str(minsize)) )
            elif ( not self.cancel_for_chase_sell_flag and \
            (len(self.icebergOrderselllist) == 0 or price_sell - self.icebergOrderselllist[0]['orderprice'] <= -self.min_sep or 
            (abs(price_sell - price_sell0) < self.epsilon and price_sell - self.icebergOrderselllist[0]['orderprice'] < -self.epsilon)) ):
                # ABOTS-110: check prices for le bot
                if self.prices_are_out_of_range(price_sell):
                    return
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

    def do_posting(self):
        self.logger.debug("[do_posting]::")
        ###########################################
        ## Let's first do min spread improvment 
        self.do_posting_min_spread()
        ##########################################
        ### reset cancel for chase flag to false (when it's true, min spread order posting would delay as chase will do it anyways)
        self.cancel_for_chase_buy_flag = False
        self.cancel_for_chase_sell_flag = False
        ##########################################
        min_sep = self.min_sep
        dlvl = 0
        self.logger.debug('[do_posting]:: start posting buy orders - dlvl=%s,listlength=%s' % (str(dlvl), str(len(self.icebergOrderbuylist)) ))
        while len(self.icebergOrderbuylist) < self.maxquotinglevels and dlvl < self.quotinglevels: 
            ## if already have 5 levels of quotes on the market already, do not post
            ## otherwise, post with minimum tick sep??
            numOrders = len(self.icebergOrderbuylist)
            orderprice = self.get_posting_price('BUY',dlvl)
            if orderprice <= 0.0:
                self.logger.critical('[do_posting]:: buy orderprice not valid')
                dlvl += 1
                continue
            pricelist = [order['orderprice'] for order in self.icebergOrderbuylist]
            self.logger.info('[do_posting]:: buy price list : %s, orderprice : %s, dlvl : %s' % (str(pricelist), str(orderprice), str(dlvl)))
            ## initial post
            if numOrders > 0:
                if any(abs(orderprice - iprice) < 1e-10 for iprice in pricelist): ## do not post if price level already there
                    dlvl += 1
                    self.logger.info('[do_posting]:: same price exists BUY, not posting!')
                    continue
                if pricelist[::-1] != sorted(pricelist):
                    self.logger.critical("[do_posting]:: something is not right, order prices are not sorted!")
                    return False
                pricelist.sort()
                ins_index = bisect(pricelist, orderprice)
            else:
                ins_index = 0

            if numOrders == 0 or ins_index == 0 or (ins_index == numOrders and orderprice - pricelist[-1] > min_sep) or (orderprice - pricelist[ins_index-1] > min_sep and orderprice - pricelist[ins_index] < -min_sep) :
                if numOrders > 0 and ins_index == 0 and pricelist[ins_index] - orderprice < min_sep:
                    orderprice = round(pricelist[ins_index] - min_sep, self.orderpricerounding)
                ##post

                ordersize = self.generate_buyorder_size(orderprice)
                ordersize = min(ordersize, self.open_max_amount - self.open_buy_orders_amount)
                minsize = self.get_minsize_from_minvalue(orderprice)
                if ordersize <= 0 or (minsize > 0 and ordersize < minsize) or self.should_cancel(orderprice, ordersize, 'BUY'):
                    dlvl += 1
                    self.logger.info('[do_posting]:: pair %s ordersize invalid %s minsize = %s, or BUY orderprice too aggressive' % (self.tradingpair, str(ordersize), str(minsize)))
                    continue
                sendorderid = self.generate_order(orderprice, ordersize, sellbroker=None, buybroker=self.brokers[0])
                self.logger.debug('[do_posting]:: pair %s prepare to send buy order price=%s, size=%s, orderid=%s' % (self.tradingpair, str(orderprice), str(ordersize), str(sendorderid)))
                if sendorderid is not None:
                    icebergorder = {"tiernumber": dlvl,
                                    "ordersize": ordersize,
                                    "orderprice":orderprice,
                                    "orderside":"BUY",
                                    "orderid":sendorderid,
                                    "accumFilledAmount":0,
                                    "lastAccumFilledAmout":0,
                                    "inactive": 0,
                                    "orderTime": time.time()
                                    }
                    self.icebergOrderbuylist.insert(numOrders - ins_index, icebergorder)
                    self.open_buy_orders_amount += ordersize
                    self.logger.info('[do_posting]:: %s buy order sent: %s' % (self.tradingpair, str(icebergorder)))
            dlvl = dlvl + 1

        dlvl = 0
        self.logger.debug('[do_posting]:: start posting sell orders - dlvl=%s,listlength=%s' % (str(dlvl), str(len(self.icebergOrderselllist)) ))
        while len(self.icebergOrderselllist) < self.maxquotinglevels and dlvl < self.quotinglevels: 
            numOrders = len(self.icebergOrderselllist)
            orderprice = self.get_posting_price("SELL",dlvl)
            if orderprice <= 0.0:
                self.logger.critical('[do_posting]:: sell orderprice not valid!')
                dlvl += 1
                continue
            pricelist = [order['orderprice'] for order in self.icebergOrderselllist]
            self.logger.info('[do_posting]:: sell price list : %s, orderprice : %s, dlvl : %s' % (str(pricelist), str(orderprice), str(dlvl)))
            if numOrders > 0:
                if any(abs(orderprice - iprice) < 1e-10 for iprice in pricelist): ## do not post if price level already there
                    dlvl += 1
                    self.logger.info('[do_posting]:: same price exists SELL, not posting!')
                    continue
                if pricelist != sorted(pricelist):
                    self.logger.critical("[do_posting]:: something is not right, sell order prices are not sorted!")
                    return False
                ins_index = bisect(pricelist, orderprice)
            else:
                ins_index = 0
                
            if numOrders == 0 or (ins_index == 0 and orderprice - pricelist[0] < -min_sep) or ins_index == numOrders or (orderprice - pricelist[ins_index-1] > min_sep and orderprice - pricelist[ins_index] < -min_sep) :
                if numOrders > 0 and ins_index == numOrders and numOrders > 0 and orderprice - pricelist[ins_index - 1] < min_sep:
                    orderprice = round(pricelist[ins_index - 1] + min_sep, self.orderpricerounding)             
                ##post
                ordersize = self.generate_sellorder_size(orderprice)
                ordersize = min(ordersize, self.open_max_amount - self.open_sell_orders_amount)
                minsize = self.get_minsize_from_minvalue(orderprice)
                if ordersize <= 0 or (minsize > 0 and ordersize < minsize) or self.should_cancel(orderprice, ordersize, 'SELL'):
                    dlvl += 1
                    self.logger.info('[do_posting]:: pair %s ordersize invalid %s, minsize = %s, or SELL orderprice too aggressive' % (self.tradingpair, str(ordersize), str(minsize)) )
                    continue
                sendorderid = self.generate_order(orderprice, ordersize, sellbroker=self.brokers[0], buybroker=None)
                self.logger.debug('[do_posting]:: pair %s prepare to send sell order price=%s, size=%s, orderid=%s' % (self.tradingpair, str(orderprice), str(ordersize), str(sendorderid)))
                if sendorderid is not None:
                    icebergorder = {"tiernumber": dlvl,
                                    "ordersize": ordersize,
                                    "orderprice":orderprice,
                                    "orderside":"SELL",
                                    "orderid":sendorderid,
                                    "accumFilledAmount":0,
                                    "lastAccumFilledAmout":0,
                                    "inactive": 0,
                                    "orderTime": time.time()
                                    }
                    self.icebergOrderselllist.insert(ins_index, icebergorder)
                    self.open_sell_orders_amount += ordersize
                    self.logger.info('[do_posting]:: %s sell order sent: %s' % (self.tradingpair, str(icebergorder)))
            dlvl = dlvl + 1

    def _move_to_cancel_list(self, order):
        """ remove records from broker list """
        self.cancel_list.append(order)

    def monitor_until_canceled(self):
        """ monitor records until they are completed """
        try:
            for order in self.cancel_list:
                side, fside, orderid = order['orderside'], 'BUY_LIMIT' if order['orderside'] == 'BUY' else 'SELL_LIMIT', order['orderid']
                orderdetails = self.brokers[0].client.get_order_details(self.tradingpair, fside, orderid, data_format='legacy')

                if self.is_orderdetails_valid(orderdetails):
                    order['lastAccumFilledAmount'] = order['accumFilledAmount']
                    order['accumFilledAmount'] = float(orderdetails.get('FilledAmount',0))
                    executed_qty = order['accumFilledAmount'] - order['lastAccumFilledAmount']
                    self.update_imbalance(order['orderside'],executed_qty,order['orderprice'])
                    self.logger.debug(f"update imbalance from order monitoring :: {order['orderid']} {order['accumFilledAmount']} - {order['lastAccumFilledAmount']} = {order['accumFilledAmount'] - order['lastAccumFilledAmount']}, {order['orderprice']}")
                    if orderdetails.get('Status', None) in ['Completed', 'Canceled']:
                        order['inactive'] = 9
                        try:
                            if side == 'BUY':
                                self.brokers[0].buyorderlist.remove(orderid)
                            if side == 'SELL':
                                self.brokers[0].sellorderlist.remove(orderid)
                        except:
                            self.logger.warning(f"unable to remove {orderid} from {side} list")
        except Exception as e:
                self.logger.error(f"{e}")

        self.cancel_list = [order for order in self.cancel_list if order['inactive'] != 9]

    def controlled_cancel(self, broker, pair, order_type, order_id):
        """ controlled cancel with information return """
        result = broker.client.cancel_wait(order_id)[0]
        if not result:
            self.logger.warning(f"cancel order {order_id} unsuccessful")
        else:
            self.logger.debug(f"cancel order {order_id} successful")

    def exit_processing(self):
        """make sure to exit the bot safely, no opening orders left"""
        for buyorderid in self.brokers[0].buyorderlist:
            self.logger.debug("cancel buy orders: %s" % buyorderid)
            self.controlled_cancel(self.brokers[0],self.tradingpair, 'BUY_LIMIT', buyorderid)
            self.total_cancels += 1
            time.sleep(0.05)
        
        for sellorderid in self.brokers[0].sellorderlist:
            self.logger.debug("cancel sell orders: %s" % sellorderid)
            self.controlled_cancel(self.brokers[0],self.tradingpair, 'SELL_LIMIT', sellorderid)
            self.total_cancels += 1
            time.sleep(0.05)

        if self.clerk and config.RECORD_TO_DB:
            self.update_order_history(True)

        if self.clerk and config.RECORD_TO_DB:
            botdetails = {}
            try:
                botdetails['Parameters'] = self.get_config_data()
                botdetails['EndTime'] = datetime.datetime.now(datetime.timezone.utc)
                botdetails['Status'] = 'Exited'
                self.clerk.update_bot_info_sqla(botdetails, self.botid)
            except Exception as e:
                self.logger.error("Failed updating Bot info at exiting due to: {0}".format(e))
                return False
            else:
                pass

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
    
    ###########################################################
    # TODO: all these should go into common Service Bot layer
    ###########################################################

    def get_config_data(self):
        config = {
            'sampleInterval':self.sampleinterval,
            'movingAverageWindow':self.movingaverage, 
            'quotingLevel':self.quotinglevels, 
            'maxQuotingLevels':self.maxquotinglevels,
            'taskOrderSize': self.taskordersize,
            'tierDepth':self.tierdepth, 
            'quotingEdge':self.quotingedge, 
            'emaHalfLife': self.ema_half_life,
            'postingVolScale': self.vol_scale,
            'minSeparationFraction': self.min_sep_frac, 
            'ordersizeScaler': self.ordersize_scaler, 
            'minimumSpread': self.minimum_spread, 
            'minimumSpreadBps': self.minimum_spread_bps,
            'alphaWeights': self.alpha_weights_str,
            'maxLongQuantity': self.maxlonguantity,
            'maxShortQuantity': self.maxshortquantity,
            'openMaxAmount': self.open_max_amount,
            'maxImbalanceBtc': self.imbalance_thresh,
            'maxImbalanceAlt': self.imbalance_thresh_alt,       # ABOTS-105
            'altcoinOrderRandomRange': self.orderandomrange,
            'minOrderPrice':self.minorderprice, 
            'maxOrderPrice':self.maxorderprice,
            'reservedAltcoin': self.reservedaltcoin, 
            'reservedQuotecoin': self.reservedquotecoin,
            'initialImbalance': self.initial_imbalance,
            'initialImbalanceAlt': self.initial_imbalance_alt,  # ABOTS-105
            'minimumOrderSize': self.minimum_order_size,
            'minimumOrderNotional': self.minimum_order_notional
        }
        return json.dumps(config)

    def register_bot(self):
        """register Bot during initialization, throw errors if CoinClerk instance doesn't exist"""
        botdetails ={}
        # self.logger.debug("=============================== 1409")
        # try:
        #     botdetails['IpAddr'] = requests.get("https://api.ipify.org/?format=json").json()['ip']
        # except:
        #     pass
        # self.logger.debug("=============================== 1414")
        try:
            botdetails['Parameters'] = self.get_config_data()
        except Exception as e:
            self.logger.error("Failed registering Bot due to: {0}".format(e))
        try:
            botdetails['ProcessID'] = str(time.time())
            botdetails['PidName'] = str(self.brokers[0].get_exchangename() + '-' + self.tradingpair)
            botdetails['Status'] = 'Launched'
            # in the meantime, check the existence of CoinClerk instance
            self.botid = self.clerk.insert_bot_info_sqla(botdetails)
        except Exception as e:
            self.logger.error("Failed registering Bot due to: {0}".format(e))
            raise e
        else:
            if not self.botid:
                self.logger.error("Meaningless Bot ID, registration failed!")
                return False

    def sanity_check(self):
        """make sure bot config is correct"""
        botdetails = {}
        # if botdetails:
        if self.clerk and config.RECORD_TO_DB:
            try:
                botdetails['Parameters'] = self.get_config_data()
                botdetails['Status'] = 'Running'
                self.clerk.update_bot_info_sqla(botdetails, self.botid)
            except Exception as e:
                self.logger.error("Failed updating Bot info before kicking-off due to: {0}".format(e))
                return False

        return self.passanitycheck

    def record_order_to_db(self, broker, tradingpair, ordertype, orderid):
        orderdetails, retries = None, 0
        while orderdetails is None and retries < 5:
            orderdetails = broker.client.get_order_details(tradingpair, ordertype, orderid, data_format='legacy')
            if retries > 0:
                time.sleep(0.2)
            retries += 1
        if orderdetails:
            self.logger.debug(f'add to db {orderdetails}')
            orderdetails['Source'] = 'Liquidity'
            orderdetails['BotID'] = self.botid
            self.clerk.insert_execution_order_sqla(orderdetails)

            if self.clerk and config.RECORD_TO_CSV and orderdetails.get('Status', None) == 'Completed' :
                del orderdetails['Source']
                del orderdetails['BotID'] 
                self.record_order_to_csv(orderdetails)

        else:
            ghost_orderdetails = self.create_ghost_order(broker, tradingpair, ordertype, orderid, self.botid)
            self.logger.debug(f'add to db {ghost_orderdetails} (ghost order)')
            self.clerk.insert_execution_order_sqla(ghost_orderdetails)

    def update_order_history(self, dump_to_csv = False):
        if not self.clerk:
            return
        records = self.clerk.get_record_from_db(self.tradingpair, self.brokers[0].get_exchangename(), self.brokers[0].get_account_id(), source='Liquidity')

        for record in records:

            orderdetails, retries = None, 0
            side = 'BUY_LIMIT' if record.LongShort == 'Long' else 'SELL_LIMIT'
            while orderdetails is None and retries < 5:
                orderdetails = self.brokers[0].client.get_order_details(record.TradingPair, side, record.OrderID, data_format='legacy')
                if retries > 0:
                    time.sleep(0.2)
                retries += 1

            if orderdetails is not None and isinstance(orderdetails, dict) and orderdetails != {} and orderdetails.get('Price',-1) > 0:
                self.clerk.update_execution_order_sqla(orderdetails, record.ID)
                    
                if self.clerk and config.RECORD_TO_CSV and orderdetails.get('Status',None) == 'Completed' or dump_to_csv:
                    self.record_order_to_csv(orderdetails)

    def get_latest_orders(self, limit=10, filename=None):
        results = []
        if self.clerk:
            dataset = self.clerk.get_record_from_db(self.tradingpair, self.brokers[0].get_exchangename(), self.brokers[0].get_account_id(), num=10, status=['Completed','Not Completed','Canceled'], source='Liquidity', botid=self.botid)
        for data in dataset:
            results.append(data.__dict__)
        return results

    def create_ghost_order(self, broker, tradingpair, ordertype, orderid, botid=0):
        """create a ghost order when no Order details can be retrieved"""
        orderdetails = {}
        try:
            orderdetails['OrderID'] = orderid
            orderdetails['TimeStamp'] = int(datetime.datetime.now().timestamp()*1000)
            orderdetails['TradingPair'] = tradingpair
            orderdetails['Exchange'] = broker.get_exchangename()
            orderdetails['AccountKey'] = broker.get_account_id()
            orderdetails['LongShort'] = 'Long' if ordertype == 'BUY_LIMIT' else 'Short'
            orderdetails['Amount'] = -1
            orderdetails['FilledAmount'] = -1
            orderdetails['Price'] = -1
            orderdetails['FilledCashAmount'] = -1
            orderdetails['Status'] = 'Ghost'
            orderdetails['SellCommission'] = -1
            orderdetails['BuyCommission'] = -1
            orderdetails['NetFilledAmount'] = -1
            orderdetails['NetFilledCashAmount'] = -1
            orderdetails['Source'] = 'Liquidity'
            orderdetails['BotID'] = botid
        except Exception as e:
            self.logger.error("Failed creating ghost Order {0} due to: {1}".format(orderid, e))
            return None
        else:
            return orderdetails

    def record_order_to_csv(self, orderdetails):
        """record order results to a csv file"""

        if isinstance(orderdetails, dict) and len(orderdetails) > 0:
            filename = self.altcoin.upper() + '_' + self.quotecoin.upper() + '_' + self.brokers[0].get_exchangename() +'_Liquidity_log.csv'
            if not os.path.isfile(filename):
                with open(filename, 'w') as csvfile:
                    headers = ['OrderID', 'TimeStamp', 'TradingPair', 'Exchange', 'AccountKey', 'LongShort', 'Amount', 'FilledAmount', 'Price', 'Status','FilledCashAmount', 'BuyCommission', 'NetFilledCashAmount', 'SellCommission', 'NetFilledAmount']
                    csvwriter = csv.DictWriter(csvfile, delimiter=',', lineterminator='\n', fieldnames=headers)
                    csvwriter.writeheader()
                    csvwriter.writerow(orderdetails)
            else:
                with open(filename, 'a') as csvfile:
                    headers = ['OrderID', 'TimeStamp', 'TradingPair', 'Exchange', 'AccountKey', 'LongShort', 'Amount', 'FilledAmount', 'Price', 'Status','FilledCashAmount', 'BuyCommission', 'NetFilledCashAmount', 'SellCommission', 'NetFilledAmount']
                    csvwriter = csv.DictWriter(csvfile, delimiter=',', lineterminator='\n', fieldnames=headers)
                    csvwriter.writerow(orderdetails)
        else:
            # TODO logging
            pass        
        
    def read_order_from_csv(self, filename=None):
        """retrieve order results from a saved csv file"""
        orderdictlist = []

        try:
            if not filename:
                filename = self.altcoin.upper() + '_' + self.quotecoin.upper() + '_' + self.brokers[0].get_exchangename() +'_Liquidity_log.csv'

            with open(filename, 'r') as csvfile:
                csvreader = csv.DictReader(csvfile)
                orderdictlist = [order for order in csvreader]
        except IOError:
            self.logger.warning("The order list csv file %s doesn't exist" % filename)
            return False
        else:
            return orderdictlist


    def Go(self):
        """ """
        # first build up a sample with 120 slices
        print('Go -- In the MM Bot')
        self.build_initial_sample()
        self.initialize_ema_quantities(self.midpxsample, self.retsample, self.spreadsample)
        # start
        endtime = time.time() + 600
        
        while time.time() < endtime:
            if self.tick():
                self.do_market_making()
        
        self.exit_processing()