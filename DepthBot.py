#########################################################################################
# Date: 2018.05
# Auther: happyshiller 
# Email: happyshiller@gmail.com
# Github: https://github.com/happyshiller/Altonomy
# Function: quoting depth
#########################################################################################

import os
import csv
import time
import random
import json
import datetime
import requests
import pandas as pd # to manipulate dataframes
import numpy as np # to manipulate arrays
from . import config
from .ServiceBot import ServiceBot


class DepthBot(ServiceBot):
    """ExecutionBot is a generic class for setting depth"""

    def __init__(self, brokers, clerk, altcoin, quotecoin, logger=None):
        """Initialize attributes of the ServiceBot, then attributes specific to DepthBot"""
        super().__init__(brokers, clerk, altcoin, quotecoin, logger)
        # sample size
        self.samplesize = 0
        # take 10 minutes moving average, input in minutes from UI
        self.movingaveragewindow = 10
        # take a slice (bid ask - mid) every 5s
        self.sampleinterval = 5
        # how many seconds does it cost to build the sample?
        # size * interval
        #self.sampleruntime = 600
        # a list of historical mid price
        self.midpxsample = []
        # the rolling moving average price
        self.movingaverage = 0.0
        # quoting edge, which add to moving average price
        self.quotingedge = 0.0
        # how many levels Algo quotes in the market
        # 5 level at each side
        self.quotinglevels = 5
        # split the total amount into 10 piece
        self.quotingpieces = 10
        # the depth interval between each tier
        self.tierdepth = 0.0
        # the edge subtracted/added on MA when there's already quotes out
        self.threshold = 0.0
        # which side(s) of the orderbook Algo quotes
        # 0: bid, 1: ask, 2: both bid & ask
        self.quotingside = 'BOTH'
        # sample updating time stamp
        self.sample_ts = 0
        #order list of all quoting levels
        self.icebergOrderbuylist = []
        self.icebergOrderselllist = []
        #store moving average
        self.storemovingaverage = 0.0
        self.currentmovingaverage = 0.0
        #current depth book
        self.currentdepthbid = [{ 'price': 0.0, 'volume': 0.0 }]
        self.currentdepthask = [{ 'price': 0.0, 'volume': 0.0 }]
        #store current depth book
        #self.storecurrentdepthbid = 0.0
        #self.storecurrentdepthask = 0.0
        self.buy_remainingquantity_total = 0.0
        self.sell_remainingquantity_total = 0.0
        self.order_history = []
        self.last_run_time = 0
        self.max_upd_frequency = 0.0
        self.sleep_time = 0.0
        self.total_orders_limit = 200
        self.total_orders_placed = 0
        self.message = ''
        # self.minimum_order_size = 0.0
        if self.clerk and config.RECORD_TO_DB:
            self.register_bot()        

    # def update_minimum_order_size(self, min_size=0.0):
    #     """update the update frequency getting info from exchange API"""
    #     try:
    #         fmin_size = float(min_size)
    #         if fmin_size >= 0.0:
    #             self.minimum_order_size = fmin_size
    #         else:
    #             self.passanitycheck = False
    #             self.logger.error(f"The min order size {min_size} is not a positive float, updating failed!")
    #             return False
    #     except ValueError:
    #         self.passanitycheck = False
    #         self.logger.error(f"The min order size {min_size} is not a float, updating failed!\n")
    #         return False

    def update_scale_factor(self, scale_factor=0.0):
        """update the scale factor for order sizes from exchange API"""
        try:
            fscale_factor = float(scale_factor)
            if fscale_factor >= 0.0:
                self.scale_factor = fscale_factor
            else:
                self.passanitycheck = False
                self.logger.error(f"The scale factor {scale_factor} is not a positive float, updating failed!")
                return False
        except ValueError:
            self.passanitycheck = False
            self.logger.error(f"The scale factor {scale_factor} is not a float, updating failed!\n")
            return False

    def update_total_orders_limit(self, orders_limit=200):
        """update the update frequency getting info from exchange API"""
        try:
            itotal_orders_limit = int(float(orders_limit))
            if itotal_orders_limit >= 0:
                self.total_orders_limit = itotal_orders_limit
            else:
                self.passanitycheck = False
                self.logger.error(f"The orders limit {orders_limit} is not a positive integer, updating failed!")
                return False
        except ValueError:
            self.passanitycheck = False
            self.logger.error(f"The orders list {orders_limit} is not an integer, updating failed!\n")
            return False

    def update_max_upd_frequency(self, update_timing=0.0):
        """update the update frequency getting info from exchange API"""
        try:
            fupdate_timing = float(update_timing)
            if fupdate_timing >= 0.0:
                self.max_upd_frequency = fupdate_timing
                self.calculate_sleep_time()
            else:
                self.passanitycheck = False
                self.logger.error(f"The update frequency {update_timing} is not a positive number, updating failed!")
                return False
        except ValueError:
            self.passanitycheck = False
            self.logger.error(f"The update frequency {update_timing} is not float, updating failed!\n")
            return False


    def calculate_sleep_time(self):
        """claculate sleep time when tick function returned False"""
        if self.max_upd_frequency >= 0.0:
            self.sleep_time = self.max_upd_frequency * 0.5
        else:
            self.passanitycheck = False
            self.logger.error(f"Update frequency {self.max_upd_frequency} should be positive.")
            return False


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
            
            
    def update_movingaverage_threshold(self, threshold=0.0):
        """"""
        try:
            fedge = float(threshold)
            if fedge > 0.0:
                self.threshold = round(fedge, self.orderpricerounding)
            else:
                self.passanitycheck = False
                self.logger.error("The cross book edge {eg} is not a positive number, updating failed!".format(eg=threshold))
                return False
        except ValueError:
            self.logger.error("The cross book edge {eg} is not float, updating failed!\n".format(eg=threshold))
            return False


    def update_quoting_levels(self, quotinglevels=5):
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
    

    def update_quoting_side(self, side='BOTH'):
        """
        :side: 0: bid, 1: ask, 2: both bid & ask
        :update how many levels will be quoted in the market
        """
        if side == 'BUY' or side == 'SELL' or side == 'BOTH':
            self.quotingside = side
        else:
            self.passanitycheck = False
            self.logger.error("The quoting side {side} is not meaningful, updating failed!".format(side=side))
            return False
    
    
    def update_depth_orders_quantity(self, quantity):
        """
        :quantity: the volume target within 24 hours
        :update the task quantity 
        """
        try:
            fquantity = float(quantity)
            if fquantity > 0.0:
                self.taskquantity = fquantity
                # calculate order size for each tier
                self.calculate_task_order_size()
            else:
                self.logger.error("The task quantity %s is not positive number, updating failed!\n" % str(quantity))
                self.passanitycheck = False
                return False
        except ValueError:
            self.logger.error("The task quantity %s is not float, updating failed!\n" % str(quantity))
            self.passanitycheck = False
            return False


    def get_config_data(self):
        config = {
            'sampleInterval':self.sampleinterval,
            'movingAverageWindow':self.movingaverage, 
            'quotingLevel':self.quotinglevels, 
            'quotingPieces':self.quotingpieces,
            'tierDepth':self.tierdepth, 
            'quotingEdge':self.quotingedge, 
            'movingAverageThreshold':self.threshold,
            'quotingSide':self.quotingside,
            'depthOrdersQuantity':self.taskquantity, 
            'altcoinOrderRandomRange': self.orderandomrange,
            'minOrderPrice':self.minorderprice, 
            'maxOrderPrice':self.maxorderprice,
            'reservedAltcoin': self.reservedaltcoin, 
            'reservedQuotecoin': self.reservedquotecoin,
            'maxUpdateFrequency': self.max_upd_frequency,
            'totalOrdersLimit': self.total_orders_limit,
            'minimumOrderSize': self.minimum_order_size,
            'minimumOrderNotional': self.minimum_order_notional
        }
        return json.dumps(config)

    def register_bot(self):
        """register Bot during initialization, throw errors if CoinClerk instance doesn't exist"""
        botdetails ={}
        # try:
        #     botdetails['IpAddr'] = requests.get("https://api.ipify.org/?format=json").json()['ip']
        # except:
        #     pass
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

    def calculate_task_order_size(self):
        """calculate the order size based on total task quantity and spliting sizes"""
        if self.taskquantity > 0 and self.quotinglevels > 0:
            self.taskordersize = round(float(self.taskquantity / self.quotingpieces), self.ordersizerounding)
        else:
            self.passanitycheck = False
            self.logger.error("Total task quantity {qty} or quoting levels {lvl} should be positive.".format(qty=self.taskquantity, lvl=self.quotinglevels))
            return False
    

    def generate_buyorder_size(self, tierlevel=1):
        """"""        
        randomordersize = min(self.taskordersize * (1 + (tierlevel - 1) * self.scale_factor) * random.uniform(1.0-self.orderandomrange, 1.0+self.orderandomrange), self.taskquantity)
        
        if randomordersize:
            self.logger.info("The buy order size is %s" % str(round(randomordersize, self.ordersizerounding)))
            return round(randomordersize, self.ordersizerounding)
        else:
            self.logger.info("The buy order size is %s" % str(round(self.taskordersize, self.ordersizerounding)))
            return round(self.taskordersize, self.ordersizerounding) 


    def generate_sellorder_size(self, tierlevel=1):
        """"""        
        randomordersize = min(self.taskordersize * (1 + (tierlevel - 1) * self.scale_factor) * random.uniform(1.0-self.orderandomrange, 1.0+self.orderandomrange), self.taskquantity)
        
        if randomordersize:
            self.logger.info("The sell order size is %s" % str(round(randomordersize, self.ordersizerounding)))
            return round(randomordersize, self.ordersizerounding)
        else:
            self.logger.info("The sell order size is %s" % str(round(self.taskordersize, self.ordersizerounding)))
            return round(self.taskordersize, self.ordersizerounding) 


    def reject_outliers(self, datalist, sigma=2):
        """clean sample data, remove outliers from historical mid price list"""
        # P(1-sigma) = 13.6% x 2; P(2-sigma) = (13.6% + 34.1%) x 2;
        if isinstance(datalist, list) and len(datalist) > 0:
            datarray = np.array(datalist)
            return datarray[abs(datarray - np.mean(datarray)) < sigma * np.std(datarray)]
        else:
            return None


    def build_midpx_sample(self):
        """build up the sample set of mid price"""
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

            depthbid = self.brokers[0].get_depth_bid_and_size(self.tradingpair, self.quotinglevels)
            depthask = self.brokers[0].get_depth_ask_and_size(self.tradingpair, self.quotinglevels)
            
            if isinstance(depthbid, list) and isinstance(depthask, list):
                midprice = round(float((depthask[0]['price'] + depthbid[0]['price'])/2), self.orderpricerounding)
                self.midpxsample.append(midprice)
            else:
                continue

            timeinterval = time.time() - beginningtime
            if self.sampleinterval > timeinterval:
                time.sleep(self.sampleinterval-timeinterval)
        return self.midpxsample
        
        
    def create_midpx_sample(self):
        """add a new slice to the sample, delete an old one in every 5s"""
        midprice = 0.0
        # bidaskspread = 0.0

        bid0 = self.currentdepthbid[0]['price']
        ask0 = self.currentdepthask[0]['price']
        # no update if there is no meaningful orderbook data    
        if bid0 > 0 and ask0 > 0:
            midprice = round(float((bid0 + ask0)/2.0), self.orderpricerounding)
            self.midpxsample.append(midprice)
            del self.midpxsample[0]
        return self.midpxsample      


    def generate_moving_average_price(self, sampledata):
        """generate rolling moving average price based on sample"""
        movingaverage = 0.0
        midpxlist = sampledata
        #samplesize = self.movingaveragewindow * 60 / self.sampleinterval
        # if len(sampledata) == 0:
        #     sampledata = self.build_midpx_sample()
        # elif len(sampledata) == self.samplesize:
        #     self.update_midpx_sample(sampledata)
        # else:
        #     return False

        if isinstance(midpxlist, list) and len(midpxlist) >= self.samplesize:
            midpxlist = self.reject_outliers(midpxlist)
            if midpxlist is not None:
                midpxlist = midpxlist.tolist()
                if len(midpxlist) > 0:
                    movingaverage = sum(midpxlist) / len(midpxlist)
        else:
            return False   
        return movingaverage    
       

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
            self.message = '[WARNING: Order Size / Notional Below Limit]'
            return None

        if sellbroker:
            limitsellorderID = sellbroker.client.sell_wait(self.tradingpair, orderprice, ordersize)[0]
            self.increase_order_count()
        if buybroker:
            limitbuyorderID = buybroker.client.buy_wait(self.tradingpair, orderprice, ordersize)[0]
            self.increase_order_count()
        # TODO add recording functions
        # time.sleep(0.2)
        if limitsellorderID:
            sellbroker.sellorderlist.append(limitsellorderID)
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
            try:
                if self.clerk and config.RECORD_TO_DB:
                    self.record_order_to_db(buybroker, self.tradingpair, 'BUY_LIMIT', limitbuyorderID)
            except:
                pass
            return limitbuyorderID
            # self.record_order_to_csv(buybroker, self.tradingpair, 'BUY_LIMIT', limitbuyorderID)
            # self.record_order_to_db(buybroker, self.tradingpair, 'BUY_LIMIT', limitbuyorderID)
        ###
    
    def create_single_tier(self,tiernumber,ordersize=-1,totalquantity=-1):
        """create a single tier and generating order,each tier generate an order with details of ordersize,total quantity for this tier,orderprice,quoting side, orderID"""
        filledamount = 0.0
        buyordersize = self.generate_buyorder_size(tiernumber)
        sellordersize = self.generate_sellorder_size(tiernumber)
        if totalquantity < 0:
            totalquantity = self.taskquantity

        if self.quotingside == 'BUY':
                if ordersize < 0:
                    ordersize = buyordersize
                orderprice = self.currentdepthbid[0]['price'] - self.quotingedge - tiernumber * self.tierdepth
                #place order and return order id from exchange
                buyorderID = self.generate_order(orderprice,ordersize,None,self.brokers[0])
                self.logger.debug(f"create single tier {buyorderID}")
                #initial balance for each level
                if buyorderID:
                    icebergorder = [tiernumber,ordersize,totalquantity,orderprice,self.quotingside,buyorderID,filledamount]
                else:
                    ## If new order failed (likely minimum size rejection) - manually assign a fake order detail
                    icebergorder = [tiernumber,0,totalquantity,orderprice,self.quotingside,buyorderID,0]          
                return icebergorder
        else:
                if ordersize < 0:
                    ordersize = sellordersize
                orderprice = self.currentdepthask[0]['price'] + self.quotingedge + tiernumber * self.tierdepth
                sellorderID = self.generate_order(orderprice,ordersize,self.brokers[0],None)
                self.logger.debug(f"create single tier {sellorderID}")
                if sellorderID:
                    icebergorder = [tiernumber,ordersize,totalquantity,orderprice,self.quotingside,sellorderID,filledamount]
                else:
                    icebergorder = [tiernumber,0,totalquantity,orderprice,self.quotingside,sellorderID,0] 
                return icebergorder


    def create_initial_tier(self,quotinglevels):
        """create the intial tier list，one order per tier"""  
        iceburgOrderlist = []
        for i in range(quotinglevels):
            iceburgOrderlist.append(self.create_single_tier(i))
        return iceburgOrderlist


    def get_single_quoting_price(self,tiernumber):
        """get the quoting price for each level for moving average update"""   
        #self.storemovingaverage = self.generate_moving_average_price(self.midpxsample)
        
        if self.quotingside == "BUY":
            orderprice = self.currentdepthbid[0]['price'] - self.quotingedge - tiernumber * self.tierdepth               
            return orderprice
        else:
            orderprice = self.currentdepthask[0]['price'] + self.quotingedge + tiernumber * self.tierdepth
            return orderprice


    def create_quoting_price_list(self):
        """create the quoting price list of each level"""
        quotingpricelist = []
        for i in range(self.quotinglevels):
            quotingpricelist.append(self.get_single_quoting_price(i))
        return quotingpricelist    


    def update_total_quantity_for_layers(self):
        if self.quotingside == 'BUY':
            temp_buy_remainquantity_total = 0.0
            for i in range(self.quotinglevels):
                currentbuyorderid = self.icebergOrderbuylist[i][5]
                if currentbuyorderid:
                    buyorderdetails, retries = None, 0
                    while buyorderdetails is None and retries < 2:
                        buyorderdetails = self.brokers[0].client.get_order_details(self.tradingpair, 'BUY_LIMIT', currentbuyorderid, data_format='legacy')
                        if retries > 0:
                            time.sleep(0.2)
                        retries += 1
                    if buyorderdetails is not None and isinstance(buyorderdetails, dict):
                        #self.icebergOrderbuylist[i][2] -= float(buyorderdetails['FilledAmount'])
                        self.icebergOrderbuylist[i][6] = float(buyorderdetails.get('FilledAmount',0.0))
                    else:
                        self.icebergOrderbuylist[i][6] = 0.0
                    temp_buy_remainquantity_total += round(self.icebergOrderbuylist[i][2] - self.icebergOrderbuylist[i][6],4)
            self.buy_remainingquantity_total = temp_buy_remainquantity_total
        elif self.quotingside == 'SELL':
            temp_sell_remainquantity_total = 0.0
            for i in range(self.quotinglevels):
                currentsellorderid = self.icebergOrderselllist[i][5]
                if currentsellorderid:
                    sellorderdetails, retries = None, 0
                    while sellorderdetails is None and retries < 2:
                        sellorderdetails = self.brokers[0].client.get_order_details(self.tradingpair, 'SELL_LIMIT', currentsellorderid, data_format='legacy')
                        if retries > 0:
                            time.sleep(0.2)
                        retries += 1
                    if sellorderdetails is not None and isinstance(sellorderdetails, dict):
                        self.icebergOrderselllist[i][6] = float(sellorderdetails.get('FilledAmount',0.0))
                    else:
                        self.icebergOrderselllist[i][6] = 0.0
                    temp_sell_remainquantity_total += round(self.icebergOrderselllist[i][2] - self.icebergOrderselllist[i][6],4)
            self.sell_remainingquantity_total = temp_sell_remainquantity_total

    def replace_single_tier(self, currenttier, remainingQuantity, price):
        if self.quotingside == 'BUY':
            buyordersize = self.generate_buyorder_size(currenttier)
            sendorderId = self.generate_order(price,min(remainingQuantity,buyordersize),None,self.brokers[0])
            if sendorderId is not None:
                self.icebergOrderbuylist[currenttier][0] = currenttier
                self.icebergOrderbuylist[currenttier][1] = min(remainingQuantity,buyordersize) #update order size
                self.icebergOrderbuylist[currenttier][2] = remainingQuantity
                self.icebergOrderbuylist[currenttier][3] = price
                self.icebergOrderbuylist[currenttier][5] = sendorderId
                self.icebergOrderbuylist[currenttier][6] = 0.0
            else:
                self.icebergOrderbuylist[currenttier][0] = currenttier
                self.icebergOrderbuylist[currenttier][1] = 0.0
                self.icebergOrderbuylist[currenttier][3] = 0.0
                self.icebergOrderbuylist[currenttier][5] = None
                self.icebergOrderbuylist[currenttier][6] = 0.0
        elif self.quotingside == 'SELL':
            sellordersize = self.generate_sellorder_size(currenttier)
            sendorderId = self.generate_order(price,min(remainingQuantity,sellordersize),self.brokers[0],None)      
            if sendorderId is not None:
                self.icebergOrderselllist[currenttier][0] = currenttier
                self.icebergOrderselllist[currenttier][1] = min(remainingQuantity,sellordersize) #update order size
                self.icebergOrderselllist[currenttier][2] = remainingQuantity
                self.icebergOrderselllist[currenttier][3] = price
                self.icebergOrderselllist[currenttier][5] = sendorderId
                self.icebergOrderselllist[currenttier][6] = 0.0
            else:
                self.icebergOrderselllist[currenttier][0] = currenttier
                self.icebergOrderselllist[currenttier][1] = 0.0
                self.icebergOrderselllist[currenttier][3] = price
                self.icebergOrderselllist[currenttier][5] = None
                self.icebergOrderselllist[currenttier][6] = 0.0            

    def depth_quoting_strategy(self):
        """generate depth quoting strategy,initiate a quoting wall, then place in orders until the total quantity of each tier is filled. Put in new buy orders at the bottom, sell orders at the top"""
        #icebergorder = [tiernumber,ordersize,quantity,orderprice,quotingside,buyorderID,filledamount]
        
        if self.storemovingaverage == 0.0:
            self.storemovingaverage = self.currentmovingaverage
        if self.quotingside == 'BUY' and len(self.icebergOrderbuylist) == 0:        #if it's buy order and no quotes in the market yet, create initial layer quotes          
            self.icebergOrderbuylist = self.create_initial_tier(self.quotinglevels)        
        elif self.quotingside == 'BUY' and len(self.icebergOrderbuylist) > 0 :      #if it's buy order and there're already quotes in the market                

            # check for any invalid orders in the list and replace
            for i in range(len(self.icebergOrderbuylist)):
                tiernumber = i
                order = self.icebergOrderbuylist[i]
                remainingQuantity = order[2] - order[6]
                price = self.get_single_quoting_price(tiernumber)
                if order[5] is None and remainingQuantity > 0:
                    self.replace_single_tier(tiernumber, remainingQuantity, price)
                    self.logger.info(f"replace empty {order[4]} tier {tiernumber} with new order {order[5]} @ price {order[3]}, amount {remainingQuantity}")
                    time.sleep(0.2)

            movingaverage_change = self.currentmovingaverage - self.storemovingaverage
            self.update_total_quantity_for_layers()
            currenttier = -1
            tempremainingBuyQuantity = -1
            while currenttier < self.quotinglevels - 1 and tempremainingBuyQuantity <= 0:
                currenttier += 1
                tempremainingBuyQuantity = self.icebergOrderbuylist[currenttier][2] - self.icebergOrderbuylist[currenttier][6]
                remainingBuyQuantity = max(0,tempremainingBuyQuantity)
                
            # all levels are now 0, exit
            if (currenttier == self.quotinglevels - 1) and tempremainingBuyQuantity <= 0:
                self.update_total_quantity_for_layers()
                return -1

            if movingaverage_change >= -self.threshold: # if MA moves within range or moves up, consider our top bid
                if self.icebergOrderbuylist[currenttier][6] < self.icebergOrderbuylist[currenttier][1]: # if order partially filled, wait/do nothing
                    # scenario 1: there is still amount remaining unfilled in the order
                    pass
                else:
                    # scenario 2: all the amount has been filled
                    self.icebergOrderbuylist[currenttier][2] = remainingBuyQuantity
                    if remainingBuyQuantity > 0:   #if the remaining quantity is positive, then we put in a new order with size min of remaining quantity and random ordersize
                        self.replace_single_tier(currenttier, remainingBuyQuantity, self.icebergOrderbuylist[currenttier][3])
                        self.logger.debug(f"depth_quoting_strategy {self.icebergOrderbuylist[currenttier][5]}")
                    for i in range(len(self.icebergOrderbuylist)):
                        self.logger.debug(f"buy list {i} = {self.icebergOrderbuylist[i]}")
                        self.logger.info("Level: %s,The order size is %s,the remaining quantity is %s, the filled amount is %s, the orderid is %s" 
                        % (i,self.icebergOrderbuylist[i][1], self.icebergOrderbuylist[i][2], self.icebergOrderbuylist[i][6],self.icebergOrderbuylist[i][5]))  

                ## if change is big enough, cancel bottom layer and repost to top
                if movingaverage_change > self.threshold: #MA moves up out of range
                    newquotingprice = self.get_single_quoting_price(0)
                    if newquotingprice >= self.icebergOrderbuylist[currenttier][3] + self.tierdepth: #if the new price is higher than our best bid + tier depth,cancel bottom layer,insert new order at the top
                        buyordersize = self.generate_buyorder_size(currenttier)
                        tiernumber = self.quotinglevels-1
                        ## Cancel bottom layer
                        remainingBuyQuantity_lasttier = self.icebergOrderbuylist[tiernumber][2] - self.icebergOrderbuylist[tiernumber][6]
                        
                        if self.icebergOrderbuylist[tiernumber][6] < self.icebergOrderbuylist[tiernumber][1]: # if order partially filled
                            if self.icebergOrderbuylist[tiernumber][5]: ## if orderid is not none
                                self.brokers[0].client.cancel_wait(self.icebergOrderbuylist[tiernumber][5], no_wait=True)
                            ## to-do: check if cancel is successful, then release the leftover quantity
                        ## repost to top
                        if remainingBuyQuantity_lasttier > 0:
                            newbuytier = self.create_single_tier(0,min(remainingBuyQuantity_lasttier,buyordersize),remainingBuyQuantity_lasttier) #put in new order at the top with min of order size and remaining size
                            del self.icebergOrderbuylist[tiernumber]
                            self.icebergOrderbuylist.insert(0,newbuytier)
                        self.storemovingaverage = self.currentmovingaverage

                    for i in range(len(self.icebergOrderbuylist)):
                        self.logger.info("Level: %s,The order size is %s,the remaining quantity is %s, the filled amount is %s, the orderid is %s" 
                        % (i,self.icebergOrderbuylist[i][1], self.icebergOrderbuylist[i][2], self.icebergOrderbuylist[i][6],self.icebergOrderbuylist[i][5]))  

            else:   #otherwise when the MA move down out of range, then we create a new tier at the bottom          
                newquotingprice = self.get_single_quoting_price(self.quotinglevels-1)  #create new quoting price using updated bid
                if newquotingprice <= self.icebergOrderbuylist[-1][3] - self.tierdepth:   #if the new price is lower than the lowest bid - tierdepth
                    buyordersize = self.generate_buyorder_size(self.quotinglevels-1)
                    ## Cancel top layer
                    currentbuyorderid = self.icebergOrderbuylist[currenttier][5]
                    if  self.icebergOrderbuylist[currenttier][6] < self.icebergOrderbuylist[currenttier][1]:  #if didn't fill completely,canel order, put in order with original size +- random at the bottom
                        if currentbuyorderid:
                            self.brokers[0].client.cancel_wait(currentbuyorderid, no_wait=True)
                        ## to-do: check if cancel is successful, then release the left over quantity
                    ## repost at the bottom
                    if remainingBuyQuantity > 0:
                        newbuytier = self.create_single_tier(self.quotinglevels-1,min(remainingBuyQuantity,buyordersize),remainingBuyQuantity) #put in new order at the bottom with min of order size and remaining size
                        del self.icebergOrderbuylist[currenttier]
                        self.icebergOrderbuylist.append(newbuytier)
                    self.storemovingaverage = self.currentmovingaverage

                for i in range(len(self.icebergOrderbuylist)):
                        self.logger.info("Level: %s,The order size is %s,the remaining quantity is %s, the filled amount is %s, the orderid is %s" 
                        % (i,self.icebergOrderbuylist[i][1], self.icebergOrderbuylist[i][2], self.icebergOrderbuylist[i][6],self.icebergOrderbuylist[i][5]))  

        elif self.quotingside == 'SELL' and len(self.icebergOrderselllist) == 0:        #if it's sell order and no quotes in the market yet, create initial layer quotes          
            self.icebergOrderselllist = self.create_initial_tier(self.quotinglevels)
        elif self.quotingside == 'SELL' and len(self.icebergOrderselllist) > 0 :      #if it's sell order and there're already quotes in the market 

            # check for any invalid orders in the list and replace
            for i in range(len(self.icebergOrderselllist)):
                tiernumber = i
                order = self.icebergOrderselllist[i]
                remainingQuantity = order[2] - order[6]
                price = self.get_single_quoting_price(tiernumber)
                if order[5] is None and remainingQuantity > 0:
                    self.replace_single_tier(tiernumber, remainingQuantity, price)
                    self.logger.info(f"replace empty {order[4]} tier {tiernumber} with new order {order[5]} @ price {order[3]}, amount {remainingQuantity}")
                    time.sleep(0.2)

            movingaverage_change = self.currentmovingaverage - self.storemovingaverage
            self.update_total_quantity_for_layers()
            currenttier = -1
            tempremainingSellQuantity = -1 #self.icebergOrderselllist[currenttier][2] - self.icebergOrderselllist[currenttier][6]
            while currenttier < self.quotinglevels - 1 and tempremainingSellQuantity <= 0:
                currenttier += 1
                tempremainingSellQuantity = self.icebergOrderselllist[currenttier][2] - self.icebergOrderselllist[currenttier][6]
                remainingSellQuantity = max(0,tempremainingSellQuantity)

            # all levels are now 0, exit
            if (currenttier == self.quotinglevels - 1) and tempremainingSellQuantity <= 0:
                self.update_total_quantity_for_layers()
                return -1

            if movingaverage_change <= self.threshold: # if MA moves within range or moves down, consider our top ask
                if self.icebergOrderselllist[currenttier][6] < self.icebergOrderselllist[currenttier][1]: # if order partially filled, wait/do nothing
                    pass
                else:    
                    self.icebergOrderselllist[currenttier][2] = remainingSellQuantity                                         #else order fully filled and
                    if remainingSellQuantity > 0:   #if the remaining quantity is positive, then we put in a new order with size min of remaining quantity and random ordersize
                        self.replace_single_tier(currenttier, remainingSellQuantity, self.icebergOrderselllist[currenttier][3])
                        self.logger.debug(f"depth_quoting_strategy {self.icebergOrderselllist[currenttier][5]}")
                    for i in range(len(self.icebergOrderselllist)):
                        self.logger.info("Level: %s,The order size is %s,the remaining quantity is %s, the filled amount is %s, the orderid is %s" 
                        % (i,self.icebergOrderselllist[i][1], self.icebergOrderselllist[i][2], self.icebergOrderselllist[i][6],self.icebergOrderselllist[i][5]))  

                ## if change is big enough, cancel top layer and repost to bottom
                if movingaverage_change < -self.threshold:
                    newquotingprice = self.get_single_quoting_price(0)
                    if newquotingprice <= self.icebergOrderselllist[currenttier][3] - self.tierdepth: #if the new price is lower than our best ask - tier depth
                        sellordersize = self.generate_sellorder_size(currenttier)
                        tiernumber = self.quotinglevels-1
                        ## Cancel top layer
                        currentsellorderid = self.icebergOrderselllist[tiernumber][5]
                        remainingSellQuantity_lasttier = self.icebergOrderselllist[tiernumber][2] - self.icebergOrderselllist[tiernumber][6]
                        if self.icebergOrderselllist[tiernumber][6] < self.icebergOrderselllist[tiernumber][1]: 
                            if currentsellorderid:
                                self.brokers[0].client.cancel_wait(currentsellorderid, no_wait=True)
                        ## repost to top
                        if remainingSellQuantity_lasttier > 0:
                            newselltier = self.create_single_tier(0,min(remainingSellQuantity_lasttier,sellordersize),remainingSellQuantity_lasttier) #put in new order at the bottom with min of order size and remaining size
                            del self.icebergOrderselllist[tiernumber]
                            self.icebergOrderselllist.insert(0,newselltier)
                        self.storemovingaverage = self.currentmovingaverage 

                    for i in range(len(self.icebergOrderselllist)):
                        self.logger.info("Level: %s,The order size is %s,the remaining quantity is %s, the filled amount is %s, the orderid is %s" 
                        % (i,self.icebergOrderselllist[i][1], self.icebergOrderselllist[i][2], self.icebergOrderselllist[i][6],self.icebergOrderselllist[i][5]))

            else:   #otherwise when the MA move up out of range, then we create a new tier at the top          
                newquotingprice = self.get_single_quoting_price(self.quotinglevels-1)  #create new quoting price using updated ask
                if newquotingprice >= self.icebergOrderselllist[-1][3] + self.tierdepth:   #if the new price is higher than the highest ask + tier depth
                    sellordersize = self.generate_sellorder_size(self.quotinglevels-1)
                    ## Cancel bottom layer
                    currentsellorderid = self.icebergOrderselllist[currenttier][5]
                    if self.icebergOrderselllist[currenttier][6] < self.icebergOrderselllist[currenttier][1]:  #if didn't fill completely,canel order, put in order with original size +- random at the bottom
                        if currentsellorderid:
                            self.brokers[0].client.cancel_wait(currentsellorderid, no_wait=True)
                    ## repost at the top
                    if remainingSellQuantity > 0:
                        newselltier = self.create_single_tier(self.quotinglevels-1,min(remainingSellQuantity,sellordersize),remainingSellQuantity) #put in new order at the bottom with min of order size and remaining size
                        del self.icebergOrderselllist[currenttier]
                        self.icebergOrderselllist.append(newselltier)
                    self.storemovingaverage = self.currentmovingaverage

                for i in range(len(self.icebergOrderselllist)):
                        self.logger.info("Level: %s,The order size is %s,the remaining quantity is %s, the filled amount is %s, the orderid is %s" 
                        % (i,self.icebergOrderselllist[i][1], self.icebergOrderselllist[i][2], self.icebergOrderselllist[i][6],self.icebergOrderselllist[i][5]))
                        
               

    def exit_processing(self):
        """make sure to exit the bot safely, no opening orders left"""
        for buyorderid in self.brokers[0].buyorderlist:
            self.brokers[0].client.cancel_wait(buyorderid, no_wait=True)
            time.sleep(0.05)
        
        for sellorderid in self.brokers[0].sellorderlist:
            self.brokers[0].client.cancel_wait(sellorderid, no_wait=True)
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

    def update_movingaverage(self):
        self.currentmovingaverage = self.generate_moving_average_price(self.midpxsample)
        try:
            self.logger.info(f"the current moving average value is {self.currentmovingaverage}")
        except:
            pass
    
    def update_depthbook(self):
        self.currentdepthbid = self.brokers[0].get_depth_bid_and_size(self.tradingpair, self.quotinglevels)
        self.currentdepthask = self.brokers[0].get_depth_ask_and_size(self.tradingpair, self.quotinglevels)
        # TODO: discuss with quant team on how to properly handle such situations
        if self.currentdepthbid is None:
            self.currentdepthbid = [{ 'price': 0.0, 'volume': 0.0 }]
        if self.currentdepthask is None:
            self.currentdepthask = [{ 'price': 0.0, 'volume': 0.0 }]
        try:
            self.logger.info(f"current bid0 is {self.currentdepthbid[0]['price']}, current ask0 is {self.currentdepthask[0]['price']}")
        except:
            pass

    def tick(self):
        """Update orderbook in every tick and update account balance in every 5 minutes"""
        if time.time() - self.last_run_time < self.max_upd_frequency:
            time.sleep(self.sleep_time)
            return False
        else:
            self.last_run_time = time.time()
        # setup condition to update account balance in every 300s
        do_update_accout = False
        if int(time.time() - self.account_ts) > config.ACCOUNT_TS: # 300s
            self.account_ts = time.time()
            do_update_accout = True
        # setup condition to update mid price sample in every 5s
        do_update_midpxsample = False
        if time.time() - self.sample_ts > self.sampleinterval:
            self.sample_ts = time.time()
            do_update_midpxsample = True
        
        if isinstance(self.brokers, list) and len(self.brokers) == 1:
            # update account balance
            if do_update_accout:
                self.brokers[0].update_account_balance(self.brokers[0].get_account_id())
                if self.clerk and config.RECORD_TO_DB:
                    self.update_order_history()
            # update sample 

            quotecoinbalance = self.brokers[0].get_coin_tradable_balance(self.quotecoin)
            altcoinbalance = self.brokers[0].get_coin_tradable_balance(self.altcoin)
            askprice = self.currentdepthask[0]['price']
            #askprice = self.brokers[0].get_lowest_ask_and_volume(self.tradingpair)
            # break if there is not enough altcoin or quote coin
            if self.quotingside == 'SELL' and (altcoinbalance - self.taskquantity) < self.reservedaltcoin:
                self.logger.info(f"The {altcoinbalance - self.taskquantity} tradable balance of altcoin {self.altcoin} is less than the minimum reserved altcoin balance of {self.reservedaltcoin}.")
                time.sleep(2)
                self.brokers[0].clear_broker()
                self.brokers[0].update_orderbook(self.tradingpair)
                # update sample 
                # if do_update_midpxsample:
                #     self.update_midpx_sample()
                return False
            # quote coin balance check
            
            if self.quotingside == 'BUY' and (quotecoinbalance - self.taskquantity * askprice) < self.reservedquotecoin:   #isinstance(askprice, dict) and
                self.logger.info(f"The {quotecoinbalance - self.taskquantity * askprice} tradable balance of quotecoin {self.quotecoin} is less than the minimum reserved quotecoin balance of {self.reservedquotecoin}.")
                time.sleep(2)
                self.brokers[0].clear_broker()
                self.brokers[0].update_orderbook(self.tradingpair)
                # update sample 
                # if do_update_midpxsample:
                #     self.update_midpx_sample()
                return False
            # update orderbook
            self.brokers[0].clear_broker()
            self.brokers[0].update_orderbook(self.tradingpair)
            
            ## update sample (do update_depthbook first as this will update the self.currentdepthbid and self.currentdepthask)
            self.update_depthbook()
            if do_update_midpxsample:
                self.create_midpx_sample()
                self.update_movingaverage()

            if self.total_orders_placed >= self.total_orders_limit:
                self.message = '[ERROR: Too Many Orders Placed]'
                time.sleep(self.sleep_time)
                return False

            return True
            
        else:
            # TODO logging 
            return False
    
    # NOTE: unlike other bots, this is a pure recording function
    def record_order_to_csv(self, orderdetails):
        """record order results to a csv file"""

        if isinstance(orderdetails, dict) and len(orderdetails) > 0:
            filename = self.altcoin.upper() + '_' + self.quotecoin.upper() + '_' + self.brokers[0].get_exchangename() +'_Depth_log.csv'
            if not os.path.isfile(filename):
                # table name: orderid, timestamp(execution time), tradingpair, exchange, accountkey, type(buy/sell), amount, filled amount, price
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
                filename = self.altcoin.upper() + '_' + self.quotecoin.upper() + '_' + self.brokers[0].get_exchangename() +'_Depth_log.csv'

            with open(filename, 'r') as csvfile:
                csvreader = csv.DictReader(csvfile)
                orderdictlist = [order for order in csvreader]
        except IOError:
            self.logger.warning("The order list csv file %s doesn't exist" % filename)
            return False
        else:
            return orderdictlist

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
            orderdetails['Source'] = 'Depth'
            orderdetails['BotID'] = botid
        except Exception as e:
            self.logger.error("Failed creating ghost Order {0} due to: {1}".format(orderid, e))
            return None
        else:
            return orderdetails

    def record_order_to_db(self, broker, tradingpair, ordertype, orderid):
        orderdetails, retries = None, 0
        while orderdetails is None and retries < 5:
            orderdetails = broker.client.get_order_details(tradingpair, ordertype, orderid, data_format='legacy')
            if retries > 0:
                time.sleep(0.2)
            retries += 1
        if orderdetails:
            self.logger.debug(f'add to db {orderdetails}')
            orderdetails['Source'] = 'Depth'
            orderdetails['BotID'] = self.botid
            self.clerk.insert_execution_order_sqla(orderdetails)

            if self.clerk and config.RECORD_TO_CSV and orderdetails['Status'] == 'Completed' :
                del orderdetails['Source']
                del orderdetails['BotID'] 
                self.record_order_to_csv(orderdetails)

        else:
            ghost_orderdetails = self.create_ghost_order(broker, tradingpair, ordertype, orderid, self.botid)
            self.logger.debug(f'add to db {ghost_orderdetails} (ghost order)')
            self.clerk.insert_execution_order_sqla(ghost_orderdetails)

    def update_order_history(self, dump_to_csv = False):
        records = self.clerk.get_record_from_db(self.tradingpair, self.brokers[0].get_exchangename(), self.brokers[0].get_account_id(), source='Depth')

        for record in records:

            orderdetails, retries = None, 0
            side = 'BUY_LIMIT' if record.LongShort == 'Long' else 'SELL_LIMIT'
            while orderdetails is None and retries < 5:
                orderdetails = self.brokers[0].client.get_order_details(record.TradingPair, side, record.OrderID, data_format='legacy')
                if retries > 0:
                    time.sleep(0.2)
                retries += 1

            if orderdetails is not None and isinstance(orderdetails, dict) and orderdetails != {}:
                self.clerk.update_execution_order_sqla(orderdetails, record.ID)
                    
                if self.clerk and config.RECORD_TO_CSV and orderdetails['Status'] == 'Completed' or dump_to_csv:
                    self.record_order_to_csv(orderdetails)

    def get_latest_orders(self, limit=10, filename=None):
        results = []
        if self.clerk:
            dataset = self.clerk.get_record_from_db(self.tradingpair, self.brokers[0].get_exchangename(), self.brokers[0].get_account_id(), num=10, status=['Completed','Not Completed','Canceled'], source='Depth', botid=self.botid)
        for data in dataset:
            results.append(data.__dict__)
        return results

    def increase_order_count(self, count=1):
        try:
            self.total_orders_placed += int(count)
        except:
            pass

    # def get_latest_orders(self, num=10, filename=None):
    #     """retrieve order results from a saved csv file"""
    #     orderdictlist = []

    #     filename = self.altcoin.upper() + '_' + self.quotecoin.upper() + '_' + self.brokers[0].get_exchangename() +'_Execution_log.csv'
    #     if os.path.isfile(filename):
    #         orderdictlist = self.read_order_from_csv(filename)

    #     if isinstance(orderdictlist, list) and len(orderdictlist) > 0:
    #         inum = min(len(orderdictlist), num)
    #         return orderdictlist[-inum:]
    #     else:
    #         # TODO loggering
    #         return False

    def Go(self):
        """ """
        # first build up a sample with 120 slices
        self.build_midpx_sample()
        # start
        endtime = time.time() + 900
        
        while 1 and time.time() < endtime:
            if self.tick():
                self.depth_quoting_strategy()
        
        self.exit_processing()
        

    # def update_sample_size(self, num=120):
    #     """update the size of sample required to calculate average price."""
    #     try:
    #         inum = int(num)
    #         if inum > 0:
    #             self.samplesize = inum
    #             self.sampleruntime = inum * self.sampleinterval
    #         else:
    #             self.passanitycheck = False
    #             self.logger.error("The sample size {size} is not a positive number, updating failed!".format(size=num))
    #             return False
    #     except ValueError:
    #         self.passanitycheck = False
    #         self.logger.error("The sample size {size} is not integer, updating failed!\n".format(size=num))
    #         return False

    
     # def build_midpx_sample(self):     
    #     """build up the sample set of mid price"""
    #     midprice = 0.0
    #     # bidaskspread = 0.0
    #     # self.midpxsamplelist = []
    #     while len(self.midpxsample) < self.samplesize:

    #         beginningtime = time.time()

    #         if isinstance(self.brokers, list) and len(self.brokers) == 1:
    #             # update orderbook
    #             self.brokers[0].clear_broker()
    #             self.brokers[0].update_orderbook(self.tradingpair)
    #         else:
    #             raise Exception

    #         depthbid = self.brokers[0].get_depth_bid_and_size(self.tradingpair, self.quotinglevels)
    #         depthask = self.brokers[0].get_depth_ask_and_size(self.tradingpair, self.quotinglevels)
            
    #         if isinstance(depthbid, list) and isinstance(depthask, list):
    #             if (depthask[0]['volume']/depthbid[0]['volume']) > 1:
    #                 for i in range(self.quotinglevels):
    #                     if (depthask[0]['volume']/depthbid[i]['volume']) < 100:
    #                         if depthask[0]['price'] > depthbid[i]['price'] > 0:
    #                             # bidaskspread = round(float(depthask[0]['price'] - depthbid[i]['price']), self.orderpricerounding)
    #                             midprice = round(float((depthask[0]['price'] + depthbid[i]['price'])/2), self.orderpricerounding)
    #                             self.midpxsample.append(midprice)
    #                             break
    #             elif (depthbid[0]['volume']/depthask[0]['volume']) > 1:
    #                 for i in range(self.quotinglevels):
    #                     if (depthbid[0]['volume']/depthask[i]['volume']) < 100:
    #                         if depthask[i]['price'] > depthbid[0]['price'] > 0:
    #                             # bidaskspread = round(float(depthask[i]['price'] - depthbid[0]['price']), self.orderpricerounding)
    #                             midprice = round(float((depthask[i]['price'] + depthbid[0]['price'])/2), self.orderpricerounding)
    #                             self.midpxsample.append(midprice)
    #                             break
    #             else:
    #                 pass
    #         else:
    #             continue

    #         timeinterval = time.time() - beginningtime
    #         if self.sampleinterval > timeinterval:
    #             time.sleep(self.sampleinterval-timeinterval)
        
    #     return self.midpxsample
    

    # def update_midpx_sample(self):
    #     """add a new slice to the sample, delete an old one in every 5s"""
    #     midprice = 0.0
    #     # bidaskspread = 0.0

    #     depthbid = self.brokers[0].get_depth_bid_and_size(self.tradingpair, self.quotinglevels)
    #     depthask = self.brokers[0].get_depth_ask_and_size(self.tradingpair, self.quotinglevels)
    #     # no update if there is no meaningful orderbook data    
    #     if isinstance(depthbid, list) and isinstance(depthask, list):
    #         if (depthask[0]['volume']/depthbid[0]['volume']) > 1:
    #             for i in range(self.quotinglevels):
    #                 if (depthask[0]['volume']/depthbid[i]['volume']) < 100:
    #                     if depthask[0]['price'] > depthbid[i]['price'] > 0:
    #                         # bidaskspread = round(float(depthask[0]['price'] - depthbid[i]['price']), self.orderpricerounding)
    #                         midprice = round(float((depthask[0]['price'] + depthbid[i]['price'])/2), self.orderpricerounding)
    #                         self.midpxsample.append(midprice)
    #                         del self.midpxsample[0]
    #                         break
    #         elif (depthbid[0]['volume']/depthask[0]['volume']) > 1:
    #             for i in range(self.quotinglevels):
    #                 if (depthbid[0]['volume']/depthask[i]['volume']) < 100:
    #                     if depthask[i]['price'] > depthbid[0]['price'] > 0:
    #                         # bidaskspread = round(float(depthask[i]['price'] - depthbid[0]['price']), self.orderpricerounding)
    #                         midprice = round(float((depthask[i]['price'] + depthbid[0]['price'])/2), self.orderpricerounding)
    #                         self.midpxsample.append(midprice)
    #                         del self.midpxsample[0]
    #                         break
    #         else:
    #             pass

    #     return self.midpxsample      
        

        # def generate_quoting_buy_price(self):
    #     """ """
    #     bidpricelist = []
    #     bidprice = 0.0
    #     bidaskspread = 0.0
    #     # self.midpxsample is a list, a mutable object in python
    #     self.movingaverage = self.generate_moving_average_price(self.midpxsample)
    #     bestoffer = self.brokers[0].get_lowest_ask_and_volume(self.tradingpair)
    #     bestbid = self.brokers[0].get_highest_bid_and_volume(self.tradingpair)
        
    #     if isinstance(bestoffer, dict) and isinstance(bestbid, dict) and bestoffer['price'] > bestbid['price'] > 0 \
    #     and self.movingaverage:
    #         #bidaskspread = round(float(bestoffer['price'] - bestbid['price']), self.orderpricerounding)
    #         bidprice = round(float(self.movingaverage - self.quotingedge), self.orderpricerounding)
    #     else:
    #         # TODO logging
    #         return False
        
    #     if self.minorderprice < bidprice < bestoffer['price']:
    #         # bidpricelist.append(bidprice)
    #         for i in range(self.quotinglevels):
    #             bidpricelist.append(bidprice - i * self.tierdepth)
            
    #         return bidpricelist
    #     else:
    #         # TODO logging
    #         return False

    

    # def generate_quoting_buy_size(self):
    #     """ """
    #     bidsizelist = []
    #     for i in range(self.quotinglevels):
    #         bidsizelist.append(self.taskordersize)
        
    #     return bidsizelist
    

    # def generate_quoting_sell_price(self):
    #     """ """
    #     offerpricelist = []
    #     offerprice = 0.0
    #     bidaskspread = 0.0
    #     # self.midpxsample is a list, a mutable object in python
    #     self.movingaverage = self.generate_moving_average_price(self.midpxsample)
    #     bestoffer = self.brokers[0].get_lowest_ask_and_volume(self.tradingpair)
    #     bestbid = self.brokers[0].get_highest_bid_and_volume(self.tradingpair)
        
    #     if isinstance(bestoffer, dict) and isinstance(bestbid, dict) and bestoffer['price'] > bestbid['price'] > 0 \
    #     and self.movingaverage:
    #         #bidaskspread = round(float(bestoffer['price'] - bestbid['price']), self.orderpricerounding)
    #         offerprice = round(float(self.movingaverage + self.quotingedge), self.orderpricerounding)
    #     else:
    #         # TODO logging
    #         return False
        
    #     if bestbid['price'] < offerprice < self.maxorderprice:
    #         for i in range(self.quotinglevels):
    #             offerpricelist.append(offerprice - i * self.tierdepth)
            
    #         return offerpricelist
    #     else:
    #         # TODO logging
    #         return False
    

    # def generate_quoting_sell_size(self):
    #     """ """
    #     offersizelist = []
    #     for i in range(self.quotinglevels):
    #         offersizelist.append(self.taskordersize)
        
    #     return offersizelist


        # def depth_quoting_strategy(self):
    #     """ """
    #     # which side(s) of the orderbook Algo quotes
    #     # 0: bid, 1: ask, 2: both bid & ask
    #     if self.quotingside == 0:
    #         if len(self.brokers[0].buyorderlist) > 0:
    #             for buyorderid in self.brokers[0].buyorderlist:
    #                 buyorderdetails = self.brokers[0].client.get_order_details(self.tradingpair, 'BUY_LIMIT', buyorderid, data_format='legacy')
    #                 if isinstance(buyorderdetails, dict) and len(buyorderdetails) > 0:
    #                     if buyorderdetails['FilledAmount'] > 0:
    #                         # TODO what if cancel failed?
    #                         self.brokers[0].client.cancel_wait(buyorderid)
    #                         self.brokers[0].buyorderlist.remove(buyorderid)
    #                         # TODO add another new order at the bottom of the series
    #                     else:
    #                         # get out of the for loop at the first order with 0 FilledAmount
    #                         break
    #         else:
    #             bidpricelist = self.generate_quoting_buy_price()
    #             bidsizelist = self.generate_quoting_buy_size()
    #             # make sure price vs size are paired from high to low
    #             if isinstance(bidpricelist, list) and isinstance(bidsizelist, list) and \
    #             (len(bidpricelist) == len(bidsizelist)):
    #                 for bidprice, bidsize in zip(bidpricelist, bidsizelist):
    #                     self.generate_order(bidprice, bidsize, self.brokers[0], None)
    #     # 0: bid, 1: ask, 2: both bid & ask
    #     elif self.quotingside == 1:
    #         if len(self.brokers[0].sellorderlist) > 0:
    #             for sellorderid in self.brokers[0].sellorderlist:
    #                 sellorderdetails = self.brokers[0].client.get_order_details(self.tradingpair, 'SELL_LIMIT', sellorderid, data_format='legacy')
    #                 if isinstance(sellorderdetails, dict) and len(sellorderdetails) > 0:
    #                     if sellorderdetails['FilledAmount'] > 0:
    #                         # TODO what if cancel failed?
    #                         self.brokers[0].client.cancel_wait(sellorderid)
    #                         self.brokers[0].sellorderlist.remove(sellorderid)
    #                         # TODO add another new order at the bottom of the series
    #                     else:
    #                         # get out of the for loop at the first order with 0 FilledAmount
    #                         break
    #         else:
    #             offerpricelist = self.generate_quoting_sell_price()
    #             offersizelist = self.generate_quoting_sell_size()
    #             # make sure price vs size are paired from low to high
    #             if isinstance(offerpricelist, list) and isinstance(offersizelist, list) and \
    #             len(offerpricelist) == len(offersizelist):
    #                 for offerprice, offersize in zip(offerpricelist, offersizelist):
    #                     self.generate_order(offerprice, offersize, None, self.brokers[0])
    #     # 0: bid, 1: ask, 2: both bid & ask
    #     elif self.quotingside == 2:
    #         # TODO finish the two side quoting
    #         if len(self.brokers[0].buyorderlist) > 0 and len(self.brokers[0].sellorderlist) > 0:
    #             pass
    #         else:
    #             pass
    #     else:
    #         return False

    # def create_midpx_sample(self):
    #     """take sample every 5s,delete the first one when the sample length reached MA rolling window"""
    #     midprice = 0.0
    #     #samplesize = self.movingaveragewindow * 60 / self.sampleinterval
    #     depthbid = self.brokers[0].get_depth_bid_and_size(self.tradingpair, self.quotinglevels)
    #     depthask = self.brokers[0].get_depth_ask_and_size(self.tradingpair, self.quotinglevels)

    #     while len(self.midpxsample) < self.samplesize:

    #         beginningtime = time.time()

    #         if isinstance(self.brokers, list) and len(self.brokers) == 1:
    #             # update orderbook
    #             self.brokers[0].clear_broker()
    #             self.brokers[0].update_orderbook(self.tradingpair)
                
    #         if isinstance(depthbid, list) and isinstance(depthask, list):
    #             midprice = round(float((depthask[0]['price'] + depthbid[0]['price'])/2), self.orderpricerounding)
    #             self.midpxsample.append(midprice)
    #         else:
    #             continue

    #         timeinterval = time.time() - beginningtime
    #         if self.sampleinterval > timeinterval:
    #             time.sleep(self.sampleinterval-timeinterval)

    #         return self.midpxsample
    #     else:
    #         self.midpxsample.append(midprice)
    #         del self.midpxsample[0]
    #         return self.midpxsample