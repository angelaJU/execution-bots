#########################################################################################
# Date: 2018.05
# Auther: happyshiller 
# Email: happyshiller@gmail.com
# Github: https://github.com/happyshiller/Altonomy
# ServiceBot is the base class for Volume, Execution and Depth Algo
#########################################################################################

from altonomy.core import logger as _logger
from . import config



class ServiceBot():
    """ """

    def __init__(self, brokers, clerk, altcoin, quotecoin, logger=None):
        """ """
        # list of broker objective
        # ExecutionBot and DepthBot only need one broker
        self.brokers = brokers
        # a clerk objective
        self.clerk = clerk
        # only one altcoin
        self.altcoin = altcoin
        # only one quotecoin 
        self.quotecoin = quotecoin
        # for instance: ELFBTC, ITCBTC
        self.tradingpair = None
        # minimum waiting time in seconds
        self.minwaitingtime = 15
        # maximum waiting time in seconds
        self.maxwaitingtime = 45
        # random waiting time based on min/max waiting time
        # (random.randint(self.minwaitingtime*1000, self.maxwaitingtime*1000))/1000 
        self.orderwaitingtime = 0.0
        # total quantity of altcoin
        self.taskquantity = 0.0
        # average single order size based on task quantity
        # equal to taskquantity / orderwaitingtime
        self.taskordersize = 0.0
        # scale the order size with +/-randomvalue(20%)
        self.isordersizerandom = False
        # random.uniform(0.8, 1.2)
        self.orderandomrange = 0.2
        # range of order size
        self.minordersize = 0.0
        self.maxordersize = 0.0
        # order price
        self.taskorderprice = 0.0
        # range of order price
        self.minorderprice = 0.0
        self.maxorderprice = 0.0
        # minimum tick size
        self.minimumticksize = 0.0
        # precision details of an order: price, size, value
        # self.orderprecision = {}
        self.orderpricerounding = 0
        self.ordersizerounding = 0
        self.ordervaluerounding = 0
        # bid ask spread
        # TODO clarify 
        self.bidaskspread = 0.0
        # the depth level in orderbook
        self.depthlevel = 5
        # the quantity of alt coin shall leave untouched
        self.reservedaltcoin = 0
        # the quantity of quote coin shall leave untouched
        self.reservedquotecoin = 0.0
        # minimum sizes
        self.minimum_order_size = 0
        self.minimum_order_notional = 0
        # good to go signal
        self.passanitycheck = True
        # account updating time stamp
        self.account_ts = 0
        # order generating time stamp
        self.order_ts = 0
        # recording time stamp
        self.recording_ts = 0
        # logger
        self.logger = logger if logger else _logger(__name__)
        # initiate the trading pair, and precision details
        self.init_tradingpair()
        # update orderbook once at initialization
        self.init_orderbook()
        # bot id
        self.botid = None
        self.cancel_list = []

    def init_tradingpair(self):
        """generate tradingpair based on alt & quote coin, and its minimum tick and other precision details"""
        # empty string evaluate to False in Python
        if self.altcoin and self.quotecoin:
            self.tradingpair = self.altcoin + self.quotecoin
            # there could be two or three precision info
            precision = self.brokers[0].get_pair_precision(self.tradingpair)
            if isinstance(precision, dict) and len(precision) > 0:
                try:
                    self.orderpricerounding = int(precision['priceprecision'])
                    self.ordersizerounding = int(precision['amountprecision'])
                except Exception as e:
                    self.logger.error("Failed to get precision info due to: {err}!".format(err=e))
                    raise e
                self.ordervaluerounding = int(precision.get('valueprecision', 0))
            else:
                self.logger.error("There is NO valid precision info!")
                return False
            # calculate the minimum tick
            if self.orderpricerounding:
                self.minimumticksize = round(float(10 ** -self.orderpricerounding), self.orderpricerounding)
        else:
            self.logger.error("Neither altcoin {alt} or quotecoin {quote} should be empty!".format(alt=self.altcoin, quote=self.quotecoin))
            return False
    

    def init_orderbook(self):
        """update the orderbook of given trading pair during class initialization"""
        if self.brokers[0]:
            self.brokers[0].update_orderbook(self.tradingpair)
        else:
            self.logger.error("The broker list is empty, updating orderbook during initialization failed!\n")
            return False
    

    def update_min_time_break(self, timebreak):
        """update the minimum waiting time between each order"""
        try:
            itimebreak = int(float(timebreak))
            if itimebreak > 0:
                self.minwaitingtime = itimebreak
            else:
                self.logger.error("The minimum waiting time {tm} is not positive number, updating failed!\n".format(tm=timebreak))
                self.passanitycheck = False
                return False
        except ValueError:
            self.logger.error("The minimum waiting time {tm} is not integer, updating failed!\n".format(tm=timebreak))
            self.passanitycheck = False
            return False
    

    def update_max_time_break(self, timebreak):
        """update the maximum waiting time between each order"""
        try:
            itimebreak = int(float(timebreak))
            if itimebreak > 0:
                self.maxwaitingtime = itimebreak
            else:
                self.logger.error("The maximum waiting time {tm} is not positive number, updating failed!\n".format(tm=timebreak))
                self.passanitycheck = False
                return False
        except ValueError:
            self.logger.error("The maximum waiting time {tm} is not integer, updating failed!\n".format(tm=timebreak))
            self.passanitycheck = False
            return False
    

    def update_altcoin_daily_task_quantity(self, quantity):
        """
        :quantity: the total target quantity
        :update the task quantity 
        """
        try:
            fquantity = float(quantity)
            if fquantity > 0:
                self.taskquantity = fquantity
                # calculate order size for each order pair based on total task quantity
                self.calculate_task_order_size()
            else:
                self.logger.error("The task quantity {qty} is not positive number, updating failed!\n".format(qty=quantity))
                self.passanitycheck = False
                return False
        except ValueError:
            self.logger.error("The task quantity {qty} is not float, updating failed!\n".format(qty=quantity))
            self.passanitycheck = False
            return False
    

    def calculate_task_order_size(self):
        """calculate the order size based on daily task quantity and waiting time"""
        if self.taskquantity > 0 and self.maxwaitingtime >= self.minwaitingtime > 0:
            averagewaiting = (self.maxwaitingtime + self.minwaitingtime)/2
            # self.taskordersize = self.taskquantity / int(24*60*60/averagewaiting)
            self.taskordersize = self.taskquantity / (24*60*60/averagewaiting)
        else:
            self.passanitycheck = False
            self.logger.error("Total task quantity %s or max waiting time %s or min waiting time %s \
            should be positive.\n" % (str(self.taskquantity), str(self.maxwaitingtime), str(self.minwaitingtime)))
            return False


    def update_min_order_price(self, price):
        """
        :min_order_price: the minimum order price
        :update the minimum order price limit
        """
        try:
            fprice = float(price)
            if fprice > 0:
                self.minorderprice = fprice
            else:
                self.passanitycheck = False
                self.logger.error("The task min order price %s is not positive number, updating failed!\n" % str(price))
                return False
        except ValueError:
            self.passanitycheck = False
            self.logger.error("The task min order price %s is not float, updating failed!\n" % str(price))
            return False


    def update_max_order_price(self, price):
        """
        :max_order_price: the maximum order price
        :update the maximum order price limit
        """
        try:
            fprice = float(price)
            if fprice > 0:
                self.maxorderprice = fprice
            else:
                self.passanitycheck = False
                self.logger.error("The task max order price %s is not positive number, updating failed!\n" % str(price))
                return False
        except ValueError:
            self.passanitycheck = False
            self.logger.error("The task max order price %s is not float, updating failed!\n" % str(price))
            return False


    def update_reserved_altcoin(self, amount):
        """update the reserved amount of altcoin"""
        try:
            famount = float(amount)
            if famount >= 0.0:
                self.reservedaltcoin = famount
            else:
                self.reservedaltcoin = 0.0
                self.logger.error("The reserved altcoin {amt} is not a positive number. Take default value 0.0!\n".format(amt=amount))
        except ValueError:
            self.reservedaltcoin = 0.0
            self.logger.error("The reserved altcoin {amt} is not float, updating failed! Take default value 0.0!\n".format(amt=amount))
    

    def update_reserved_quotecoin(self, amount):
        """update the reserved amount of quote coin"""
        try:
            famount = float(amount)
            if famount >= 0.0:
                self.reservedquotecoin = famount
            else:
                self.reservedquotecoin = 0.0
                self.logger.error("The reserved quote coin {amt} is not a positive number. Take default value 0.0!\n".format(amt=amount))
        except ValueError:
            self.reservedquotecoin = 0.0
            self.logger.error("The reserved quote coin {amt} is not float, updating failed! Take default value 0.0!\n".format(amt=amount))
    

    def update_altcoin_order_random_range(self, israndom=True, range=0.2):
        """
        :israndom: 'TRUE' or 'FALSE'
        :range: default value 20%
        :update the random range on based order size 
        """
        if isinstance(israndom, bool):
            self.isordersizerandom = israndom
        else:
            self.passanitycheck = False
            self.logger.error("The israndom variable %s is not boolean, updating failed!\n" % str(israndom))
            return False

        try:
            frange = float(range)
            if frange > 0:
                self.orderandomrange = frange
            else:
                self.passanitycheck = False
                self.logger.error("The order size random range %s is not positive number, updating failed!\n" % str(range))
                return False
        except ValueError:
            self.passanitycheck = False
            self.logger.error("The order size random range %s is not float, updating failed!\n" % str(range))
            return False

    def update_minimum_order_notional(self, amount):
        try:
            famount = float(amount)
            if famount > 0.0:
                self.minimum_order_notional = famount
            else:
                self.minimum_order_notional = 0
                self.logger.warning("The minimum order notional %s is not a positive number. Setting to default value 0\n" % str(amount))
        except ValueError:
            self.minimum_order_size = 0
            self.logger.warning("The minimum order notional %s is not a float, updating failed! Setting to default value 0\n" % str(amount))

    def update_minimum_order_size(self, amount):
        try:
            famount = float(amount)
            if famount > 0.0:
                self.minimum_order_size = famount
            else:
                self.minimum_order_size = 0
                self.logger.warning("The minimum order size %s is not a positive number. Setting to default value 0\n" % str(amount))
        except ValueError:
            self.minimum_order_size = 0
            self.logger.warning("The minimum order size %s is not a float, updating failed! Setting to default value 0\n" % str(amount))

    def sanity_check(self):
        """make sure bot config is correct"""
        return self.passanitycheck
    


    def function_samples(self, **kwargs):
        """
        :**arg: pass a variable number of arguments to a function.
        :       variable means that you do not know before hand that 
        :       how many arguments can be passed to function by user.
        :keys: samplesize, sampleinterval, sampleruntime 
        :initialize the moving average sample set
        """
        pass
    
    def get_quotecoin_tradable_balance(self):
        """get quote coin tradable balance"""
        account = {}

        if not isinstance(self.brokers, list) or not len(self.brokers)>0:
            self.logger.error("The broker list is empty, update quote coin tradable balance failed!\n")
            return False

        for broker in self.brokers:
            account[broker.get_account_id()] = broker.get_coin_tradable_balance(self.quotecoin)
        
        return account    

    def get_quotecoin_frozen_balance(self):
        """get quote coin frozen balance"""
        account = {}

        if not isinstance(self.brokers, list) or not len(self.brokers)>0:
            self.logger.error("The broker list is empty, update quote coin frozen balance failed!\n")
            return False

        for broker in self.brokers:
            account[broker.get_account_id()] = broker.get_coin_frozen_balance(self.quotecoin)
        
        return account    

    def get_altcoin_tradable_balance(self):
        """get altcoin tradable balance"""
        account = {}

        if not isinstance(self.brokers, list) or not len(self.brokers)>0:
            self.logger.error("The broker list is empty, update altcoin tradable balance failed!\n")
            return False
        
        for broker in self.brokers:
            account[broker.get_account_id()] = broker.get_coin_tradable_balance(self.altcoin)
        
        return account    

    def get_altcoin_frozen_balance(self):
        """get altcoin frozen balance"""
        account = {}

        if not isinstance(self.brokers, list) or not len(self.brokers)>0:
            self.logger.error("The broker list is empty, update altcoin frozen balance failed!\n")
            return False

        for broker in self.brokers:
            account[broker.get_account_id()] = broker.get_coin_frozen_balance(self.altcoin)
        
        return account

    def get_bid1_price(self):
        """get the price and volume of bid1"""
        bid1 = {}
        bid1 = self.brokers[0].get_highest_bid_and_volume(self.tradingpair)
        
        if isinstance(bid1, dict) and len(bid1)>0:
            return bid1['price']
        else:
            # TODO logging
            return None
    
    def get_ask1_price(self):
        """ """
        ask1 = {}
        ask1 = self.brokers[0].get_lowest_ask_and_volume(self.tradingpair)
        
        if isinstance(ask1, dict) and len(ask1):
            return ask1['price']
        else:
            # TODO logging
            return None

    def meets_minimum_size_and_notional_thresholds(self, price, volume):
        """ checks if the minimum size requirements are met """
        if volume < self.minimum_order_size:
            self.logger.warning(f"generated order size {volume} is below minimum order size {self.minimum_order_size}")
            return False
        if price * volume < self.minimum_order_notional:
            self.logger.warning(f"generated order notional {volume * price} is below minimum order notional {self.minimum_order_notional}")
            return False
        return True