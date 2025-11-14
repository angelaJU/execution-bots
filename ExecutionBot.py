#########################################################################################
# Date: 2018.05
# Auther: happyshiller 
# Email: happyshiller@gmail.com
# Github: https://github.com/happyshiller/Altonomy
# Function: automatic execution
#########################################################################################

import os
import csv
import time
import datetime
import random
import json
import math
import functools
from altonomy.core import logger
from altonomy.core import client
from . import config
import requests
import operator
from importlib import import_module
from .HelmClient import HelmClient
import ast
class ExecutionBot():
    """ExecutionBot is a generic class for automatic execution, grid trading, Spoofy......"""

    def __init__(self, broker, altcoin, quotecoin, bot_id, config, logger=None):
        """
        """
        # a broker objective
        self.broker = broker
        # only 1 altcoin
        self.altcoin = altcoin
        # only 1 quotecoin 
        self.quotecoin = quotecoin
        # Bot Id
        self.bot_id = bot_id
        # for instance: ELFBTC, ITCBTC
        self.tradingpair = None
        # 'BUY' or 'SELL'
        self.orderdirection = None
        # execution strategy: 'VANILLA', 'STRAWBERRY'(smart algo)
        self.strategy = 'VANILLA'
        # minimum waiting time in seconds
        self.minwaitingtime = 15
        # maximum waiting time in seconds
        self.maxwaitingtime = 45
        # random waiting time based on min/max waiting time
        # (random.randint(self.minwaitingtime*1000, self.maxwaitingtime*1000))/1000 
        self.orderwaitingtime = 0.0
        # total quantity of altcoin need to be executed
        self.taskquantity = 0.0
        # single order size
        self.taskordersize = 0.0
        # scale the order size with +/-randomvalue(20%)
        self.isordersizerandom = False
        # random.uniform(0.8, 1.2)
        self.orderandomrange = 0.2
        # order price
        self.taskorderprice = 0.0
        # range of order price
        self.minorderprice = 0.0
        self.maxorderprice = 0.0
        # how long the algo waits for next update of orderbook or balance 
        self.updatingtimebreak = 2.0
        # increase/decrease order price by a given number 
        # increase/decrease certain number of minimum tick
        self.increment = 0.0
        # minimum tick size
        self.minimumticksize = 0.0
        # bid ask spread
        self.bidaskspread = 0.0
        # bid ask spread benchmark, the regular width of the bid ask spread
        self.bidaskspreadmark = 0.0
        # precision details of an order: price, size, value
        # self.orderprecision = {}
        self.orderpricerounding = 8
        self.ordersizerounding = 0
        self.ordervaluerounding = 0
        # account updating time stamp
        self.account_ts = 0
        # order generating time stamp
        self.order_ts = 0
        # recording time stamp
        self.recording_ts = 0
        # the quantity of alt coin shall leave untouched
        self.reservedaltcoin = 0
        # the quantity of quote coin shall leave untouched
        self.reservedquotecoin = 0
        # the threshold of maximum executable amount of alt & quote coin
        self.execquotecointhold = 0
        self.execaltcointhold = 0
        # minimum order size 
        self.minordersize = 0
        # maximum order size 
        self.maxordersize = 0
        # good to go signal
        self.passanitycheck = True
        self.depthlevel = 0
        self.target_altcoin = -1.0
        self.target_quotecoin = -1.0
        self.check_target = False
        self.trigger_condition = ' '
        self.stop_condition = ' '
        self.order_type = 'LIMIT'
        self.remark = ''
        self.target_account_position = None
        self.leverage = 1
        self.instrument_data = None
        self.logger = logger if logger else _logger(__name__)
        self.exchange_name = self.exchange_name_of(self.broker.get_account_id())
        self.position = {}
        self.completed_orders = []
        # logger
        #
        self.ref_client = client(
            logger=self.logger
        )
        self.init_tradingpair()
        self.load_instrument_data()
        self.load_execution_configuration(config)
        self.init_orderbook()
        self.init_position()
        self.init_futures_data()
        self.start_book_listener()

    def load_execution_configuration(self, _config):
        """ load execution bot configuration data """
        if _config:
            self.update_instrument_type(str(_config.get('instrument_type', 'SPOT')).upper())
            self.update_order_direction(str(_config.get('order_direction', 'BUY')).upper())
            self.update_min_time_break(int(float(_config.get('min_time_break', 15))))
            self.update_max_time_break(int(float(_config.get('max_time_break',45))))
            self.update_altcoin_order_random_range(israndom=True, range=float(_config.get('random_range',0.2)))
            self.update_altcoin_order_size(float(_config.get('order_amount', 50)))
            self.update_execution_strategy(str(_config.get('execution_strategy', 'VANILLA')))
            self.update_order_price_increment(abs(float(_config.get('price_increment_multiplier', 0.0))))
            self.update_reserved_altcoin(float(_config.get('reserved_amount', 100)))
            self.update_reserved_quotecoin(float(_config.get('reserved_amount',10)))
            self.update_min_order_price(float(_config.get('min_price',0.0001)))
            self.update_max_order_price(float(_config.get('max_price',0.001)))
            self.update_updating_break(float(_config.get('updating_break', 2)))
            self.update_depth_level(int(float(_config.get('depth_level', 1))))
            self.update_bid_ask_spread_benchmark(float(_config.get('spread_benchmark', 0.0)))
            self.update_target_altcoin(float(_config.get('target_altcoin',-1.0)))
            self.update_target_quotecoin(float(_config.get('target_quotecoin',-1.0)))
            self.update_trigger_condition(_config.get('trigger_condition', ' '))
            self.update_order_type(_config.get('order_type', 'LIMIT'))            # ABOTS-115: fix incorrect method
            self.update_remark(_config.get('remark', ''))
            self.update_stop_condition(_config.get('stop_condition', ' '))
            self.update_target_account_position(_config.get('target_account_position', None))
            self.calculate_check_target()
            self.logger.debug(f"loaded configurations from config {_config}")

    def start_book_listener(self):
        try:
            account = self.name_of(self.broker.get_account_id())
            hc = HelmClient(endpoint=config.HELM_REPO, logger=self.logger)
            hc.start_orderbook_listener(account, self.exchange_name, self.altcoin, self.quotecoin)
            self.broker.client.register_resource_usage("book", self.broker.client.exchange_name(self.exchange_name), self.pair)
            self.logger.info(f'Register resource usage: resource_type=book, exchange=={self.exchange_name}, pair={self.pair}')
        except Exception as e:
            self.logger.error(f'Book listener startup failed - {e}')

    def get_exchange_symbol(self):
        try:
            self.logger.info(f'exchange_name={self.exchange_name}')
            exchange = getattr(
                import_module(f"altonomy.exchanges.{self.exchange_name}"),
                self.exchange_name)('', '', logger=self.logger)
            exchange_symbol = exchange.symbol_convert_from_common(self.pair)
            self.logger.info(
                f'pair={self.pair}, exchange_symbol={exchange_symbol}')
            return exchange_symbol
        except Exception as e:
            self.logger.error(f'cannot convert to exchange symbol {e}')
            return self.pair

    def load_instrument_data(self):
        exchange_id = self.exchange_id_of(account_id=self.broker.get_account_id())
        s = self.broker.client.instrument_data()
        instruments = s.get_active_instruments_for_exchange(exchange_id)
        exchange_symbol = self.get_exchange_symbol()
        for ins in instruments:
            if ins.exchange_symbol == exchange_symbol:
                self.instrument_data = ins
        if self.instrument_data is None:
            self.logger.error(
                f'Instrument_data not found for {exchange_symbol}')
            return
        self.logger.debug(f'instrument_data - {self.instrument_data}')
        try:
            self.orderpricerounding = int(self.instrument_data.price_precision)
            self.ordersizerounding = int(self.instrument_data.size_precision)
            self.minimumticksize = round(float(10 ** -self.orderpricerounding), self.orderpricerounding)
        except Exception as e:
            self.logger.error(f"failed to load instrument {self.instrument_data.exchange_symbol} 's precisions|e={e}")
            raise e  # DO NOT allow bot to continue if it cannot get correct precisions from instrument_data_v3
        self.logger.debug(f"precisions - {self.orderpricerounding} | {self.minimumticksize} | {self.ordersizerounding}")

    def init_futures_data(self):
        if not self.is_futures_of():
            return

        #leverage
        leverage = self.broker.client.get_product_leverage(self.tradingpair, account_id=self.broker.get_account_id())
        self.leverage = int(leverage) if leverage else 1

    def init_tradingpair(self):
        """generate tradingpair based on alt & quote coin, and its minimum tick and other precision details"""
        # empty string evaluate to False in Python
        if self.quotecoin: # for futures, altcoin could be empty
            self.tradingpair = self.altcoin + self.quotecoin
            # there could be two or three precision info
            precision = self.broker.get_pair_precision(self.tradingpair)
            if isinstance(precision, dict) and len(precision) > 0:
                try:
                    self.orderpricerounding = int(precision['priceprecision'])
                    self.ordersizerounding = int(precision['amountprecision'])
                except Exception as e:
                    self.logger.error("Get precision info failed, %s!\n" % str(e))
                    raise e
                self.ordervaluerounding = int(precision.get('valueprecision', 0))
            else:
                self.logger.error("There is NO valid precision info!\n")
                return False
            # calculate the minimum tick
            if self.orderpricerounding:
                self.minimumticksize = round(float(10 ** -self.orderpricerounding), self.orderpricerounding)
        else:
            self.logger.error("Neither altcoin %s or quotecoin %s should be empty!\n" % (self.altcoin, self.quotecoin))
            return False


    def init_orderbook(self):
        """update the orderbook of self.tradingpair during class initialization"""
        if self.broker and self.tradingpair:
            self.update_order_book_meaningfully()
        else:
            self.logger.error("The broker is empty, updating orderbook of %s during initialization failed!" % self.tradingpair)
            return False
    

    def init_position(self):
        """initialize execution report with posoiton data from redis"""

        r = self.broker.client.redis
        position_data = r.hgetall(f'{config.BOT_OUTPUT_REDIS_KEY}:{self.bot_id}:position')
        if position_data != {}:
            self.logger.info(f'Loading position data available in redis : {position_data}')
            self.position['TimeStamp'] = position_data['TimeStamp']
            self.position['Long'] = position_data['Long']
            self.position['Short'] = position_data['Short']
            self.position['Cost'] = position_data['Cost']
            self.position['Revenue'] = position_data['Revenue']
            self.position['WAPX'] = position_data['WAPX']
            self.position['TotalFilledAmount'] = position_data['TotalFilledAmount']
            self.position['TotalFilledCashAmount'] = position_data['TotalFilledCashAmount']
            orders = position_data.get('completed_orders')
            self.position['completed_orders'] = ast.literal_eval(orders)
            
        else:
            self.logger.info("position data not available in redis")
            self.position['TimeStamp'] = 0.0
            self.position['Long'] = 0.0
            self.position['Short'] = 0.0
            self.position['Cost'] = 0.0
            self.position['Revenue'] = 0.0
            self.position['WAPX'] = 0.0
            self.position['TotalFilledAmount'] = 0.0
            self.position['TotalFilledCashAmount'] = 0.0
            self.position['completed_orders'] = []

        self.completed_orders = self.position['completed_orders']

        
    @property
    def pair(self):
        return self.altcoin + self.quotecoin

    def update_altcoin(self, acoin):
        """
        :acoin: alt coin, such as ELF
        :update the altcoin
        """
        # empty string evaluate to False in Python
        if not acoin:
            self.logger.error("The AltCoin variable is empty, updating failed!\n")
            return False
        else:
            self.altcoin = acoin


    def update_quotecoin(self, bcoin):
        """
        :bcoin: quote coin, such as BTC
        :update the quotecoin
        """
        # empty string evaluate to False in Python
        # isinstance(s, str)
        if not bcoin:
            self.logger.error("The QuoteCoin variable is empty, updating failed!\n")
            return False
        else:
            self.quotecoin = bcoin

    def update_trigger_condition(self, trigger_condition):
        """
        :trigger_condition (str) in the form "exchange;pair;direction;value"
        :update trigger condition
        """
        if isinstance(trigger_condition, str) and trigger_condition.count(";") == 3:
            self.trigger_condition = trigger_condition
        else:
            self.logger.debug("invalid trigger condition format - needs to be 'exchange;pair;direction;value'")
            self.trigger_condition = ' '

    def update_stop_condition(self, stop_condition):
        """
        :stop_condition (str) in the form "asset;direction;value"
        :update stop_condition
        """
        if isinstance(stop_condition, str) and stop_condition.count(";") == 2:
            self.stop_condition = stop_condition
        else:
            self.logger.debug("invalid stop condition format - needs to be 'asset;direction;value'")
            self.stop_condition = ' '

    def update_order_type(self, order_type):
        """
        :order_type (str)
        :update trigger condition
        """
        if isinstance(order_type, str) and order_type in ['LIMIT', 'MARKET_FAST', 'MARKET_MEDIUM', 'MARKET_SLOW', 'MARKET', 'LIMIT_CLOSE', 'MARKET_CLOSE']:
            self.order_type = order_type
        else:
            self.logger.debug("invalid order type - needs to be one of these 'LIMIT, MARKET_FAST, MARKET_MEDIUM, MARKET_SLOW, MARKET'")
            self.order_type = 'LIMIT'

    def update_instrument_type(self, type):
        """
        :type: 'SPOT', 'FUTURES'
        :update instrument type
        """
        self.instrument_type = type
        if type != 'SPOT' and type != 'FUTURES':
            self.passanitycheck = False
            self.logger.error(f"The instrument type {type} is neither SPOT or FUTURES, updating failed!")
            return False

    def update_order_direction(self, direction):
        """
        :direction: 'BUY', 'SELL'
        :update order direction
        """
        if direction == 'BUY' or direction == 'SELL':
            self.orderdirection = direction
        else:
            self.passanitycheck = False
            self.logger.error("The order direction %s is neither SELL or BUY, updating failed!\n" % direction)
            return False
    

    def update_execution_strategy(self, strategy='VANILLA'):
        """
        :strategy: 'VANILLA'(basic), 'STRAWBERRY'(smart algo)
        :update execution strategy
        """
        if strategy == 'VANILLA' or strategy == 'STRAWBERRY':
            self.strategy = strategy
        else:
            self.passanitycheck = False
            self.logger.error("The execution strategy %s is neither 'VANILLA' or 'STRAWBERRY', updating failed!\n" % strategy)
            return False
    
    def update_target_altcoin(self, altcoin):
        """
        :altcoin: the altcoin target to reach
        :update the altcoin target to reach
        """
        try:
            faltcoin = float(altcoin)
            if faltcoin > 0.0 or faltcoin == -1.0:
                self.target_altcoin = faltcoin
        except:
            return False

    def update_target_quotecoin(self, quotecoin):
        """
        :quotecoin: the quotecoin target to reach
        :update the quotecoin target to reach
        """
        try:
            fquotecoin = float(quotecoin)
            if fquotecoin > 0.0 or fquotecoin == -1.0:
                self.target_quotecoin = fquotecoin
        except:
            return False

    def update_target_account_position(self, position):
        """
        :position: the pair position to reach
        :update the pair position to reach
        :   +x      - LONG x position
        :   0       - 0 position
        :   -x      - SHORT x position
        :   None    - disabled position reach
        """
        try:
            if position is not None:
                self.target_account_position = float(position)
        except:
            return False

    def update_min_time_break(self, timebreak):
        """
        :timebreak: the minimum seconds the Bot need to wait
        :update the minimum waiting time between each order
        """
        try:
            itimebreak = int(float(timebreak))
            if itimebreak >= 0:
                self.minwaitingtime = itimebreak
            else:
                self.passanitycheck = False
                self.logger.error("The minimum waiting time %s is not positive number, updating failed!\n" % str(timebreak))
                return False
        except ValueError:
            self.passanitycheck = False
            self.logger.error("The minimum waiting time %s is not integer, updating failed!\n" % str(timebreak))
            return False
    

    def update_max_time_break(self, timebreak):
        """
        :timebreak: the maximum seconds the Bot need to wait
        :update the maximum waiting time between each order
        """
        try:
            itimebreak = int(float(timebreak))
            if itimebreak > 0:
                self.maxwaitingtime = itimebreak
            else:
                self.passanitycheck = False
                self.logger.error("The maximum waiting time %s is not positive number, updating failed!\n" % str(timebreak))
                return False
        except ValueError:
            self.passanitycheck = False
            self.logger.error("The maximum waiting time %s is not integer, updating failed!\n" % str(timebreak))
            return False
    

    def update_reserved_altcoin(self, amount):
        """
        :amount: the amount of alt coin need to be saved
        :update the reserved amount of quote coin
        """
        try:
            famount = float(amount)
            if famount >= 0:
                self.reservedaltcoin = famount
            else:
                # ALTEX-143: negative numbers = disable check
                self.reservedaltcoin = -1
                self.logger.debug(f"disabling reserved quotecoin check")
        except ValueError:
            self.passanitycheck = False
            self.logger.error("The amount of reserved altcoin %s is not float, updating failed!\n" % str(amount))
            return False
    

    def update_reserved_quotecoin(self, amount):
        """
        :amount: the amount of quote coin need to be saved
        :update the reserved amount of altcoin
        """
        try:
            famount = float(amount)
            if famount >= 0:
                self.reservedquotecoin = famount
            else:
                # ALTEX-143: negative numbers = disable check
                self.reservedquotecoin = -1
                self.logger.debug(f"disabling reserved quotecoin check")
        except ValueError:
            self.passanitycheck = False
            self.logger.error("The amount of reserved quotecoin %s is not float, updating failed!\n" % str(amount))
            return False

    # TODO: this is not used
    def update_exec_altcoin_thold(self, amount):
        """
        :amount: the maximum executable amount of altcoin 
        :update the maximum executable amount of altcoin
        """
        try:
            famount = float(amount)
            if famount >= 0:
                self.execaltcointhold = famount
            else:
                self.passanitycheck = False
                self.logger.error("The maximum executable amount %s of altcoin %s is not positive number, updating failed!\n" % (str(amount), self.altcoin))
                return False
        except ValueError:
            self.passanitycheck = False
            self.logger.error("The executable amount %s of altcoin %s is not float, updating failed!\n" % (str(amount), self.altcoin))
            return False

    # TODO: this is not used
    def update_exec_quotecoin_thold(self, amount):
        """
        :amount: the maximum executable amount of quote coin 
        :update the maximum executable amount of quote coin
        """
        try:
            famount = float(amount)
            if famount >= 0:
                self.execquotecointhold = famount
            else:
                self.passanitycheck = False
                self.logger.error("The maximum executable amount %s of quote coin %s is not positive number, updating failed!\n" % (str(amount), self.quotecoin))
                return False
        except ValueError:
            self.passanitycheck = False
            self.logger.error("The executable amount %s of quote coin %s is not float, updating failed!\n" % (str(amount), self.quotecoin))
            return False
    

    def update_altcoin_task_quantity(self, quantity):
        """
        :quantity: total quantity need to be bought or sold 
        :update the task quantity 
        """
        try:
            fquantity = float(quantity)
            if fquantity > 0:
                self.taskquantity = fquantity
                # calculate order size for each order based on total task quantity
                self.calculate_task_order_size()
            else:
                self.passanitycheck = False
                self.logger.error("The task quantity %s is not positive number, updating failed!\n" % str(quantity))
                return False
        except ValueError:
            self.passanitycheck = False
            self.logger.error("The task quantity %s is not float, updating failed!\n" % str(quantity))
            return False
    

    def update_altcoin_order_size(self, quantity):
        """
        :quantity: the base quantity of each order 
        :update the order size for each order
        """
        try:
            fquantity = float(quantity)
            if fquantity > 0:
                self.taskordersize = fquantity
            else:
                self.passanitycheck = False
                self.logger.error("The task order size %s is not positive number, updating failed!\n" % str(quantity))
                return False
        except ValueError:
            self.passanitycheck = False
            self.logger.error("The task order size %s is not float, updating failed!\n" % str(quantity))
            return False
    
    
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
    

    def update_order_price_increment(self, increment):
        """
        :increment: a number of tick
        :update increment on order price
        """
        try:
            if self.instrument_data:
                self.increment = float(increment) * self.instrument_data.tick_size
            else:
                self.increment = float(increment)
            self.logger.debug(f'increment = {self.increment}')
        except ValueError:
            self.passanitycheck = False
            self.logger.error("The increment %s of ticker on order price is not integer, updating failed!\n" % str(increment))
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
                self.logger.debug(f"price floor set to {self.minorderprice}")   # ABOTS-117
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
                self.logger.debug(f"price ceiling set to {self.maxorderprice}")   # ABOTS-117
            else:
                self.passanitycheck = False
                self.logger.error("The task max order price %s is not positive number, updating failed!\n" % str(price))
                return False
        except ValueError:
            self.passanitycheck = False
            self.logger.error("The task max order price %s is not float, updating failed!\n" % str(price))
            return False


    def update_depth_level(self, depthlevel):
        """
        :depthlevel: the depth level in the order book
        :update the levels we want to use
        """
        try:
            idepthlevel = int(float(depthlevel))
            if idepthlevel > 0:
                self.depthlevel = idepthlevel
            else:
                self.passanitycheck = False
                self.logger.error("The depth level %s is not positive number, updating failed!\n" % str(depthlevel))
                return False
        except ValueError:
            self.passanitycheck = False
            self.logger.error("The depth level %s is not integer, updating failed!\n" % str(depthlevel))
            return False


    def update_bid_ask_spread_benchmark(self, bidaskspreadmark):
        """
        bidaskspreadmark: the regular width of bid and ask spread in ticks
        """
        try:
            ibidaskspreadmark = int(float(bidaskspreadmark))
            if ibidaskspreadmark >= 0:
                self.bidaskspreadmark = ibidaskspreadmark
            else:
                self.passanitycheck = False
                self.logger.error("The bid ask spread benchmarch %s is not positive number, updating failed!\n" % str(bidaskspreadmark))
                return False
        except ValueError:
            self.passanitycheck = False
            self.logger.error("The bid ask spread benchmarch %s is not integer, updating failed!\n" % str(bidaskspreadmark))
            return False


    def update_remark(self, remark):
        self.remark = ''
        if remark and isinstance(remark, str):
            try:
                remark = json.loads(remark.replace("\'", "\""))
                if isinstance(remark, dict):
                    self.remark = json.dumps(remark)
            except Exception:
                self.logger.error(f'Invalid remark - {remark}')

        self.logger.info(f'self.remark - {self.remark}')


    def update_updating_break(self, timebreak):
        """
        :timebreak: the maximum seconds the Bot need to wait
        :update the break time when price across the range
        """
        try:
            ftimebreak = float(timebreak)
            if ftimebreak > 0:
                self.updatingtimebreak = ftimebreak
            else:
                self.passanitycheck = False
                self.logger.error("The updating break time %s of orderbook is not positive number, updating failed!\n" % str(timebreak))
                return False
        except ValueError:
            self.passanitycheck = False
            self.logger.error("The updating break time %s of orderbook is not float, updating failed!\n" % str(timebreak))
            return False

    def calculate_check_target(self):
        if (self.target_altcoin > 0) or (self.target_quotecoin > 0) or (self.target_account_position is not None):
            self.check_target = True

    def calculate_task_order_size(self):
        """calculate the order size based on task quantity"""
        # TODO add more checks on input
        if self.minwaitingtime > 0 and self.maxwaitingtime > 0:
            averagewaiting = (self.maxwaitingtime + self.minwaitingtime)/2
            self.taskordersize = self.taskquantity / (24*60*60/averagewaiting)
        else:
            self.passanitycheck = False
            self.logger.error("Total task quantity is below 0")
            return False

    def get_latest_orders(self, num=10, filename=None):
        """retrieve order results from a saved csv file"""

        if isinstance(self.completed_orders, list) and len(self.completed_orders) > 0:
            inum = min(len(self.completed_orders), num)
            return self.completed_orders[-inum:]
        else:
            # TODO loggering
            return False

    def exit_processing(self):
        """"""
        self.broker.client.deregister_resource_usage(
            "book",
            self.broker.client.exchange_name(self.exchange_name),
            self.pair)
        self.logger.info(f'Deregister resource usage: resource_type=book, \
            exchange={self.exchange_name}, pair={self.pair}')

    def create_ghost_order(self, broker, tradingpair, ordertype, orderid):
        """create a ghost order when no Order details can be retrieved"""
        orderdetails = {}
        try:
            orderdetails['OrderID'] = orderid
            orderdetails['TimeStamp'] = int(datetime.datetime.now().timestamp()*1000)
            orderdetails['TradingPair'] = tradingpair
            orderdetails['Exchange'] = broker.get_exchangename()
            orderdetails['AccountKey'] = broker.get_account_id()
            orderdetails['LongShort'] = 'Long' if 'BUY' in ordertype else 'Short'
            orderdetails['Amount'] = -1
            orderdetails['FilledAmount'] = -1
            orderdetails['Price'] = -1
            orderdetails['FilledCashAmount'] = -1
            orderdetails['Status'] = 'Ghost'
            orderdetails['SellCommission'] = -1
            orderdetails['BuyCommission'] = -1
            orderdetails['NetFilledAmount'] = -1
            orderdetails['NetFilledCashAmount'] = -1
            orderdetails['Source'] = 'Automatic'
        except Exception as e:
            self.logger.error("Failed creating ghost Order {0} due to: {1}".format(orderid, e))
            return None
        else:
            return orderdetails

    def get_contract_size(self):
        if self.is_futures_of():
            if self.is_coin_margin(self.instrument_data.altonomy_symbol):
                return int(self.instrument_data.contract_size)

        return None

    def record_order_details(self, broker, tradingpair, ordertype, orderid, skip_sleep=False):
        """record order results to completed_orders list"""
        #
        order_ts = time.time()
        exitloop = False

        while not exitloop:
            # get orderdetails
            orderdetails = broker.client.get_order_details(tradingpair, ordertype, orderid, data_format='legacy', contract_size=self.get_contract_size())
            
            if (isinstance(orderdetails, dict) and (len(orderdetails) > 0) and (orderdetails['Status'] == 'Completed')) or ((time.time() - order_ts) > self.maxwaitingtime):
                exitloop = True
            else:
                time.sleep(3)
        
        if not (isinstance(orderdetails, dict) and len(orderdetails) > 0): ## after max wait time, orderdetails still not available, consider order lost
            ## order details is none, cancel to make sure order is out
            broker.client.cancel_wait(orderid, no_wait=True, remark=self.remark)
            self.logger.error("Order detail of %s is None! (after max waiting time)" % orderid)
            return False
        if orderdetails['Status'] == 'Not Completed':
            orderdetails['Status'] = 'Cancelled'
            broker.client.cancel_wait(orderid, no_wait=True, remark=self.remark)
        elif orderdetails['Status'] == 'Completed':
            if skip_sleep:
                pass
            elif (time.time() - order_ts) < self.minwaitingtime:
                time.sleep(self.minwaitingtime-(time.time() - order_ts))
        else: ## should not be here
            pass
        ###
        self.logger.debug(f'Adding order details for {orderid} : {orderdetails}')
        if isinstance(orderdetails, dict) and len(orderdetails) > 0:
            self.logger.debug(f"inserting {orderid} in list  {orderdetails}")
            self.completed_orders.append(orderdetails.copy())
            self.logger.debug(f"Now completed-orders list is  {self.completed_orders}")
   
    def generate_execution_report(self):
        """generate an execution report"""
        # table name: orderid, timestamp(execution time), tradingpair, exchange, accountkey, type(buy/sell), amount, filled amount, price
        # keys = ['OrderID', 'TimeStamp', 'TradingPair', 'Exchange', 'AccountKey', 'LongShort', 'Amount', 'FilledAmount', 'Price']     
        
        self.logger.debug(f'In generate_execution_report posotion = {self.position}')
        orderdictlist = self.position['completed_orders'] = self.completed_orders
        self.logger.debug(f'In generate_execution_report posotion = {self.position}')
        self.logger.debug(f'In generate_execution_report orderdictlist = {orderdictlist}')
        if isinstance(orderdictlist, list) and len(orderdictlist) > 0:
            self.position['TimeStamp'] = (time.time())*1000
            self.position['Long'] = sum(float(order['FilledAmount']) for order in orderdictlist if order['LongShort'] == 'Long')
            self.position['Short'] = -sum(float(order['FilledAmount']) for order in orderdictlist if order['LongShort'] == 'Short')
            self.position['Cost'] = -sum((float(order['FilledAmount']) * float(order['Price'])) for order in orderdictlist if order['LongShort'] == 'Long')
            self.position['Revenue'] = sum((float(order['FilledAmount']) * float(order['Price'])) for order in orderdictlist if order['LongShort'] == 'Short')

            if self.is_futures_of():
                contract_size = self.get_contract_size()
                self.position['Long'] = self.position['Long'] if self.is_opening_position() else self.position['Long'] * -1.0
                self.position['Short'] = self.position['Short'] * -1.0 if self.is_opening_position() else self.position['Short']
                self.position['Cost'] = -sum((float(order['FilledAmount']) * (contract_size if contract_size else float(order['Price']))) for order in orderdictlist) if self.is_opening_position() else 0
                self.position['Revenue'] = 0 if self.is_opening_position() else sum((float(order['FilledAmount']) * (contract_size if contract_size else float(order['Price']))) for order in orderdictlist)

                if self.dual_side_position_of(self.broker.get_account_id()) and not self.is_opening_position():
                    self.position['Long'], self.position['Short']  = self.position['Short'], self.position['Long']

            try:
                if self.position['Long'] > 0:
                    self.position['WAPX'] = abs(float(self.position['Cost']) / self.position['Long'])
                elif self.position['Short'] < 0:
                    self.position['WAPX'] = abs(float(self.position['Revenue']) / self.position['Short'])
                elif self.position['Long'] < 0:
                    self.position['WAPX'] = abs(float(self.position['Revenue']) / self.position['Long'])
                elif self.position['Short'] > 0:
                    self.position['WAPX'] = abs(float(self.position['Cost']) / self.position['Short'])

            except ZeroDivisionError:
                self.position['WAPX'] = 0.0

            total_filled_amount = sum((float(order['FilledAmount'])) for order in orderdictlist)
            self.position['TotalFilledAmount'] = total_filled_amount
            self.logger.debug(f'total_filled_amount={total_filled_amount}')
            
            total_filled_cash_amount = sum((float(order['FilledCashAmount'])) for order in orderdictlist)
            self.position['TotalFilledCashAmount'] = total_filled_cash_amount
            self.logger.debug(f'total_filled_cash_amount={total_filled_cash_amount}')

        return self.position

    def generate_buyorder_price_vanilla(self):
        """generate buy order price based on lowestask - increment"""
        lowestask = None
        highestbid = None
        orderprice = 0.0
        lowestask = self.broker.get_lowest_ask_and_volume(self.tradingpair)
        highestbid = self.broker.get_highest_bid_and_volume(self.tradingpair)

        if isinstance(lowestask, dict) and isinstance(highestbid, dict) and lowestask['price'] > highestbid['price'] > 0:
            orderprice = round(lowestask['price'] - self.increment, self.orderpricerounding) 
            if self.minorderprice < orderprice < self.maxorderprice:
                # ABOTS-117: add floor/ceiling debug messages
                self.logger.debug(f"order price {orderprice} is within the accepted range ({self.minorderprice}, {self.maxorderprice})")
                return orderprice
            else:
                self.logger.error("The buy price %s is out of price range of min %s and max %s!" 
                % (str(orderprice), str(self.minorderprice), str(self.maxorderprice)))
                time.sleep(self.updatingtimebreak)
                return False
        else:
            self.logger.error("The bid1 price or ask1 price of orderbook of %s is not meaningful!" % self.tradingpair)
            return False
        

    def generate_sellorder_price_vanilla(self):
        """generate sell order price based on highestbid + increment"""
        lowestask = None
        highestbid = None
        orderprice = 0.0
        lowestask = self.broker.get_lowest_ask_and_volume(self.tradingpair)
        highestbid = self.broker.get_highest_bid_and_volume(self.tradingpair)

        if isinstance(lowestask, dict) and isinstance(highestbid, dict) and lowestask['price'] > highestbid['price'] > 0:
            orderprice = round(highestbid['price'] + self.increment, self.orderpricerounding)
            self.logger.info("The bid price is %s, and offer price is %s. The order price is %s." % 
            (str(highestbid['price']), str(lowestask['price']), orderprice))

            if self.minorderprice < orderprice < self.maxorderprice:
                # ABOTS-117: add floor/ceiling debug messages
                self.logger.debug(f"order price {orderprice} is within the accepted range ({self.minorderprice}, {self.maxorderprice})")
                return orderprice
            else:
                self.logger.error("The sell price %s is out of price range of min %s and max %s!" 
                % (str(orderprice), str(self.minorderprice), str(self.maxorderprice)))
                time.sleep(self.updatingtimebreak)
                return False
        else:
            self.logger.error("The bid1 price or ask1 price of orderbook of %s is not meaningful!" % self.tradingpair)
            return False
    

    def _generate_vanilla_order_volume(self):
        order_volume = self.taskordersize * random.uniform(1.0 - self.orderandomrange, 1.0 + self.orderandomrange)
        if order_volume:
            self.logger.debug(f"derived order volume {order_volume} using uniform distribution in range ({1.0 - self.orderandomrange, 1.0 + self.orderandomrange})")
            return round(order_volume, self.ordersizerounding)
        return round(self.taskordersize, self.ordersizerounding)


    def generate_buyorder_volume_vanilla(self):
        order_volume = self._generate_vanilla_order_volume()
        self.logger.info(f"The buy order size is {order_volume}")
        return order_volume
    

    def generate_sellorder_volume_vanilla(self):
        order_volume = self._generate_vanilla_order_volume()
        self.logger.info(f"The sell order size is {order_volume}")
        return order_volume
    
    
    def generate_order(self, orderprice, ordervolume, bidbroker=None, askbroker=None):
        """
        :bidbroker: the broker takes our sell order
        :askbroker: the broker takes our buy order
        """
        limitbuyorderID = None
        limitsellorderID = None
        sell_order_type = f"SELL_{self.order_type}".upper() if self.is_opening_position() else f"BUY_{self.order_type}".upper()
        buy_order_type = f"BUY_{self.order_type}".upper() if self.is_opening_position() else f"SELL_{self.order_type}".upper()
        self.logger.debug(f'sending order {ordervolume}@{orderprice} for {self.tradingpair}')
        # Commission discount has been take out
        if bidbroker:
            limitsellorderID = bidbroker.client.sell_wait(self.tradingpair, orderprice, ordervolume, order_type=sell_order_type, remark=self.remark)[0]
        if askbroker:
            limitbuyorderID = askbroker.client.buy_wait(self.tradingpair, orderprice, ordervolume, order_type=buy_order_type, remark=self.remark)[0]
        
        time.sleep(0.5)
        if limitsellorderID:
            self.logger.debug(f"in generate_order, recording sell {limitsellorderID}")
            self.record_order_details(bidbroker, self.tradingpair, sell_order_type, limitsellorderID)
        if limitbuyorderID:
            self.logger.debug(f"in generate_order, recording buy {limitbuyorderID}")
            self.record_order_details(askbroker, self.tradingpair, buy_order_type, limitbuyorderID)

    
    def execute_buyorder_vanilla(self):
        """hit bid, take offer"""
        orderprice = self.generate_buyorder_price_vanilla()
        ordervolume = self.generate_buyorder_volume_vanilla()
        if not orderprice or not ordervolume:
            self.logger.error(f"The order price {str(orderprice)} or order size {str(ordervolume)} is not meaningful!")
            return False

        self.generate_order(orderprice, ordervolume, askbroker=self.broker)

    
    def execute_sellorder_vanilla(self):
        """hit bid, take offer"""
        orderprice = self.generate_sellorder_price_vanilla()
        ordervolume = self.generate_sellorder_volume_vanilla()
        if not orderprice or not ordervolume:
            self.logger.error(f"The order price {str(orderprice)} or order size {str(ordervolume)} is not meaningful!")
            return False
        
        self.generate_order(orderprice, ordervolume, bidbroker=self.broker)
    

    def generate_buyorder_price_strawberry(self):
        """generate buy order price based on booksize condition,lowestask + increment"""
        orderprice = 0.0
        depthbid = self.broker.get_depth_bid_and_size(self.tradingpair,self.depthlevel)
        depthask = self.broker.get_depth_ask_and_size(self.tradingpair,self.depthlevel)

        if isinstance(depthbid,list) and isinstance(depthask,list) and depthask[0]['price'] > depthbid[0]['price'] > 0 and depthbid[0]['volume'] > 0 and depthask[0]['volume'] >0:
            self.logger.info("Best bid is %s, best offer is %s. Top bid size is %s, top ask size is %s. Order price is %s." % 
            (str(depthbid[0]['price']), str(depthask[0]['price']), str(depthbid[0]['volume']), str(depthask[0]['volume']), orderprice))
            
            self.bidaskspread = round(float(depthask[0]['price'] - depthbid[0]['price']), self.orderpricerounding)
            if self.bidaskspread <= self.bidaskspreadmark * self.minimumticksize:
                if depthbid[0]['volume']/depthask[0]['volume'] >= 5 or sum(d['volume'] for d in depthbid)/sum(d['volume'] for d in depthask) >= 5:
                    orderprice = round(float(depthask[0]['price'] - self.increment), self.orderpricerounding)
                    return orderprice
                else:
                    orderprice = round(float(depthbid[0]['price'] + 0.75*self.bidaskspread),self.orderpricerounding)
                    if self.minorderprice < orderprice < self.maxorderprice:
                        return orderprice
                    else:
                        self.logger.error("The buy price %s is out of price range of min %s and max %s!" 
                        % (str(orderprice), str(self.minorderprice), str(self.maxorderprice)))
                        return False
            else:
                if depthbid[0]['volume']/depthask[0]['volume'] >= 5 and sum(d['volume'] for d in depthbid)/sum(d['volume'] for d in depthask) >= 5:
                    orderprice = round(float(depthask[0]['price'] - self.increment), self.orderpricerounding)
                    return orderprice
                elif depthbid[0]['volume']/depthask[0]['volume'] < 5 and sum(d['volume'] for d in depthbid)/sum(d['volume'] for d in depthask) >= 5:
                    orderprice = round(float(depthbid[0]['price'] + 0.9*self.bidaskspread),self.orderpricerounding) 
                    return orderprice
                # elif depthbid[0]['volume']/depthask[0]['volume'] >= 5 and sum(d['volume'] for d in depthbid)/sum(d['volume'] for d in depthask) < 5:
                #     orderprice = round(depthbid[0]['price'] + 0.7*(depthask[0]['price']-depthbid[0]['price']) + self.increment,self.orderpricerounding)
                else:
                    orderprice = round(float(depthbid[0]['price'] + 0.85*self.bidaskspread),self.orderpricerounding)
                    if self.minorderprice < orderprice < self.maxorderprice:
                        return orderprice
                    else:
                        self.logger.error("The buy price %s is out of price range of min %s and max %s!" 
                        % (str(orderprice), str(self.minorderprice), str(self.maxorderprice)))
                        return False
        else:
            self.logger.error("The best bid price/size or bestnask price/size of orderbook of %s is not meaningful!" % self.tradingpair)
            return False
    

    def generate_sellorder_price_strawberry(self):
        """generate sell order price based on book size condition, highestbid + increment"""
        orderprice = 0.0
        depthbid = self.broker.get_depth_bid_and_size(self.tradingpair,self.depthlevel)
        depthask = self.broker.get_depth_ask_and_size(self.tradingpair,self.depthlevel)

        if isinstance(depthbid,list) and isinstance(depthask,list) and depthask[0]['price'] > depthbid[0]['price'] > 0 and depthbid[0]['volume'] > 0 and depthask[0]['volume'] >0:
            self.logger.info("Best bid is %s, best offer is %s. Top bid size is %s, top ask size is %s. Order price is %s." % 
            (str(depthbid[0]['price']), str(depthask[0]['price']), str(depthbid[0]['volume']), str(depthask[0]['volume']), orderprice))

            self.bidaskspread = round(float(depthask[0]['price'] - depthbid[0]['price']), self.orderpricerounding)
            if self.bidaskspread <= self.bidaskspreadmark * self.minimumticksize:
                if depthask[0]['volume']/depthbid[0]['volume'] >= 5 or sum(d['volume'] for d in depthask)/sum(d['volume'] for d in depthbid) >= 5:
                    orderprice = round(float(depthbid[0]['price'] + self.increment), self.orderpricerounding)
                    return orderprice
                else:
                    orderprice = round(depthask[0]['price'] - 0.75*self.bidaskspread,self.orderpricerounding)
                    if self.minorderprice < orderprice < self.maxorderprice:
                        return orderprice
                    else:
                        self.logger.error("The buy price %s is out of price range of min %s and max %s!" 
                        % (str(orderprice), str(self.minorderprice), str(self.maxorderprice)))
                        return False
            else:
                if depthask[0]['volume']/depthbid[0]['volume'] >= 5 and sum(d['volume'] for d in depthask)/sum(d['volume'] for d in depthbid) >= 5:
                    orderprice = round(float(depthbid[0]['price'] + self.increment), self.orderpricerounding)
                    return orderprice
                elif depthask[0]['volume']/depthbid[0]['volume'] < 5 and sum(d['volume'] for d in depthask)/sum(d['volume'] for d in depthbid) >= 5:
                    orderprice = round(float(depthask[0]['price'] - 0.9*self.bidaskspread),self.orderpricerounding) 
                    return orderprice
                # elif depthask[0]['volume']/depthbid[0]['volume'] >= 5 and sum(d['volume'] for d in depthask)/sum(d['volume'] for d in depthbid) < 5:
                #     orderprice = round(depthask[0]['price'] - 0.7*(depthask[0]['price']-depthbid[0]['price']) - self.increment,self.orderpricerounding)
                else:
                    orderprice = round(float(depthask[0]['price'] - 0.85*self.bidaskspread),self.orderpricerounding)
                    if self.minorderprice < orderprice < self.maxorderprice:
                        return orderprice
                    else:
                        self.logger.error("The buy price %s is out of price range of min %s and max %s!" 
                        % (str(orderprice), str(self.minorderprice), str(self.maxorderprice)))
                        return False
        else:
            self.logger.error("The best bid price/size or bestnask price/size of orderbook of %s is not meaningful!" % self.tradingpair)
            return False
    

    def generate_buyorder_volume_strawberry(self):
        """"""
        ordervolume = self.taskordersize * random.uniform(1.0-self.orderandomrange, 1.0+self.orderandomrange)
        
        if ordervolume:
            self.logger.info("The buy order size is %s" % str(round(ordervolume, self.ordersizerounding)))
            return round(ordervolume, self.ordersizerounding)
        else:
            self.logger.info("The buy order size is %s" % str(round(self.taskordersize, self.ordersizerounding)))
            return round(self.taskordersize, self.ordersizerounding) 
    
    
    def generate_sellorder_volume_strawberry(self):
        """"""
        ordervolume = self.taskordersize * random.uniform(1.0-self.orderandomrange, 1.0+self.orderandomrange)
        
        if ordervolume:
            self.logger.info("The sell order size is %s" % str(round(ordervolume, self.ordersizerounding)))
            return round(ordervolume, self.ordersizerounding)
        else:
            self.logger.info("The sell order size is %s" % str(round(self.taskordersize, self.ordersizerounding)))
            return round(self.taskordersize, self.ordersizerounding)
    

    def execute_buyorder_strawberry(self):
        """smart execution algo"""
        orderprice = self.generate_buyorder_price_strawberry()
        ordervolume = self.generate_buyorder_volume_strawberry()
        if not orderprice or not ordervolume:
            self.logger.error("The order price %s or order size %s is not meaningful!" % (str(orderprice),str(ordervolume)))
            return False

        self.generate_order(orderprice, ordervolume, askbroker=self.broker)

    
    def execute_sellorder_strawberry(self):
        """smart execution algo"""
        orderprice = self.generate_sellorder_price_strawberry()
        ordervolume = self.generate_sellorder_volume_strawberry()
        if not orderprice or not ordervolume:
            self.logger.error("The order price %s or order size %s is not meaningful!" % (str(orderprice),str(ordervolume)))
            return False
        
        self.generate_order(orderprice, ordervolume, bidbroker=self.broker)

    def update_account_balance_meaningfully(self):
        usable, retries = False, 0
        while not usable and retries < 2:
            if retries > 0:
                time.sleep(0.25)
            self.broker.update_account_balance(self.broker.get_account_id())
            if self.is_futures_of():
                usable = self.broker.get_coin_tradable_balance(self.instrument_data.settlement_asset) >= 0.0
            else:
                usable = self.broker.get_coin_tradable_balance(self.quotecoin) >= 0.0 and self.broker.get_coin_tradable_balance(self.altcoin) >= 0.0
            retries += 1
        if not usable:
            self.logger.warning(f"unable to get meaningful balance data after {retries} retries")

    def update_order_book_meaningfully(self):
        self.broker.clear_broker()
        usable, retries = False, 0
        while not usable and retries < 5:
            if retries > 0:
                time.sleep(0.25)
            self.broker.update_orderbook(self.tradingpair)
            lowestask = self.broker.get_lowest_ask_and_volume(self.tradingpair)
            highestbid = self.broker.get_highest_bid_and_volume(self.tradingpair)
            usable = lowestask is not None and highestbid is not None
            retries += 1
        if not usable:
            self.logger.warning(f"unable to get meaningful bid ask data after {retries} retries")

    @functools.lru_cache(maxsize=None)
    def exchange_name_of(self, account_id):
        return self.broker.client.get_account_config('exchange_name', account_id=account_id)

    @functools.lru_cache(maxsize=None)
    def name_of(self, account_id):
        return self.broker.client.get_account_config('name', account_id=account_id)

    @functools.lru_cache(maxsize=None)
    def is_futures_of(self):
        return self.instrument_type == "FUTURES"

    @functools.lru_cache(maxsize=None)
    def dual_side_position_of(self, account_id):
        dual_side_position = self.broker.client.get_account_config('dual_side_position', account_id=account_id)
        return eval(dual_side_position) if dual_side_position else False

    @functools.lru_cache(maxsize=None)
    def exchange_id_of(self, account_id):
        return self.broker.client.get_account_config('exchange_id', account_id=account_id)

    def is_coin_margin(self, altonomy_symbol):
        alto_symbol = altonomy_symbol.split('/')
        return len(alto_symbol) >= 3 and alto_symbol[2] == 'COIN'

    def is_opening_position(self) -> bool:
        if self.order_type in ['LIMIT_CLOSE', 'MARKET_CLOSE']:
            return False
        return True

    def target_account_position_reached(self) -> bool:

        pair_long_position = self.broker.get_coin_tradable_balance(f'{self.tradingpair} Long')
        pair_short_position = self.broker.get_coin_tradable_balance(f'{self.tradingpair} Short')


        self.logger.debug(f"checking account position: pair_long_position={pair_long_position}, pair_short_position={pair_short_position}")
        
        if self.dual_side_position_of(self.broker.get_account_id()):
            if self.target_account_position > 0:
                if self.orderdirection == 'BUY':
                    return pair_long_position >= self.target_account_position if self.is_opening_position() else True
                else:
                    return True if self.is_opening_position() else pair_long_position <= self.target_account_position
            if self.target_account_position == 0:
                if self.orderdirection == 'BUY':
                    return True if self.is_opening_position() else pair_short_position <= 0.0
                else:
                    return True if self.is_opening_position() else pair_long_position <= 0.0
            if self.target_account_position < 0:
                if self.orderdirection == "BUY":
                    return True if self.is_opening_position() else pair_short_position <= math.fabs(self.target_account_position)
                else:
                    return pair_short_position >= math.fabs(self.target_account_position) if self.is_opening_position() else True
        else:
            if self.target_account_position > 0:
                if self.orderdirection == 'BUY':
                    return pair_long_position >= self.target_account_position
                else:
                    return pair_short_position > 0 or pair_long_position <= self.target_account_position
            if self.target_account_position == 0:
                if self.orderdirection == 'BUY':
                    return pair_long_position >= self.target_account_position or (pair_short_position <= 0 and pair_long_position <= 0)
                else:
                    return pair_short_position >= self.target_account_position or (pair_short_position <= 0 and pair_long_position <= 0)
            if self.target_account_position < 0:
                if self.orderdirection == 'BUY':
                    return pair_short_position <= math.fabs(self.target_account_position) or pair_long_position > 0
                else:
                    return pair_short_position >= math.fabs(self.target_account_position)

        return False

    def validate_ref_orderbook(self, ob):
        if len(ob.bids) == 0 or len(ob.asks) == 0:
            # ignore empty books, probably an error
            self.logger.error(f'ref_orderbook - empty book')
            return False
        if not ob.timestamp:
            # ignore books without timestamps, probably an error
            self.logger.error(f'ref_orderbook - book without timestamps')
            return False
        if time.time() - float(ob.timestamp) > (
                config.REFERENCE_ORDERBOOK_REFRESH_MAX_TIME):
            self.logger.error(f'ref_orderbook - staled market data - timestamp={ob.timestamp}')
            return False

        return True

    def get_reference_price(self, ref_pair, ref_exchange):
        ref_price = None
        try:
            ob = self.ref_client.get_orderbook(pair=ref_pair, exchange=ref_exchange)
            if not self.validate_ref_orderbook(ob):
                return ref_price

            self.logger.debug(f'obtained reference orderbook for {ref_pair} - {ref_exchange}')
            ref_price = ob.get(ref_pair, {}).get({"BUY": "asks", "SELL": "bids"}.get(self.orderdirection), [{}])[0].get("price", 0)
        except Exception as e:
            self.logger.error(f'Error in get_reference_price - {e}')

        self.logger.debug(f'ref_price = {ref_price}')
        return ref_price

    def tick(self):
        """Update orderbook in every tick and update account balance in every 5 minutes"""
        # Setup condition to update account balance in every 300s
        do_update_accout = False
        if int(time.time() - self.account_ts) > config.ACCOUNT_TS: # 300s
            self.account_ts = time.time()
            do_update_accout = True
        
        if self.broker:

            if do_update_accout:
                self.update_account_balance_meaningfully()
                #self.update_order_book_meaningfully() #??
            
            # balance check to make sure there is enough balance
            quotecoinbalance = self.broker.get_coin_tradable_balance(self.quotecoin)
            altcoinbalance = self.broker.get_coin_tradable_balance(self.altcoin)
            askprice = self.broker.get_lowest_ask_and_volume(self.tradingpair)
            # quotecoinexecuted, altcoinexecuted = self.get_executed_amount()

            if self.trigger_condition != ' ':
                try:
                    ref_exchange, ref_pair, trigger_direction, trigger_price = [value.strip() for value in self.trigger_condition.split(";")]
                    # ABOTS-97: make exchange name non case sensitive
                    ref_exchange = self.broker.client.exchange_name(ref_exchange.capitalize())
                    ref_price = self.get_reference_price(ref_pair, ref_exchange)
                    self.logger.info(f"Evaluating trigger condition {ref_price} {trigger_direction} {float(trigger_price)}")
                    if not ref_price or not getattr(operator, trigger_direction)(ref_price, float(trigger_price)):
                        self.logger.info(f"trigger condition {ref_price} {trigger_direction} {trigger_price} is not met")
                        time.sleep(self.updatingtimebreak)
                        return False
                except Exception as e:
                    self.logger.debug(f"failed to evaluation trigger condition due to {e}")
                    return False

            # ABOTS-177: add stop condition on balance
            if self.stop_condition != ' ':
                try:
                    stop_asset, stop_direction, stop_value = [value.strip() for value in self.stop_condition.split(";")]
                    available_balance = self.broker.get_coin_tradable_balance(stop_asset)
                    self.logger.info(f"Evaluating stop condition {stop_asset} {available_balance} {stop_direction} {float(stop_value)}")
                    if getattr(operator, stop_direction)(available_balance, float(stop_value)):
                        self.logger.error(f"stop condition {available_balance} {stop_direction} {stop_value} has been met")
                        time.sleep(self.updatingtimebreak)
                        return False
                except Exception as e:
                    self.logger.debug(f"failed to evaluation stop condition due to {e}")

            # ALTEX-143: do not check for balances if reserve amount has been set to -1
            if self.reservedaltcoin == -1 and self.reservedquotecoin == -1:
                self.logger.debug(f"skipping balance vs reserved altcoin/quotecoin checks")
            elif self.is_futures_of():
                pass
            elif self.orderdirection == 'BUY':
                if quotecoinbalance < self.reservedquotecoin:
                    self.logger.error("The quote coin balance %s is less than reserved requirment %s of quote coin"
                    % (str(quotecoinbalance), str(self.reservedquotecoin)))
                    time.sleep(self.updatingtimebreak)
                    return False

                if askprice and (self.taskordersize * askprice['price']) > quotecoinbalance:
                    self.logger.error("There is not enough quote coin %s balance of %s!" %(self.quotecoin, str(quotecoinbalance)))
                    # TODO report warning to UI instead
                    time.sleep(self.updatingtimebreak)
                    self.update_order_book_meaningfully()
                    return False
                # if quotecoinexecuted >= self.execquotecointhold:
                #     self.logger.info(" %s of %s has been spent, equal or more than pre-determined amount of %s." 
                #     %(quotecoinexecuted, self.quotecoin, self.execquotecointhold))
                #     # TODO shall we exit the bot
                #     # raise SystemExit
                #     time.sleep(self.updatingtimebreak)
                #     return False

            elif self.orderdirection == 'SELL':
                if altcoinbalance < self.reservedaltcoin:
                    self.logger.error("The alt coin balance %s is less than reserved requirment %s of alt coin"
                    % (str(altcoinbalance), str(self.reservedaltcoin)))
                    time.sleep(self.updatingtimebreak)
                    return False

                if self.taskordersize > altcoinbalance:
                    self.logger.error("There is not enough altcoin %s balance of %s!" %(self.altcoin, str(altcoinbalance)))
                    # TODO report warning to UI instead
                    time.sleep(self.updatingtimebreak)
                    self.update_order_book_meaningfully()
                    return False
                
                # if altcoinexecuted >= self.execaltcointhold:
                #     self.logger.info(" %s of %s has been sold, equal or more than pre-determined amount of %s." 
                #     %(altcoinexecuted, self.altcoin, self.execaltcointhold))
                #     # TODO shall we exit the bot
                #     # raise SystemExit
                #     time.sleep(self.updatingtimebreak)
                #     return False

            else:
                self.logger.error("Returning False as conditions did not match")
                return False
            
            # always clear the broker's orderbook at beginning of each tick
            time.sleep(self.updatingtimebreak)
            self.update_order_book_meaningfully()

            # check if we have already reached the target
            if self.check_target:
                if self.target_altcoin > 0.0:
                    if self.target_altcoin <= float(self.position.get('TotalFilledAmount',0)):
                        self.logger.info(f"target altcoin amount reached: {self.target_altcoin} <= {self.position.get('TotalFilledAmount',0)}")
                        time.sleep(self.updatingtimebreak)
                        return False
                if self.target_quotecoin > 0.0:
                    if self.target_quotecoin <= float(self.position.get('TotalFilledCashAmount',0)):
                        self.logger.info(f"target quotecoin amount reached: {self.target_quotecoin} <= {self.position.get('TotalFilledCashAmount',0)}")
                        time.sleep(self.updatingtimebreak)
                        return False
                if self.target_account_position is not None:
                    if self.target_account_position_reached():
                        self.logger.info(f"target account position reached: {self.target_account_position}")
                        time.sleep(self.updatingtimebreak)
                        return False
            
            self.logger.info("All conditions matched.")
            return True

        else:
            self.logger.error("Returning from tick as conditions did not match")
            return False
    