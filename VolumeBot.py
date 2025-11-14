#########################################################################################
# Date: 2018.03
# Auther: happyshiller 
# Email: happyshiller@gmail.com
# Github: https://github.com/happyshiller/Altonomy
# Function: create trading volume
#########################################################################################

import csv
import datetime
import itertools
import json
import logging
import math
import os
import platform
import random
import signal
import threading
import time
import traceback
import warnings
from collections import deque
from logging import StreamHandler
from math import isclose
from types import SimpleNamespace
from altonomy.core.Order import OrderState

# import marketlog
import requests
# import schedule
from altonomy.core import client
from altonomy.core import logger as _logger
from altonomy.core.OrderBook import OrderBook, LegacyOrderBook
from altonomy.exchanges.Order import Order
from altonomy.services.exchange_broker import exchange_broker
from numpy.random import choice

from . import config

# lib for detecting memory leak 
# import tracemalloc
# import gc  
# import objgraph 

# define the directory where csv file saved
# THIS_FOLDER = os.path.dirname(os.path.abspath(__file__))
# filename = os.path.join(THIS_FOLDER, 'order.csv')



SELL = -1
BUY = 1

class SendOrderThread(threading.Thread):
    def __init__(self,func,args=()):
        super(SendOrderThread, self).__init__()
        self.func = func
        self.args = args

    def run(self):
        self.result = self.func(*self.args)

    def get_result(self):
        try:
            return self.result
        except Exception:
            return None


class EmailLogHandler(StreamHandler):
    def __init__(self, subject, log_logger):
        StreamHandler.__init__(self)
        self.subject = subject
        self.buffer = []
        self.client = client()
        self.record_errors = False
        self.log = log_logger
        schedule.every(config.VOLUMEBOT_EMAIL_ALERT_TIME_PERIOD_MIN).minutes.do(self.run_email_scheduler).run()

        self.scheduler_thread = threading.Thread(target=self.schedule_thread)
        self.scheduler_thread.daemon = True
        self.scheduler_thread.start()

    def schedule_thread(self):
        while True:
            schedule.run_pending()
            time.sleep(30)

    def update_subject(self, subject):
        self.subject = subject

    def run_email_scheduler(self):
        self.record_errors = True
        mm_config = self.client.get_directory_details("market_making_support")
        sleep_time = float(mm_config.get("email_interval", 120))
        recipients = mm_config.get("email_recipients", 'support@altonomy.com').split(";")
        time.sleep(sleep_time)
        if len(self.buffer):
            self.client.submit_email_request(subject=self.subject, message_text='<br/>'.join(self.buffer),
                                             message_html='<br/>'.join(self.buffer),
                                             recipients=recipients)
        self.buffer.clear()
        self.record_errors = False

    def emit(self, record):
        msg = self.format(record)
        if self.record_errors:
            self.buffer.append(msg)

def Order(SimpleNamespace):
    pass

class VolumeBot():
    """VolumeBot is a generic class designed to meet clients' needs on trading volume....."""

    def __init__(self, brokers, clerk, altcoin, quotecoin, logger=None):
        """
        :机器人UI接口与参数介绍
        :
        :在UI页面一, 选择好一个代币(小币), 一个交易所, 一个交易对(请注意传给机器人的是计价币种, 而不是交易对)
        :使用上述三项初始化机器人对象. 初始化过程包含更新account balance and orderbook
        :
        :在UI页面二, 显示基本信息-包括可用钱(计价币种), 可用币数(代币), 冻结钱数(计价币种), 冻结币数(代币)
        :对应机器人UI接口函数: get_quotecoin_tradable_balance(), get_altcoin_tradable_balance(), 
        :                    get_quotecoin_frozen_balance(), get_altcoin_frozen_balance()
        :同样要显示交易对名称和盘口的买1, 卖1
        :对应机器人UI接口函数: get_tradingpair_name(), get_bid1_price(), get_ask1_price()
        :对比旧系统，新添加了进度 % 来显示完成度 get_progress()
        :新系统还添加了积累的未完成买单和卖单量(代币). get_netlong_exposure(), get_netshort_exposure() 
        :甚至可以进一步计算与已下单的量的 %, 并显示在未完成量后面.  
        :
        :在页面二, UI可以调用接口函数来更新机器人配置. 配置更新完成后, 首先要保存配置, 在这里调用所有的更新配置函数。
        :然后调用机器人配置完整性检测函数sanity_check(), 只有在检测结果为True时, 才启动机器人
        :更新配置的接口函数包括:
        :更新下单的优先级-随机, 先买入, 先卖出? update_order_priority()
        :更新每天的刷量任务(按小币记) update_altcoin_daily_task_quantity()
        :更新距离中间价的随机盘口差 % update_spread_shock()
        :更新报警和下单优先的阈值
        :更新最小和最大的等待时间间隔 update_min_time_break(), update_max_time_break()
        :更新最低需保留的代币(小币)数量和计价币数量 update_reserved_altcoin(), update_reserved_quotecoin()
        """
        # list of brokers
        # len(brokers) == 1 or 2 (two accounts within the same exchange)
        self.email_log_logger = _logger('Email Handler')
        self.custom_handler = EmailLogHandler(subject=f'Errors in Volume Bot for {altcoin}/{quotecoin}', log_logger=self.email_log_logger)
        self.custom_handler.setLevel(logging.ERROR)
        self.custom_handler.setFormatter(logging.Formatter('%(message)s'))
        self.logger = _logger(__name__, custom_handler=self.custom_handler, port=(logger.port if logger else 0))
        self.logger.debug('starting vol bot init...')
        self.brokers = brokers

        # Account names 
        self.logger.debug('fetching account names...')
        self.account_names = [broker.client.get_account_config('name') for broker in self.brokers]
        self.logger.debug('setting params...')

        self.clerk = clerk
        # only 1 altcoin
        self.altcoin = altcoin
        # only 1 quotecoin 
        self.quotecoin = quotecoin
        # for instance: ELFBTC, ITCBTC
        self.tradingpair = None
        # order priority
        # RANDOM: randint(-1, 1), -1: sell(), +1: buy()
        # BUYFIRST: buy(), then sell() 
        # SELLFIRST: sell(), then buy()
        self.orderpriority = 'RANDOM'
        # copy of original order priority
        self.originalpriority = 'RANDOM'
        # minimum waiting time 2 ~ 8
        self.minwaitingtime = 2
        # maximum waiting time 2 ~ 8
        self.maxwaitingtime = 8
        # random waiting time random(minwaitingtime, maxwaitingtime)
        self.orderwaitingtime = 0.0
        # how much altcoin need to be brushed in 24 hours
        self.taskquantity = 0.0
        # average single order size based on task quantity
        # equal to taskquantity / orderwaitingtime
        self.taskordersize = 0.0
        # fixed % of average single order size 
        self.fixedorderperc = 0.5
        # percentage randomness
        self.percentagerand = 0.2
        # shock on bid-ask spread range(0.0 ~ 0.5)
        self.spreadshock = 0.0
        # start with at least this amount of spread
        self.minimumspread = 0.0
        # minimum tick size
        self.minimumticksize = 0.0
        # precision details of an order: price, size, value
        # self.orderprecision = {}
        self.orderpricerounding = 0
        self.ordersizerounding = 0
        self.ordervaluerounding = 0
        # bid ask spread
        self.bidaskspread = 0.0
        # benchmark coin/pair used for generating scale factor 
        self.benchmarkpair = 'BTCUSDT'
        # scale the task order size with this factor
        self.scalefactor = 1.0
        # list of trading amount stored for calculation of scale factor
        self.amounthistory = []
        # account updating time stamp
        self.account_ts = 0
        # order generating time stamp
        self.order_ts = 0
        # recording time stamp
        self.recording_ts = 0
        # trader's position (long + short)
        # {
        #   'timestamp': time.time()
        #   'long': +100, sum(filled amount of long)
        #   'short': -80, -sum(filled amount of short)
        #   'net': +20, long + short
        #   'frozenaltcoin': sum(amount of short - filled amount of short)
        #   'frozenquotecoin': sum(amount of long - filled amount of long)*buy price)
        #   'completed': 100, max(abs(long), abs(short))
        #   'progress': 25%, completed/task quantity
        # }
        # net position = long + short
        # completed task = max(abs(long), abs(short))
        # progress = completed task / task quantity
        self.position = {}
        # the absoluate value of tolerance level of delta exposure
        self.riskthreshold = 0.0
        # the amount of alt coin shall leave untouched
        self.reservedaltcoin = 0.0
        # the amount of quote coin shall leave untouched
        self.reservedquotecoin = 0.0
        # maximum order size 
        self.orderBookTimeDelay = 0.0
        self.maxTimeDelay = 1.0
        self.last_run_time = 0
        self.max_upd_frequency = 0.0
        self.sleep_time = 0.0
        # calculate how many times the order failed to reach the exchange over a specific time
        self.failratio = 0.0
        # the percentage we want to add on to the bid/offer for the quoting price [0,1)
        self.quoteincrement = 0.0
        #volume catch up times
        self.catchuptimes = 2.0
        self.memory_window = 20
        self.execution_mode = 'MULTI'
        self.execution_window = 0.2
        self.coordination_flag = False
        self.abandon_flag = False
        self.last_buy_order = ''
        self.last_sell_order = ''
        self.start_time_utc = datetime.datetime.now(datetime.timezone.utc)
        self.internal_wait_time = 0.2
        self.max_altcoin_imbalance = -1
        self.max_quotecoin_imbalance = -1
        self.explicit_wait_time = 0
        self.explicit_wait_random = 0
        self.minimum_order_size = 0
        self.minimum_order_notional = 0
        self.cummulative_cleared_vol = 0.0
        self.spread_clearing_threshold = -1.0
        self.spread_clearing_depth = 5
        self.spread_clearing_min_order_multiplier = 1.0
        self.spread_clearing_max_vol_round = 0.0
        self.spread_clearing_max_vol_cummulative = 0.0
        self.spread_clearing_min_order_size = 0.0

        # hidden order probes
        self.probe_mode = False
        self.hidden_order_lifespan = -1
        self.probe_altcoin_limit = -1
        self.probe_quotecoin_limit = -1
        self.probe_altcoin_spent = 0
        self.probe_quotecoin_spent = 0
        self.hidden_orders = {}
        self.clear_hidden_orders_on_update = False
        # conservative pricing mode
        self.conservative_mode = False
        self._safe_size = None
        self._position_hold_time = 10800 # 3 hours
        self._position_clearing_enabled = True

        # good to go signal
        self.passanitycheck = True
        # dictionary of sum of account balance within the same broker
        # {
        #   'broker': name,
        #   'account_1': {'BTC': 200, 'ELF': 50000, ......},
        #   'account_2': {'BTC': 500, 'ELF': 10000, ......}
        # }
        self.asset = {}
        # logger
        # initiate the trading pair, and precision details
        self.logger.debug('initializing trading pairs and precisions...')
        self.init_tradingpair()
        # update orderbook once at initialization
        self.logger.debug('updating orderbooks...')
        self.init_orderbook()
        # update account balance is performed in broker obj
        # self.init_account_balance()
        # remove orders booked 24 hours ago
        # TODO probably need to reactive this function in the future 
        # self.remove_orders_over24h()
        # load saved pending orders within past 24 hours
        # self.load_pending_orders()
        # update the status of pending orders from last step
        self.logger.debug('processing pending orders...')
        try:
            self.pending_order_processing()
        except:
            self.logger.error("failed to process pending orders")
        # initiate trader's position with either 0 or saved file
        self.botid = 0
        try:
            self.init_position()
        except:
            self.logger.error("failed init position")
        # update trader's position based on updated orders file
        if self.clerk and config.RECORD_TO_DB:
            self.logger.debug('registering bot...')
            self.register_bot()
            if len(self.account_names) == 1:
                self.custom_handler.update_subject(f'Errors in Volume Bot with ID: {self.botid} with account name {self.account_names} for {altcoin}/{quotecoin}')
            else:
                self.custom_handler.update_subject(f'Errors in Volume Bot with ID: {self.botid} with account names {self.account_names} for {altcoin}/{quotecoin}')            

    def init_tradingpair(self):
        """generate tradingpair based on alt & quote coin, and its minimum tick and other precision details"""
        # empty string evaluate to False in Python
        if self.altcoin and self.quotecoin:
            self.tradingpair = self.altcoin + self.quotecoin
            # there could be two or three precision info
            precision = self.brokers[0].get_pair_precision(self.tradingpair)
            if isinstance(precision, dict) and len(precision) > 0:
                try:
                    self.orderpricerounding = int(precision['priceprecision']) if precision.get('priceprecision') else 0
                    self.ordersizerounding = int(precision['amountprecision']) if precision.get('amountprecision') else 0
                except Exception as e:
                    self.logger.error("Get precision info failed, %s!\n" % str(e))
                    raise e
                self.ordervaluerounding = int(precision.get('valueprecision', 0)) if precision.get('valueprecision') else 0
            else:
                self.logger.error("There is NO valid precision info!\n")
                return False
            # calculate the minimum tick
            if isinstance(self.orderpricerounding, int) or isinstance(self.orderpricerounding, float):
                self.minimumticksize = round(float(10 ** -self.orderpricerounding), self.orderpricerounding)
                self.logger.debug(f"minimum tick size = {self.minimumticksize}, exchange order price rounding = {self.orderpricerounding}")
        else:
            self.logger.error("Neither altcoin %s or quotecoin %s should be empty!\n" % (self.altcoin, self.quotecoin))
            return False


    def init_account_balance(self):
        """ """
        warnings.warn("The account balance is updated during the initialization of brokers", DeprecationWarning)
        if len(self.brokers) == 1 or len(self.brokers) == 2:

            for broker in self.brokers:
                broker.update_account_balance(broker.get_account_id())
        else:
            self.logger.error("Number of brokers is neither 1 nor 2, updating account balance during initialization failed!\n")
            return False


    def init_orderbook(self):
        """update the orderbook of given trading pair during class initialization"""
        if self.brokers[0]:
            self.update_order_book_meaningfully()
        else:
            self.logger.error("The broker list is empty, updating orderbook during initialization failed!\n")
            return False


    def init_position(self):
        """initialize trader's position with either last saved record or 0"""
        # database version
        if self.clerk and config.RECORD_TO_DB:
            self.generate_trader_position()
            return

        filename = self.altcoin.upper() + '_' + self.quotecoin.upper() + '_' + self.brokers[0].get_exchangename() +'_Position.csv'

        if not self.load_trader_position(filename):
            self.position['timestamp'] = 0.0
            self.position['long'] = 0.0
            self.position['short'] = 0.0
            self.position['net'] = 0.0
            self.position['frozenaltcoin'] = 0.0
            self.position['frozenquotecoin'] = 0.0
            self.position['completed'] = 0.0
            self.position['progress'] = 0.0
            self.position['benchmark'] = 0.0
            self.position['filledcashamount'] = 0.0

    def init_pending_order_list(self):
        """ """
        pass



    def update_brokers(self, brokers):
        """
        :brokers: list of brokers, maximum 2 brokers
        :update the broker list
        """
        if len(brokers) == 1 or len(brokers) == 2:
            self.brokers = brokers
        else:
            self.logger.error("Reset broker list failed!\n")
            return False


    def update_altcoin(self, acoin):
        """
        :acoin: alt coin, such as ELF
        :update the altcoin
        """
        if acoin:
            self.altcoin = acoin
        else:
            self.logger.error("The AltCoin variable is empty, updating failed!\n")
            return False


    def update_quotecoin(self, bcoin):
        """
        :bcoin: quote coin, such as BTC
        :update the quotecoin
        """
        if bcoin:
            self.quotecoin = bcoin
        else:
            self.logger.error("The QuoteCoin variable is empty, updating failed!\n")
            return False


    def update_order_priority(self, priority):
        """
        :priority: 'RANDOM', 'BUYFIRST', 'SELLFIRST'
        :update order priority
        """
        if priority == 'RANDOM' or priority == 'BUYFIRST' or priority == 'SELLFIRST':
            self.orderpriority = priority
            self.originalpriority = self.orderpriority
        else:
            self.logger.error("The order priority %s is not among 'RANDOM', 'BUYFIRST', 'SELLFIRST', updating failed!\n" % priority)
            self.passanitycheck = False
            return False


    def update_min_time_break(self, timebreak):
        """update the minimum waiting time between each order"""
        try:
            itimebreak = int(float(timebreak))
            if itimebreak > 0:
                self.minwaitingtime = itimebreak
            else:
                self.logger.error("The minimum waiting time %s is not positive number, updating failed!\n" % str(timebreak))
                self.passanitycheck = False
                return False
        except ValueError:
            self.logger.error("The minimum waiting time %s is not integer, updating failed!\n" % str(timebreak))
            self.passanitycheck = False
            return False


    def update_max_time_break(self, timebreak):
        """update the maximum waiting time between each order"""
        try:
            itimebreak = int(float(timebreak))
            if itimebreak > 0:
                self.maxwaitingtime = itimebreak
            else:
                self.logger.error("The maximum waiting time %s is not positive number, updating failed!\n" % str(timebreak))
                self.passanitycheck = False
                return False
        except ValueError:
            self.logger.error("The maximum waiting time %s is not integer, updating failed!\n" % str(timebreak))
            self.passanitycheck = False
            return False

    def update_max_time_delay(self, timeDelay):
        """update the minimum waiting time between each order"""
        try:
            ftimeDelay = float(timeDelay)
            if ftimeDelay > 0:
                self.maxTimeDelay = ftimeDelay
            else:
                self.logger.error("The minimum waiting time %s is not positive number, updating failed!\n" % str(timeDelay))
                self.passanitycheck = False
                return False
        except ValueError:
            self.logger.error("The minimum waiting time %s is not float, updating failed!\n" % str(timeDelay))
            self.passanitycheck = False
            return False

    def update_altcoin_daily_task_quantity(self, quantity):
        """
        :quantity: the volume target within 24 hours
        :update the task quantity 
        """
        try:
            fquantity = float(quantity)
            if fquantity > 0:
                self.taskquantity = fquantity
                # calculate order size for each order pair based on total task quantity
                self.calculate_task_order_size()
            else:
                self.logger.error("The task quantity %s is not positive number, updating failed!\n" % str(quantity))
                self.passanitycheck = False
                return False
        except ValueError:
            self.logger.error("The task quantity %s is not float, updating failed!\n" % str(quantity))
            self.passanitycheck = False
            return False


    def update_fixed_portion_order(self, fixedperc):
        """
        :fixedperc: between 0 ~ 100%
        :decide the fixed portion of the volume of a single order 
        """
        # total order size = fixed portion + (1-fixed portion)*random()
        try:
            ffixedperc = float(fixedperc)
            if 0.0 < ffixedperc < 1.0:
                self.fixedorderperc = ffixedperc
            else:
                self.fixedorderperc = 0.5
                self.logger.warning("The fixed portion %s is not in the range of 0.0~1.0, take default value 0.5.\n" % str(fixedperc))
        except ValueError:
            self.fixedorderperc = 0.5
            self.logger.warning("The fixed portion %s is not float, updating failed! Take default value 0.5.\n" % str(fixedperc))

    def update_percentage_random(self, percrand):
        """
        :percrand: between 0 ~ 100%
        :decide the randomness strength (final strength is constrained between 0% to 200%)
        """
        try:
            fpercrand = float(percrand)
            if 0.0 < fpercrand < 1.0:
                self.percentagerand = fpercrand
            else:
                self.percentagerand = 0.2
                self.logger.warning("The percentage randomess %s is not in the range of 0.0~1.0, take default value 0.2.\n" % str(percrand))
        except ValueError:
            self.percentagerand = 0.2
            self.logger.warning("The percentage randomess %s is not float, updating failed! Take default value 0.2.\n" % str(percrand))

    def update_spread_shock(self, shock):
        """
        :shock: absolute value of (-50% ~ 50%)
        :update the range within which to generate random shock
        """
        # the range of shock on bid-ask spread is (0.0 ~ 0.5)
        try:
            fshock = float(shock)
            if fshock > 0.99:
                self.spreadshock = 0.99
            elif fshock < 0:
                self.spreadshock = 0.0
            else:
                self.spreadshock = fshock
        except ValueError:
            self.logger.error("The spread shock %s is not float, updating failed!\n" % str(shock))
            self.passanitycheck = False
            return False


    def update_benchmark_pair(self, pair='BTCUSDT'):
        """update the benchmark trading pair"""
        if pair:
            self.benchmarkpair = pair
        else:
            self.logger.error("The benchmark pair %s is empty, updating failed!\n" % pair)
            self.passanitycheck = False
            return False


    def update_risk_threshold(self, threshold=0.0):
        """
        :threshold: risk limit
        :update the absoluate value of tolerance level of delta exposure
        """
        try:
            fthreshold = float(threshold)
            if fthreshold > 0.0:
                self.riskthreshold = fthreshold
            else:
                # default 5% of the total task quantity
                self.riskthreshold = self.taskquantity * 0.05
                self.logger.error("The risk threshold %s is not a positive number. Take default value 5 percent!\n" % str(threshold))
        except ValueError:
            # default 5% of the total task quantity
            self.riskthreshold = self.taskquantity * 0.05
            self.logger.error("The risk threshold %s is not float, updating failed! Take default value of 5 percent!\n" % str(threshold))


    def update_minimum_spread(self, bidaskspread):
        """
        :bidaskspread: the minimum spread to generate washing order pair
        :update the minimum spread 
        """
        try:
            fspread = float(bidaskspread)
            if fspread >= 0.0:
                self.minimumspread = fspread
            else:
                self.minimumspread = 0.0
                self.logger.warning("The minimum spread %s is not a positive number. Take default value 0.0!\n" % str(bidaskspread))
        except ValueError:
                self.minimumspread = 0.0
                self.logger.warning("The minimum spread %s is not float, updateing failed! Take default value 0.0!\n" % str(bidaskspread))


    def update_reserved_altcoin(self, amount):
        """update the reserved amount of altcoin"""
        try:
            famount = float(amount)
            if famount > 0.0:
                self.reservedaltcoin = famount
            else:
                self.reservedaltcoin = 0.0
                self.logger.warning("The reserved amount of altcoin %s is not a positive number. Take default value 0.0!\n" % str(amount))
        except ValueError:
            self.reservedaltcoin = 0.0
            self.logger.warning("The reserved amount of altcoin %s is not float, updating failed! Take default value 0.0!\n" % str(amount))


    def update_reserved_quotecoin(self, amount):
        """update the reserved amount of quote coin"""
        try:
            famount = float(amount)
            if famount > 0.0:
                self.reservedquotecoin = famount
            else:
                self.reservedquotecoin = 0.0
                self.logger.warning("The reserved amount of quote coin %s is not a positive number. Take default value 0.0!\n" % str(amount))
        except ValueError:
            self.reservedquotecoin = 0.0
            self.logger.warning("The reserved amount of quote coin %s is not float, updating failed! Take default value 0.0!\n" % str(amount))

    def update_bidask_spread(self, spread):
        """update the bid-ask spread"""
        pass

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


    def update_quoteratio(self,percentage):
        """update the quote percentage added to the top of book, between 0 and 1"""
        try:
            fpercentage = float(percentage)
            if fpercentage > 0 and fpercentage < 1:
                self.quoteincrement = fpercentage
            else:
                self.logger.error("The quote ratio %s is not between 0 and 1, updating failed!\n" % str(percentage))
                self.passanitycheck = False
                return False
        except ValueError:
            self.logger.error("The quote ratio %s is not float, updating failed!\n" % str(percentage))
            self.passanitycheck = False
            return False


    def update_catchuptimes(self,catchuptimes):
        """update how many times the catch up quoting size will be comparing to the original size"""
        try:
            fcatchuptimes = float(catchuptimes)
            if fcatchuptimes > 0:
                self.catchuptimes = fcatchuptimes
            else:
                self.logger.error("The catchup times %s is not positive, updating failed!\n" % str(catchuptimes))
                self.passanitycheck = False
                return False
        except ValueError:
            self.logger.error("The catchup times %s is not float, updating failed!\n" % str(catchuptimes))
            self.passanitycheck = False
            return False

    def update_memory_window(self, memory_window):
        """memory window is the number of cycles used in fail ratio calculation"""
        try:
            i_memory_window = int(float(memory_window))
            if i_memory_window > 0:
                self.memory_window = i_memory_window
            else:
                self.logger.error("The memory window %s is not positive, updating failed!\n" % str(memory_window))
                self.passanitycheck = False
                return False
        except ValueError:
            self.logger.error("The memory window %s is not an integer, updating failed!\n" % str(memory_window))
            self.passanitycheck = False
            return False

    def update_execution_window(self, execution_window):
        """execution window is the duration for orders to be placed"""
        try:
            i_execution_window = float(execution_window)
            if i_execution_window > 0:
                self.execution_window = i_execution_window
            else:
                self.logger.error("The execution window %s is not positive, updating failed!\n" % str(execution_window))
                self.passanitycheck = False
                return False
        except ValueError:
            self.logger.error("The memory window %s is not a float, updating failed!\n" % str(execution_window))
            self.passanitycheck = False
            return False

    def update_internal_wait_time(self, internal_wait_time):
        """internal wait time is the wait duration before retrying """
        try:
            i_internal_wait_time = float(internal_wait_time)
            if i_internal_wait_time > 0:
                self.internal_wait_time = i_internal_wait_time
            else:
                self.logger.error("The internal wait time %s is not positive, updating failed!\n" % str(internal_wait_time))
                self.passanitycheck = False
                return False
        except ValueError:
            self.logger.error("The internal wait time %s is not a float, updating failed!\n" % str(internal_wait_time))
            self.passanitycheck = False
            return False

    def update_execution_mode(self, execution_mode):
        EXECUTION_MODES = ['SINGLE', 'MULTI', 'MULTIIOC', 'PATIENT', 'UNSAFE']
        if execution_mode in EXECUTION_MODES:
            self.execution_mode = execution_mode
        else:
            self.logger.error(f'Execution mode {execution_mode} not in {EXECUTION_MODES}, not updated')
            self.passanitycheck = False
            return False

    def update_explicit_wait_time(self, explicit_wait_time):
        """explicit wait time is the wait duration between orders """
        try:
            self.explicit_wait_time = float(explicit_wait_time)
        except ValueError:
            self.logger.error("The explicit wait time %s is not a float, updating failed!\n" % str(explicit_wait_time))
            self.passanitycheck = False
            return False

    def update_explicit_wait_random(self, explicit_wait_random):
        """explicit wait random is the wait time randomness between orders """
        try:
            self.explicit_wait_random = float(explicit_wait_random)
        except ValueError:
            self.logger.error("The explicit wait random %s is not a float, updating failed!\n" % str(explicit_wait_random))
            self.passanitycheck = False
            return False

    def update_spread_clearing_threshold_ratio(self, spread_clearing_threshold_ratio):
        """ Aggressive spread clearing is the act of clearing out spread attacks aggressively. See ABOTS-83 for details """
        """ This threshold is the ratio at which the first N layers of the orderbook can be considered as an attack"""
        f_spread_clearing_threshold = float(spread_clearing_threshold_ratio)
        try:
            if f_spread_clearing_threshold > 0.0 and f_spread_clearing_threshold < 1.0:
                self.aggressive_spread_clearing = True
                self.spread_clearing_threshold = f_spread_clearing_threshold
            else:
                self.aggressive_spread_clearing = False
                self.spread_clearing_threshold = -1.0
                self.logger.warning("The spread_clearing_threshold_ratio ratio %s is not between 0 and 1, updating failed!\n" % str(f_spread_clearing_threshold))
            self.logger.info(f'successfully performed update_spread_clearing_threshold_ratio with value {spread_clearing_threshold_ratio}')
        except ValueError:
            self.logger.error("Unable to set spread_clearing_threshold, whose value is %s" %str(spread_clearing_threshold_ratio))

    def update_spread_clearing_depth(self, spread_clearing_depth):
        """ Aggressive spread clearing is the act of clearing out spread attacks aggressively. See ABOTS-83 for details """
        """ Depth is the number of layers in the orderbook to check (i.e. the highest N bids or the lowest N asks) """
        try:
            if spread_clearing_depth > 0:
                self.spread_clearing_depth = int(float(spread_clearing_depth))
                self.aggressive_spread_clearing = True
            else:
                self.spread_clearing_depth = int(5)
                self.aggressive_spread_clearing = False
                self.logger.warning("The spread_clearing_depth of %s is not valid, spread clearing will be turned off!\n" % str(spread_clearing_depth))
            self.logger.info(f"successfully performed update_spread_clearing_depth with value {spread_clearing_depth}")
        except ValueError:
            self.logger.error("Unable to set spread_clearing_depth, whose value is %s" %str(spread_clearing_depth))

    def update_spread_clearing_max_vol_round(self, spread_clearing_max_vol_round):
        """ Aggressive spread clearing is the act of clearing out spread attacks aggressively. See ABOTS-83 for details """
        """ This is the maximum amount of trades that the bot can clear per round """
        try:
            if spread_clearing_max_vol_round > 0.0:
                self.spread_clearing_max_vol_round = float(spread_clearing_max_vol_round)
            else:
                self.spread_clearing_max_vol_round = 0.0
                self.aggressive_spread_clearing = False
                self.logger.warning("spread_clearing_max_vol_round is less than 0, setting to 0 and disabling clearing")

            self.logger.info(f"successfully performed update_spread_clearing_max_vol_round with value {spread_clearing_max_vol_round}")
        except ValueError:
            self.logger.error("Unable to set spread_clearing_max_vol_round, whose value is %s" %str(spread_clearing_max_vol_round))

    def update_spread_clearing_max_vol_cummulative(self, spread_clearing_max_vol_cummulative):
        """ Aggressive spread clearing is the act of clearing out spread attacks aggressively. See ABOTS-83 for details """
        """ This is the maximum amount of trades that the bot can clear cummulatively """
        try:
            if spread_clearing_max_vol_cummulative > 0.0:
                self.spread_clearing_max_vol_cummulative = float(spread_clearing_max_vol_cummulative)
            else:
                self.spread_clearing_max_vol_cummulative = 0.0

            self.logger.info(f"successfully performed update_spread_clearing_max_vol_cummulative with value {spread_clearing_max_vol_cummulative}")
        except ValueError:
            self.logger.error("Unable to set spread_clearing_depth, whose value is %s" %str(spread_clearing_max_vol_cummulative))

    def update_spread_clearing_min_order_size(self, spread_clearing_min_order_size):
        """ Aggressive spread clearing is the act of clearing out spread attacks aggressively. See ABOTS-83 for details """
        """ This is the maximum amount of trades that the bot can clear per round """
        try:
            if spread_clearing_min_order_size > 0.0:
                self.spread_clearing_min_order_size = float(spread_clearing_min_order_size)
            else:
                self.spread_clearing_min_order_size = 0.0
                self.aggressive_spread_clearing = False
                self.logger.error("spread_clearing_min_order_size is less than 0, setting to 0 and disabling spread clearing")

            self.logger.info(f"successfully performed update_spread_clearing_min_order_size with value {spread_clearing_min_order_size}")
        except ValueError:
            self.logger.error("Unable to set spread_clearing_min_order_size, whose value is %s" %str(spread_clearing_min_order_size))


    def update_spread_clearing_min_order_multiplier(self, spread_clearing_min_order_multiplier):
        """ Aggressive spread clearing is the act of clearing out spread attacks aggressively. See ABOTS-83 for details """
        """ This is the multiplier at which an order can be cleared (e.g. if multiplier = 10, all volumes with <= 10 * minimum order size will be cleared) """
        try:
            if spread_clearing_min_order_multiplier > 1.0:
                self.spread_clearing_min_order_multiplier = float(spread_clearing_min_order_multiplier)
            else:
                self.logger.warning("spread_clearing_min_order_multiplier is less than 1, setting to 1.")
                self.spread_clearing_min_order_multiplier = 1.0
            self.logger.info(f"successfully performed update_spread_clearing_min_order_multiplier with value {spread_clearing_min_order_multiplier}")
        except ValueError:
            self.logger.error("spread_clearing_min_order_multiplier of value %s is not a float!" %str(spread_clearing_min_order_multiplier))

    def update_max_altcoin_imbalance(self, amount):
        """update the max imbalance amount of altcoin"""
        try:
            famount = float(amount)
            if famount > 0.0:
                self.max_altcoin_imbalance = famount
            else:
                self.max_altcoin_imbalance = -1
                self.logger.warning("The max imbalance altcoin amount %s is not a positive number. Constraint will not be applied\n" % str(amount))
        except ValueError:
            self.max_altcoin_imbalance = -1
            self.logger.warning("The max imbalance altcoin amount %s is not float, updating failed! Setting to default value -1\n" % str(amount))

    def update_max_quotecoin_imbalance(self, amount):
        """update the max imbalance amount of quotecoin"""
        try:
            famount = float(amount)
            if famount > 0.0:
                self.max_quotecoin_imbalance = famount
            else:
                self.max_quotecoin_imbalance = -1
                self.logger.warning("The max imbalance quotecoin amount %s is not a positive number. Constraint will not be applied\n" % str(amount))
        except ValueError:
            self.max_quotecoin_imbalance = -1
            self.logger.warning("The max imbalance quotecoin amount %s is not float, updating failed! Setting to default value -1\n" % str(amount))

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

    def update_probe_mode(self, mode):
        if isinstance(mode, str):
            self.probe_mode = True if mode.upper().strip() == 'TRUE' else False
        else:
            self.probe_mode = False
            self.logger.warning("probe mode is not a boolean, defaulting to False")

    def update_conservative_mode(self, mode):
        if isinstance(mode, str):
            self.conservative_mode = True if mode.upper().strip() == 'TRUE' else False
        else:
            self.conservative_mode = False
            self.logger.warning("conservative mode is not a boolean, defaulting to False")

    def update_clear_hidden_orders_on_update(self, mode):
        if isinstance(mode, str):
            self.clear_hidden_orders_on_update = True if mode.upper().strip() == 'TRUE' else False
            if self.clear_hidden_orders_on_update:
                self.hidden_orders = {}
        else:
            self.clear_hidden_orders_on_update = False
            self.logger.warning("clear hidden orders on update is not a boolean, defaulting to False")

    def update_hidden_order_lifespan(self, lifespan):
        try:
            iamount = int(float(lifespan))
            if iamount > 0:
                self.hidden_order_lifespan = iamount
            else:
                self.hidden_order_lifespan = -1
                self.logger.warning("The hidden order lifespan %s is not a positive number. Setting to default value of -1\n" % str(lifespan))
        except ValueError:
            self.hidden_order_lifespan = -1
            self.logger.warning("The hidden order lifespan %s is not an integer, updating failed! Setting to default value of -1\n" % str(lifespan))

    def update_probe_altcoin_limit(self, amount):
        try:
            famount = float(amount)
            if famount > 0.0:
                self.probe_altcoin_limit = famount
            else:
                self.probe_altcoin_limit = -1
                self.logger.warning("The probe altcoin limit %s is not a positive number. Setting to default value -1\n" % str(amount))
        except ValueError:
            self.probe_altcoin_limit = -1
            self.logger.warning("The probe altcoin limit %s is not a float, updating failed! Setting to default value -1\n" % str(amount))

    def update_probe_quotecoin_limit(self, amount):
        try:
            famount = float(amount)
            if famount > 0.0:
                self.probe_quotecoin_limit = famount
            else:
                self.probe_quotecoin_limit = -1
                self.logger.warning("The probe quotecoin limit %s is not a positive number. Setting to default value -1\n" % str(amount))
        except ValueError:
            self.probe_quotecoin_limit = -1
            self.logger.warning("The probe quotecoin limit %s is not a float, updating failed! Setting to default value -1\n" % str(amount))


    def get_pending_orders(self):
        """get pending orders by account id & direction"""
        #{accountkey: {'buy': [orderid, orderid...], 'sell': [orderid, orderid...] }
        pendingorder = {}

        if not isinstance(self.brokers, list) or not len(self.brokers)>0:
            self.logger.warning("The broker list is empty, update pending orders failed!\n")
            return False

        for broker in self.brokers:
            if len(broker.buyorderlist) > 0:
                for pendingorderid in broker.buyorderlist:
                    pendingorder[broker.get_account_id()]['buy'].append(pendingorderid)

            if len(broker.sellorderlist) > 0:
                for pendingorderid in broker.sellorderlist:
                    pendingorder[broker.get_account_id()]['sell'].append(pendingorderid)

        return pendingorder


    def get_quotecoin_tradable_balance(self):
        """get quote coin tradable balance"""
        account = {}

        if not isinstance(self.brokers, list) or not len(self.brokers)>0:
            self.logger.error("The broker list is empty, update quote coin tradable balance failed!\n")
            return False

        for broker in self.brokers:
            account[str(broker.get_account_id())] = broker.get_coin_tradable_balance(self.quotecoin)

        return account

        # if self.brokers[0]:
        #     return self.brokers[0].get_coin_tradable_balance(self.quotecoin)
        # else:
        #     # logging
        #     return False


    def get_quotecoin_frozen_balance(self):
        """get quote coin frozen balance"""
        account = {}

        if not isinstance(self.brokers, list) or not len(self.brokers)>0:
            self.logger.error("The broker list is empty, update quote coin frozen balance failed!\n")
            return False

        for broker in self.brokers:
            account[str(broker.get_account_id())] = broker.get_coin_frozen_balance(self.quotecoin)

        return account

        # if self.brokers[0]:
        #     return self.brokers[0].get_coin_frozen_balance(self.quotecoin)
        # else:
        #     # logging
        #     return False


    def get_altcoin_tradable_balance(self):
        """get altcoin tradable balance"""
        account = {}

        if not isinstance(self.brokers, list) or not len(self.brokers)>0:
            self.logger.error("The broker list is empty, update altcoin tradable balance failed!\n")
            return False

        for broker in self.brokers:
            account[str(broker.get_account_id())] = broker.get_coin_tradable_balance(self.altcoin)

        return account

        # if self.brokers[0]:
        #     return self.brokers[0].get_coin_tradable_balance(self.altcoin)
        # else:
        #     # logging
        #     return False


    def get_altcoin_frozen_balance(self):
        """get altcoin frozen balance"""
        account = {}

        if not isinstance(self.brokers, list) or not len(self.brokers)>0:
            self.logger.error("The broker list is empty, update altcoin frozen balance failed!\n")
            return False

        for broker in self.brokers:
            account[str(broker.get_account_id())] = broker.get_coin_frozen_balance(self.altcoin)

        return account

        # if self.brokers[0]:
        #     return self.brokers[0].get_coin_frozen_balance(self.altcoin)
        # else:
        #     # logging
        #     return False


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


    def get_bidask_spread(self):
        """ """
        # TODO replace self.broker[0] with a method argument
        ask1 = {}
        bid1 = {}
        ask1 = self.brokers[0].get_lowest_ask_and_volume(self.tradingpair)
        bid1 = self.brokers[0].get_highest_bid_and_volume(self.tradingpair)

        if isinstance(ask1, dict) and isinstance(bid1, dict):

            if (ask1['price'] - bid1['price']) > 0.0:
                self.bidaskspread = round((ask1['price'] - bid1['price']), self.orderpricerounding)
            else:
                return False

        return self.bidaskspread


    def get_tradingpair_name(self):
        """ """
        return self.tradingpair


    def get_progress(self):
        """ """
        return self.position['progress']


    def get_washing_status(self):
        """ """
        return self.position


    def get_average_ordersize(self):
        """ """
        return self.taskordersize


    def get_current_priority(self):
        """ """
        return self.orderpriority


    def get_original_priority(self):
        """ """
        return self.originalpriority


    def get_dailymarket(self):
        """
        :get the past 24 hours market brief
        :+20%, return 'BULL'
        :-20%, return 'BEAR'
        :else, return 'FLAT'
        """
        pass


    def get_assets(self):
        """ """
        pass
        # """return total assets held across all brokers"""
        # #
        # assets = {}

        # for broker in self.brokers:
        #     accountbalance = broker.get_account_balance()

        #     try: 
        #         for coin, balance in accountbalance.items():

        #             if coin in assets:
        #                 assets[coin] += balance
        #             elif balance > 0.0:
        #                 assets[coin] = balance
        #     except:
        #         continue

        # return assets



    def read_order_from_csv(self, filename=None):
        """retrieve order results in a given time frame(24h) from a saved csv file"""
        # TODO replace 24*60*60 with a method param 
        orderdictlist = []
        currents = time.time()

        try:
            if not filename:
                filename = self.altcoin.upper() + '_' + self.quotecoin.upper() + '_' + self.brokers[0].get_exchangename() +'_Orders.csv'

            with open(filename, 'r') as csvfile:
                csvreader = csv.DictReader(csvfile)
                # for order in csvreader:
                #     if (currents - float(order['TimeStamp'])/1000) <= 24*60*60:
                #         orderdictlist.append(order)
                orderdictlist = [order for order in csvreader if (currents - float(order['TimeStamp'])/1000) <= 24*60*60]
        except IOError:
            self.logger.warning("The order list csv file %s doesn't exist" % filename)
            return False
        else:
            return orderdictlist


    def remove_orders_over24h(self, filename=None):
        """remove the orders booked 24h ago from csv file"""
        orderdictlist = []
        currents = time.time()

        if not filename:
            filename = self.altcoin.upper() + '_' + self.quotecoin.upper() + '_' + self.brokers[0].get_exchangename() +'_Orders.csv'

        try:
            with open(filename, 'r') as csvfile:
                csvreader = csv.DictReader(csvfile)
                # for order in csvreader:
                #     orderdictlist.append(order)
                orderdictlist=[order for order in csvreader]
        except IOError:
            self.logger.warning("The order list csv file %s doesn't exist" % filename)
            return False

        if isinstance(orderdictlist, list) and len(orderdictlist) > 0:
            for order in list(orderdictlist):
                try:
                    if (currents - float(order['TimeStamp'])/1000) > 24*60*60:
                        orderdictlist.remove(order)
                    else:
                        break
                except Exception as e:
                    pass #self.logger.warning(f"{e}")


            with open(filename, 'w') as csvfile2:
                headers = ['OrderID', 'TimeStamp', 'TradingPair', 'Exchange', 'AccountKey', 'LongShort', 'Amount', 'FilledAmount', 'Price', 'Status','FilledCashAmount', 'BuyCommission', 'NetFilledCashAmount', 'SellCommission', 'NetFilledAmount']
                csvwriter = csv.DictWriter(csvfile2, delimiter=',', lineterminator='\n', fieldnames=headers)
                csvwriter.writeheader()
                csvwriter.writerows(orderdictlist)


    def generate_trader_position(self):
        """generate current rolling position of past 24h (only for the current bot)"""
        # table name: orderid, timestamp(execution time), tradingpair, exchange, accountkey, type(buy/sell), amount, filled amount, price
        # keys = ['OrderID', 'TimeStamp', 'TradingPair', 'Exchange', 'AccountKey', 'LongShort', 'Amount', 'FilledAmount', 'Price']     
        orderdictlist = []
        self.position['position_clearing_enabled'] = self._position_clearing_enabled

        #Database version
        if self.clerk and config.RECORD_TO_DB:
            endtime = datetime.datetime.now(datetime.timezone.utc)
            begintime = self.start_time_utc
            exchange = self.brokers[0].get_exchangename()   # assuming all exchanges are the same
            account = self.brokers[0].get_account_id() if len(self.brokers) == 1 else [broker.get_account_id() for broker in self.brokers]

            aggregate = self.clerk.get_historical_execution_aggregates_sqla(self.tradingpair, exchange, account, begintime, endtime, 'Volume', self.botid)
            if aggregate is not False:
                for order in aggregate:
                    orderdictlist.append(order)

            if isinstance(orderdictlist, list) and len(orderdictlist) > 0:
                self.position['timestamp'] = (time.time())*1000
                self.position['long'] = sum(float(order.FilledAmount) for order in orderdictlist if order.LongShort == 'Long')
                self.position['short'] = -sum(float(order.FilledAmount) for order in orderdictlist if order.LongShort == 'Short')
                self.position['net'] = float(self.position['long'] + self.position['short'])
                self.position['frozenaltcoin'] = sum((float(order.Amount) - float(order.FilledAmount)) for order in orderdictlist if order.LongShort == 'Short')
                self.position['frozenquotecoin'] = sum((float(order.CashAmount) - float(order.FilledCashAmount)) for order in orderdictlist if order.LongShort == 'Long')
                self.position['completed'] = max(abs(self.position['long']), abs(self.position['short']))
                self.position['filledcashamount'] = sum((float(order.FilledCashAmount) if  order.LongShort == 'Long' else -float(order.FilledCashAmount)) for order in orderdictlist)
                try:
                    self.position['progress'] = float(self.position['completed']) / float(self.taskquantity)
                except ZeroDivisionError:
                    self.logger.error(f"failed to calculate progress using inputs {self.position['completed']}/{self.taskquantity}, setting to 0.0")
                    self.position['progress'] = 0.0
                self.position['benchmark'] = self.taskquantity * (endtime - begintime).total_seconds() / 86400
                try:
                    if self.position['net'] > 0 and self.max_altcoin_imbalance > 0:
                        self.position['altcoin_imbalance'] = f"{(self.position['net'] / self.max_altcoin_imbalance):0.8f}"
                    else:
                        try:
                            del(self.position['altcoin_imbalance'])
                        except KeyError:
                            pass
                    askprice = self.brokers[0].get_lowest_ask_and_volume(self.tradingpair)
                    if self.position['net'] < 0 and self.max_quotecoin_imbalance > 0 and isinstance(askprice, dict) and askprice.get('price', None) is not None:
                        self.position['quotecoin_imbalance'] = f"{(-self.position['net']*askprice['price'] / self.max_quotecoin_imbalance):0.8f}"
                    else:
                        try:
                            del(self.position['quotecoin_imbalance'])
                        except KeyError:
                            pass
                except Exception as e:
                    self.logger.error(f"failed to calculate imbalances")

                def calc_fifo_cost_profit(orders):
                    self.logger.debug('calculating fifo cost & profit')
                    #self.logger.debug(f'calculating fifo from orders {[SimpleNamespace(**o.__dict__) for o in orders]}')

                    # use SimpleNamespace as a dirty copy that won't affect the original
                    orders = deque(SimpleNamespace(FilledAmount=o.FilledAmount, Price=o.Price, LongShort=o.LongShort) for o in orders)
                    net_orders = []
                    # remove canceling orders
                    while orders:
                        order = orders.popleft()
                        if (
                            len(orders) > 0
                            and order.FilledAmount == orders[0].FilledAmount 
                            and order.Price == orders[0].Price 
                            and order.LongShort != orders[0].LongShort
                        ):
                            orders.popleft()
                        else:
                            net_orders.append(order)
                        
                    buys = [o for o in net_orders if o.LongShort == 'Long' and o.FilledAmount > 0]
                    sells = [o for o in net_orders if o.LongShort == 'Short' and o.FilledAmount > 0]
                    inventory = deque(buys)
                    inventory_negative = []
                    profit = 0
                    for sell in sells:
                        while sell.FilledAmount > 0 and len(inventory) > 0:
                            first_buy = inventory[0]
                            profit += min(first_buy.FilledAmount, sell.FilledAmount) * (
                                sell.Price - first_buy.Price
                            )
                            first_buy.FilledAmount -= sell.FilledAmount
                            if first_buy.FilledAmount <= 0:
                                sell.FilledAmount = -first_buy.FilledAmount
                                inventory.popleft()
                            else:
                                break
                        if len(inventory) == 0 and sell.FilledAmount > 0:
                            inventory_negative.append(sell)

                    if inventory_negative:
                        inventory = inventory_negative

                    amount_invetory = sum(order.FilledAmount for order in inventory if order.FilledAmount > 0)

                    if len(inventory) > 0:
                        cost = sum(
                            order.Price * order.FilledAmount 
                            for order in inventory if order.Price > 0 and order.FilledAmount > 0
                        ) / amount_invetory
                    else:
                        cost = 0
                    
                    self.logger.debug(
                        f'fifo calculation shows net inventory at {amount_invetory}'
                    )
                    self.position['fifo_inventory'] = f'{amount_invetory}: {inventory}'

                    return cost, profit
                try:
                    orders = self.clerk.all_orders()
                    self.position['fifo_price'], self.position['realized_profit'] = calc_fifo_cost_profit(orders)
                    self.logger.debug(f'calculated fifo cost {self.position["fifo_price"]} and realized profit {self.position["realized_profit"]}')
                except:
                    self.logger.error(traceback.format_exc())
                    self.logger.error('failed to calculate fifo cost & profit')
                    self.position['realized_profit'], self.position['fifo_price'] = 0, 0
            else:
                self.position['timestamp'] = 0.0
                self.position['long'] = 0.0
                self.position['short'] = 0.0
                self.position['net'] = 0.0
                self.position['frozenaltcoin'] = 0.0
                self.position['frozenquotecoin'] = 0.0
                self.position['completed'] = 0.0
                self.position['progress'] = 0.0
                self.position['benchmark'] = 0.0
                self.position['filledcashamount'] = 0.0
            return


        filename = self.altcoin.upper() + '_' + self.quotecoin.upper() + '_' + self.brokers[0].get_exchangename() +'_Orders.csv'
        if os.path.isfile(filename):
            orderdictlist = self.read_order_from_csv(filename)

        if isinstance(orderdictlist, list) and len(orderdictlist) > 0:
            self.position['timestamp'] = (time.time())*1000
            self.position['long'] = sum(float(order['FilledAmount']) for order in orderdictlist if order['LongShort'] == 'Long')
            self.position['short'] = -sum(float(order['FilledAmount']) for order in orderdictlist if order['LongShort'] == 'Short')
            self.position['net'] = float(self.position['long'] + self.position['short'])
            self.position['frozenaltcoin'] = sum((float(order['Amount']) - float(order['FilledAmount'])) for order in orderdictlist if order['LongShort'] == 'Short')
            self.position['frozenquotecoin'] = sum((float(order['Amount']) - float(order['FilledAmount']))*float(order['Price']) for order in orderdictlist if order['LongShort'] == 'Long')
            self.position['completed'] = max(abs(self.position['long']), abs(self.position['short']))
            try:
                self.position['progress'] = float(self.position['completed']) / float(self.taskquantity)
            except ZeroDivisionError:
                self.position['progress'] = 0.0


    def get_24hour_rolling_position(self):
        """return trader's position in last 24h"""
        self.generate_trader_position()
        return self.position

    
    def log_order(self, order_id, side, broker):
        if not order_id:
            return
        time.sleep(self.orderwaitingtime/8)
        try:
            if config.RECORD_TO_DB:
                self.record_order_to_db(
                    broker,
                    self.tradingpair,
                    'SELL_LIMIT' if side == SELL else 'BUY_LIMIT',
                    order_id,
                )
            if config.RECORD_TO_CSV:
                self.record_order_to_csv(
                    broker,
                    self.tradingpair,
                    'SELL_LIMIT' if side == SELL else 'BUY_LIMIT',
                    order_id,
                )
        except:
            self.logger.error(traceback.format_exc())

    @property
    def position_hold_time(self):
        return self._position_hold_time

    @position_hold_time.setter
    def position_hold_time(self, duration):
        try:
            duration = float(duration)
            if duration < 0:
                raise TypeError
            elif duration == 0:
                self._position_hold_time = None
            else:
                self._position_hold_time = duration
        except (ValueError, TypeError):
            self.logger.error(f'invalid position hold time: {duration}, default to None')
            self._position_hold_time = None

    def clear_net_position(self): 
        if not self._position_clearing_enabled:
            self.logger.error(f'position clearing disabled due to a failed position clear')
            return 
        time.sleep(self.orderwaitingtime / 8)
        pair = self.tradingpair
        net_position = self.position.get('net', 0)
        fifo_price = self.position.get('fifo_price', 0.0)
        fifo_price = round(fifo_price, self.orderpricerounding)
        if net_position == 0:
            self.last_position_improvement = time.time()
            return
        self.logger.debug(f'clearing net position of {net_position} at {fifo_price}')
        side = BUY if net_position < 0 else SELL

        def pick_brokers():
            if len(self.brokers) == 1:
                return self.brokers[0], self.brokers[0]
            elif len(self.brokers) == 2:
                buy_broker, sell_broker = random.sample(self.brokers, 2)
                if not self.safety_check(
                    fifo_price, net_position, buy_broker, 'BUY'
                ) or not self.safety_check(
                    fifo_price, net_position, sell_broker, 'SELL'
                ):
                    buy_broker, sell_broker = sell_broker, buy_broker
                return buy_broker, sell_broker
            else:
                return self.pick_2_brokers()

        buy_broker, sell_broker = pick_brokers()

        self.update_order_book_meaningfully()
        broker = self.brokers[0]
        ob = broker.orderbook

        position_clear_price = next(
            (
                price_level.price
                for price_level in (ob.asks if side == BUY else ob.bids)
                if price_level.cumulative > abs(net_position)
            ),
            0,
        )

        def calc_desperation():
            """ how desperate the bot is to clear the position 

            :returns: a number between 0 and 1: 0 being not desperate at all, and
                1 being "clear the position immediately"
            """
            if not self.position_hold_time:
                return 0
            try:
                self.last_net_position
            except AttributeError:
                self.last_net_position = 0
            try:
                self.last_position_improvement
            except AttributeError:
                self.last_position_improvement = time.time()

            if abs(net_position) < abs(self.last_net_position):
                self.logger.debug(
                    f'net position has improved to {net_position} from {self.last_net_position}'
                )
                ratio = max(0, net_position / self.last_net_position)
                self.last_position_improvement = (
                    self.last_position_improvement * ratio
                ) + (time.time() * (1 - ratio))
            self.last_net_position = net_position

            time_multiplier = min(
                1.0,
                (time.time() - self.last_position_improvement)
                / self.position_hold_time,
            )

            amount_multiplier = 2 * max(
                abs(net_position) / self.max_altcoin_imbalance,
                abs(net_position) * position_clear_price / self.max_quotecoin_imbalance,
            )
            self.logger.debug(
                f'multipliers set at time/amount {time_multiplier}/{amount_multiplier}'
            )

            return min(1, max(time_multiplier * amount_multiplier, 0))

        desperation = calc_desperation()
        self.logger.debug(f'desperation set to {desperation}')
        if position_clear_price > 0:
            acceptable_loss = round(
                abs(fifo_price - position_clear_price) * desperation,
                self.orderpricerounding,
            )
        else:
            acceptable_loss = 0

        self.logger.debug(f'acceptable loss set to {acceptable_loss}')

        if side == BUY and ob.asks[0].price <= fifo_price + acceptable_loss:
            amount = min(abs(net_position), ob.asks[0].amount)

            send_order = buy_broker.client.buy_wait
            cancel_order = buy_broker.client.cancel_wait
            price = fifo_price + acceptable_loss
        elif side == SELL and ob.bids[0].price >= fifo_price - acceptable_loss:
            amount = min(abs(net_position), ob.bids[0].amount)
            send_order = sell_broker.client.sell_wait
            cancel_order = sell_broker.client.cancel_wait
            price = fifo_price - acceptable_loss
        else:
            self.logger.debug(
                f"not sending net order at current l1 prices "
                f"{ob.asks[0].price if side == BUY else ob.bids[0].price} "
                f"(required {fifo_price + acceptable_loss if side == BUY else fifo_price - acceptable_loss})"
            )
            return

        if not self.meets_minimum_size_and_notional_thresholds(price, amount):
            self.logger.debug('net position below min order size, not clearing')
            return
        self.logger.debug(f'now sending net order {side} {pair} {price} {amount}')
        net_order_id = send_order(pair, price, amount)[0]

        if not net_order_id and not self._position_clearing_enabled == 'FORCED':
            self.logger.error(f'net order lost, disabling position clearing')
            self._position_clearing_enabled = False
            return

        order = None
        for i in range(2, 10):
            order = broker.client.get_order_details(
                pair, 'SELL_LIMIT' if side == SELL else 'BUY_LIMIT', net_order_id
            )
            if not order or order.state == OrderState.SENDING:
                self.logger.debug(f'waiting for net order {net_order_id} to update')
                continue
            if order.completed or order.canceled:
                self.logger.debug('net order {net_order_id} finished')
                break
            else:
                cancel_order(net_order_id)
            time.sleep(i)

        else:
            if not self._position_clearing_enabled == 'FORCED':
                self.logger.error(f'net order {net_order_id} in invalid state {order}, disabling position clearing')
                self._position_clearing_enabled = False
                
        self.log_order(net_order_id, side, broker)


    def load_pending_orders(self, filename=None):
        """load last saved pending orders from csv file"""
        currents = time.time()
        orderdictlist = []
        try:
            if not filename:
                filename = self.altcoin.upper() + '_' + self.quotecoin.upper() + '_' + self.brokers[0].get_exchangename() +'_Pending_Orders.csv'

            with open(filename, 'r') as csvfile:
                csvreader = csv.DictReader(csvfile)
                # orderdictlist = list(csvreader)
                # for pendingorder in csvreader:
                #     if (currents - float(pendingorder['TimeStamp'])/1000) <= 24*60*60:
                #         orderdictlist.append(pendingorder)
                try:
                    orderdictlist = [pendingorder for pendingorder in csvreader if (currents - float(pendingorder['TimeStamp'])/1000) <= 24*60*60]
                except Exception as e:
                    self.logger.warning(f"{e}")

            if isinstance(orderdictlist, list) and len(orderdictlist):
                for pendingorderdict in orderdictlist:
                    for broker in self.brokers:
                        if pendingorderdict['AccountKey'] == str(broker.get_account_id()):
                            # use eval() function, convert string to list
                            broker.buyorderlist = eval(pendingorderdict['BuyOrder'])
                            broker.sellorderlist = eval(pendingorderdict['SellOrder'])
            else:
                # TODO logging
                return False

        except IOError:
            self.logger.warning("The pending order list csv file %s doesn't exist" % filename)
            return False


    def record_pending_orders(self):
        """record the pending orders into a csv file"""
        # {'TimeStamp':, 'Exchange':'', 'Accountkey':'', 'BuyOrder': [], 'SellOrder': []}
        pendingorderdict = {}
        dictlist = []

        for broker in self.brokers:
            pendingorderdict['TimeStamp'] = (time.time())*1000
            pendingorderdict['Exchange'] = broker.get_exchangename()
            pendingorderdict['AccountKey'] = broker.get_account_id()
            pendingorderdict['BuyOrder'] = broker.buyorderlist
            pendingorderdict['SellOrder'] = broker.sellorderlist

            # pendingorderdict = {
            #     'TimeStamp': (time.time())*1000,
            #     'Exchange': broker.get_exchangename(),
            #     'AccountKey': broker.get_account_id(),
            #     'BuyOrder': broker.buyorderlist,
            #     'SellOrder': broker.sellorderlist
            # }
            dictlist.append(pendingorderdict)
            pendingorderdict = {}

        filename = self.altcoin.upper() + '_' + self.quotecoin.upper() + '_' + self.brokers[0].get_exchangename() +'_Pending_Orders.csv'
        if not os.path.isfile(filename):
            with open(filename, 'w') as csvfile:
                headers = ['TimeStamp', 'Exchange', 'AccountKey', 'BuyOrder', 'SellOrder']
                csvwriter = csv.DictWriter(csvfile, delimiter=',', lineterminator='\n', fieldnames=headers)
                csvwriter.writeheader()
                csvwriter.writerows(dictlist)
        else:
            with open(filename, 'w') as csvfile: # 'w' not 'a' - always overwrite the previous position
                headers = ['TimeStamp', 'Exchange', 'AccountKey', 'BuyOrder', 'SellOrder']
                csvwriter = csv.DictWriter(csvfile, delimiter=',', lineterminator='\n', fieldnames=headers)
                csvwriter.writeheader()
                csvwriter.writerows(dictlist)


    def load_trader_position(self, filename=None):
        """load trader's position from last saved position record"""
        currents = time.time()
        try:
            if not filename:
                filename = self.altcoin.upper() + '_' + self.quotecoin.upper() + '_' + self.brokers[0].get_exchangename() +'_Position.csv'

            with open(filename, 'r') as csvfile:
                csvreader = csv.DictReader(csvfile)
                for position in csvreader:
                    if (currents - float(position['timestamp'])/1000) <= 24*60*60:
                        self.position = position
                # self.position = [position for position in csvreader if (currents - float(position['timestamp'])/1000) <= 24*60*60]
            # empty dictionary evaluate to False in Python
            # if self.position:
            #     return True
            # else: 
            #     return False
        except IOError:
            self.logger.warning("The position csv file %s doesn't exist" % filename)
            return False
        else:
            return self.position


    def record_trader_position(self):
        """record trader's position to a csv file"""
        filename = self.altcoin.upper() + '_' + self.quotecoin.upper() + '_' + self.brokers[0].get_exchangename() +'_Position.csv'
        if not os.path.isfile(filename):
            with open(filename, 'w') as csvfile:
                headers = ['timestamp', 'long', 'short', 'net', 'frozenaltcoin', 'frozenquotecoin', 'completed', 'progress', 'benchmark', 'filledcashamount']
                csvwriter = csv.DictWriter(csvfile, delimiter=',', lineterminator='\n', fieldnames=headers)
                csvwriter.writeheader()
                csvwriter.writerow(self.position)
        else:
            with open(filename, 'w') as csvfile: # 'w' not 'a' - always overwrite the previous position
                headers = ['timestamp', 'long', 'short', 'net', 'frozenaltcoin', 'frozenquotecoin', 'completed', 'progress', 'benchmark', 'filledcashamount']
                csvwriter = csv.DictWriter(csvfile, delimiter=',', lineterminator='\n', fieldnames=headers)
                csvwriter.writeheader()
                csvwriter.writerow(self.position)


    def exit_processing(self):
        """
        :处理无论何种原因退出的情况。保存刷量的关键信息，such as timestamp, position, progress, pending order....
        """
        # TODO save position, pending buy/sell order with saved time stamp; check pending orders one more time
        self.logger.info(f"commencing shutdown procedures for bot #{self.botid}")
        try:
            self.remove_orders_over24h()
        except:
            self.logger.warning(f"unable to remove orders from csv file")
        try:
            self.pending_order_processing()
        except:
            self.logger.warning(f"unable to update pending orders")
        try:
            self.record_pending_orders()
        except:
            self.logger.warning(f"unable to record pending orders")
        try:
            self.generate_trader_position()
        except:
            self.logger.warning(f"unable to generate trader position")
        try:
            self.record_trader_position()
        except:
            self.logger.warning(f"unable to record trader position")

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
        self.logger.info(f"shutdown completed for bot #{self.botid}")

    def get_config_data(self):
        """extract configuration details"""
        config = {
            'orderPriority': self.originalpriority,
            'minTimeBreak': self.minwaitingtime,
            'maxTimeBreak': self.maxwaitingtime,
            'dailyQuantity': self.taskquantity,
            'portionOrder': self.fixedorderperc,
            'spreadShock': self.spreadshock,
            'benchMarkPair': self.benchmarkpair,
            'riskThreshold': self.riskthreshold,
            'minimumSpread': self.minimumspread,
            'reservedAltcoin': self.reservedaltcoin,
            'reservedQuotecoin': self.reservedquotecoin,
            'maxAltcoinImbalance': self.max_altcoin_imbalance,
            'maxQuotecoinImbalance': self.max_quotecoin_imbalance,
            'maxTimeDelay': self.maxTimeDelay,
            'quoteRatio': self.quoteincrement,
            'catchUpTimes': self.catchuptimes,
            'maxUpdateFrequency': self.max_upd_frequency,
            'memoryWindow': self.memory_window,
            'executionWindow': self.execution_window,
            'internalWaitTime': self.internal_wait_time,
            'executionMode': self.execution_mode,
            'explicitWaitTime': self.explicit_wait_time,
            'explicitWaitRandom': self.explicit_wait_random,
            'spreadClearingThresholdRatio': self.spread_clearing_threshold,
            'spreadClearingDepth': self.spread_clearing_depth,
            'spreadClearingMinOrderMultiplier': self.spread_clearing_min_order_multiplier,
            'spreadClearingMaxVolRound': self.spread_clearing_max_vol_round,
            'spreadClearingMaxVolCummulative': self.spread_clearing_max_vol_cummulative,
            'spreadClearingMinOrderSize': self.spread_clearing_min_order_size,
            'percentageRandom': self.percentagerand,
            'minimumOrderSize': self.minimum_order_size,
            'minimumOrderNotional': self.minimum_order_notional,
            'probeMode': self.probe_mode,
            'hiddenOrderLifespan': self.hidden_order_lifespan,
            'probeAltcoinLimit': self.probe_altcoin_limit,
            'probeQuotecoinLimit': self.probe_quotecoin_limit,
            'clearHiddenOrdersOnUpdate': self.clear_hidden_orders_on_update,
            'conservativeMode': self.conservative_mode
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
        """
        :完整性检测用来确保所有刷量参数设置正确。收到开始刷量的命令后，首先运行这个检测。
        """
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
        """calculate the order size based on daily task quantity and waiting time"""
        benchmarkcheck = True
        if float(self.position["completed"]) >= 0.95 * float(self.position["benchmark"]):
            benchmarkcheck = True
        else:
            benchmarkcheck = False

        averagewaiting = (self.maxwaitingtime + self.minwaitingtime)/2
        if self.taskquantity > 0 and self.maxwaitingtime >= self.minwaitingtime > 0 and benchmarkcheck == True:
            self.taskordersize = self.taskquantity / (24*60*60/averagewaiting)
        elif self.taskquantity > 0 and self.maxwaitingtime >= self.minwaitingtime > 0 and benchmarkcheck == False:
            self.taskordersize = self.catchuptimes * self.taskquantity / (24*60*60/averagewaiting)
            self.logger.info("Volume catchup size is %s" % str(self.taskordersize))
        else:
            self.logger.error("Total task quantity %s or max waiting time %s or min waiting time %s \
            should be positive.\n" % (str(self.taskquantity), str(self.maxwaitingtime), str(self.minwaitingtime)))
            return False


    def calculate_order_scalefactor(self, benchmark):
        """calculate a trading volume scale factor based on recent 290 5-minute trading volume of benchmark pair"""
        # list of 5-minute trading volume with 290 records 
        self.amounthistory = self.brokers[0].get_amount_history(benchmark)
        # the list shall have at least two record to start
        if isinstance(self.amounthistory, list) and len(self.amounthistory) > 2:
            # use record[1], not record[0], becasue it is the current uncompleted volume bar 
            averageamountin24hour = sum(self.amounthistory[2:])/len(self.amounthistory[2:])
            self.scalefactor = self.amounthistory[1]/averageamountin24hour
            if self.scalefactor > 20:
                self.scalefactor = 20
        else:
            self.logger.warning("Volume scale factor will be 1.0 becasue of missing volume history of benchmark pair.\n")
            self.scalefactor = 1.0


    def generate_order_volume(self, tradingpair):
        """generate the order size based on scale factor and average order size"""
        ordervolume = None
        # order size equals fixed portion plus floating portion with scale factor * +/-% random (see percentagerand)
        # adding 20% random because the scale factor updates every 5 minutes
        ordervolume = round(float(self.fixedorderperc * self.taskordersize +
        (1 - self.fixedorderperc) * self.taskordersize * self.scalefactor * random.uniform(max(1-self.percentagerand, 0), min(1+self.percentagerand, 2))), self.ordersizerounding)

        if ordervolume:
            self.logger.info("The order volume(size) is %s." % str(ordervolume))
            return ordervolume
        else:
            self.logger.warning("The order volume(size) was not calculated correctly, default to average order size %s.\n" % str(round(self.taskordersize, 8)))
            return round(self.taskordersize, 8)


    def generate_order_price(self, tradingpair, inclination=None):
        """generate the order price shared by both buy and sell orders"""
        lowestask = {}
        highestbid = {}
        midprice = 0.0
        randomfactor = 0.0
        orderprice = 0.0

        lowestask = self.brokers[0].get_lowest_ask_and_volume(tradingpair)
        highestbid = self.brokers[0].get_highest_bid_and_volume(tradingpair)

        if isinstance(lowestask, dict) and isinstance(highestbid, dict) and lowestask['price'] > highestbid['price'] > 0:
            # ABOTS-100: adjust bid1, ask1 with hidden orders
            if self.probe_mode:
                self.logger.debug(f"probe mode enabled :: overlaying hidden orders - bid1, ask1 before = {highestbid['price']}, {lowestask['price']}")
                self.logger.debug(f"probe mode enabled :: hidden orders = {self.hidden_orders}")
                highestbid, lowestask, self.hidden_orders = self.overlay_hidden_orders(highestbid['price'], lowestask['price'], self.hidden_orders)
                self.logger.debug(f"probe mode enabled :: overlaying hidden orders - bid1, ask1 after = {highestbid['price']}, {lowestask['price']}")

            self.bidaskspread = round(float(lowestask['price'] - highestbid['price']), self.orderpricerounding)
            midprice = round(float((lowestask['price'] + highestbid['price'])/2), self.orderpricerounding)

            if inclination:
                self.logger.debug(f"price generation mode is '{inclination}'")

            if inclination == "PERCENTAGE_BELOW_ASK1":
                # puts a price slightly below the ask1
                taskprice = round(float(lowestask['price'] - (lowestask['price'] - highestbid['price'])*self.quoteincrement), self.orderpricerounding)
            elif inclination == "PERCENTAGE_ABOVE_BID1":
                # puts a price slightly above the bid1
                taskprice = round(float(highestbid['price']+ (lowestask['price'] - highestbid['price'])*self.quoteincrement), self.orderpricerounding)
            else:
                taskprice = midprice
        else:
            # self.logger.error("The bid1 price %f or ask1 price %f of orderbook of %s is not meaningful!" 
            # % (lowestask['price'], highestbid['price'], tradingpair))
            self.logger.error("The bid1 price or ask1 price of orderbook of %s is not meaningful!" % tradingpair)
            return False

        if self.execution_mode not in ['PATIENT', 'UNSAFE'] and self.bidaskspread - self.minimumspread < 0:
            self.logger.info("The bid-ask spread %s is below the minimum spread of %s." % (str(self.bidaskspread), str(self.minimumspread)))
            time.sleep(self.internal_wait_time)
            return False
        elif self.execution_mode in ['PATIENT', 'UNSAFE']:
            safe_ask, _, safe_bid = self.calc_safe_prices(self.brokers[0].orderbook)
            if safe_ask - safe_bid < min(self.minimumticksize * 10, self.minimumspread):
                time.sleep(self.internal_wait_time)
                self.logger.info(f'The safe bid-ask spread {safe_ask - safe_bid} is below the minimum spread of {self.minimumticksize * 10}')
                return False
        else:
            if self.bidaskspread < 2 * self.minimumticksize: # randomly use bid1 or ask1 when spread is less than 2 tick
                randomfactor = 0.0
                orderprice = random.choice([lowestask['price'], highestbid['price']])
            elif inclination == '1_TICK_BELOW_ASK1':
                # conservative mode operation
                orderprice = lowestask['price'] - self.minimumticksize
                self.logger.debug(f"conservative mode :: setting price to {self.minimumticksize} below {lowestask['price']}, {orderprice}")
            elif inclination == '1_TICK_ABOVE_BID1':
                # conservative mode operation
                orderprice = highestbid['price'] + self.minimumticksize
                self.logger.debug(f"conservative mode :: setting price to {self.minimumticksize} above {highestbid['price']}, {orderprice}")
            elif self.bidaskspread < 4 * self.minimumticksize: # bid ask spread is only 4 tick
                randomfactor = 0.0
                orderprice = taskprice + round(float((self.bidaskspread/2) * random.uniform(-randomfactor, randomfactor)), self.orderpricerounding)
            else:
                randomfactor = self.spreadshock # random shock between -50% ~ 50%
                orderprice = taskprice + round(float((self.bidaskspread/2) * random.uniform(-randomfactor, randomfactor)), self.orderpricerounding)

        # add another layer of protection
        if highestbid['price'] < orderprice < lowestask['price']:
            self.logger.debug("The bid1 price is %s, and the order price is %s, and the ask1 price is %s."
                % (str(highestbid['price']), str(orderprice), str(lowestask['price'])))
        else:
            orderprice = taskprice

        # ABOTS-100: perform hidden order probing - this will be less efficiency but easier to manage
        if self.probe_mode and (self.probe_altcoin_limit < 0 or self.probe_altcoin_spent < self.probe_altcoin_limit) and (self.probe_quotecoin_limit < 0 or self.probe_quotecoin_spent < self.probe_quotecoin_limit):
            hidden_order_detection_results = self.perform_hidden_order_probe(orderprice, self.orderpriority)
            for direction, side, other_side, other_direction in [("BUY", "bids", "asks", "SELL"), ("SELL", "asks", "bids", "BUY")]:
                price_detected, altcoin_dealt, quotecoin_dealt = hidden_order_detection_results.get(direction, 0)
                if price_detected > 0:
                    self.hidden_orders[other_side].append({"price": price_detected, "countdown": self.hidden_order_lifespan})
                    self.probe_altcoin_spent += altcoin_dealt
                    self.probe_quotecoin_spent += quotecoin_dealt
                    self.logger.debug(f"probe mode enabled :: detected hidden {other_direction} @ {price_detected}")
                    self.logger.debug(f"probe mode enabled :: used {self.probe_altcoin_spent} {self.altcoin}, {self.probe_quotecoin_spent} {self.quotecoin}")
                    return False
        elif self.probe_mode:
            self.logger.warning(f"probe mode enabled :: unable to probe due to {self.probe_altcoin_limit} {self.altcoin} limit or {self.probe_quotecoin_limit} {self.quotecoin} limit")

        return orderprice

    # ABOTS-100
    def overlay_hidden_orders(self, bid_1, ask_1, hidden_orders):
        """ countdown hidden orders, overlay them and return the expected bid_1, ask_1 """
        result = {"bid_1": bid_1, "ask_1": ask_1}
        for side, operation, reference in [("bids", max, "bid_1"), ("asks", min, "ask_1")]:
            cur_hidden_orders = hidden_orders.get(side, [])
            new_hidden_orders = []
            while len(cur_hidden_orders) > 0:
                hidden_order = cur_hidden_orders.pop()
                # remove hidden orders that have "expired", also orders whose prices no longer make sense
                if hidden_order["countdown"] != 0 and ((side == "bids" and hidden_order["price"] < ask_1) or (side == "asks" and hidden_order["price"] > bid_1)):
                    hidden_order["countdown"] = hidden_order.get("countdown", 0) - 1
                    new_hidden_orders.append(hidden_order)
                    result[reference] = operation(result[reference], hidden_order.get("price", 0))
            hidden_orders.update({side: new_hidden_orders})
        return {"price": result["bid_1"]}, {"price": result["ask_1"]}, hidden_orders

    # ABOTS-100
    def perform_hidden_order_probe(self, order_price, direction):
        """ probe using the broker(s) """
        # these could be separated to make it standalone
        minimum_order_size = self.minimum_order_size
        minimum_order_notional = self.minimum_order_notional
        ask_broker = self.brokers[0]    # using the same broker for now
        bid_broker = self.brokers[0]
        trading_pair = self.tradingpair
        if direction == "RANDOM":   # force to buyfirst for easier processing
            direction = "BUYFIRST"
        # end of self attributes
        altcoin_dealt = 0
        quotecoin_dealt = 0
        hidden_orders = {"BUY": [0, 0, 0], "SELL": [0, 0, 0]}
        probe_settings = {"BUYFIRST": [(ask_broker, "SELL"), (bid_broker, "BUY")], "SELLFIRST": [(bid_broker, "BUY"), (ask_broker, "SELL")]}
        if minimum_order_size > 0:
            probe_size = minimum_order_size
        elif minimum_order_notional > 0:
            probe_size = minimum_order_notional / order_price
        for broker, side in probe_settings.get(direction, []):
            if altcoin_dealt == 0:
                try:
                    order_details = self.place_and_cancel_probe(broker, trading_pair, side, order_price, probe_size)
                    self.logger.debug(f"probe mode enabled :: probe order_details = {order_details}")
                    order_status = order_details.get("Status", "")
                    dealt_amount = order_details.get("FilledAmount", 0)
                    dealt_price = order_details.get("Price", order_price)
                    # special handling in case the exchange suffers from a delay
                    if order_status == "Completed" and dealt_amount < probe_size:
                        dealt_amount = probe_size
                    if dealt_amount > 0:
                        altcoin_dealt += dealt_amount
                        quotecoin_dealt += dealt_amount * dealt_price
                        hidden_orders[side] = [dealt_price, altcoin_dealt, quotecoin_dealt]   # only able to detect 1 price at a time
                except BaseException as e:
                    self.logger.warning(f"probe mode enabled :: {e}")
        return hidden_orders

    # ABOTS-100
    def place_and_cancel_probe(self, broker, trading_pair, order_side, order_price, order_size):
        """ place and then cancel the said order """
        if not self.safety_check(order_price, order_size, broker, order_side):
            self.logger.warning(f"probe mode enabled :: unable to perform probe as it would breach reserve requirements")
            return {}
        order_id = getattr(broker.client,f"{order_side.lower()}_wait")(trading_pair, order_price, order_size)[0]
        cancel_state = broker.client.cancel_wait(order_id)
        order_details = broker.client.get_order_details(trading_pair, f"{order_side}_LIMIT", order_id, data_format='legacy')
        return order_details

    def coordinator_process(self):
        """ threading coordinator task """
        time.sleep(0.1)
        self.logger.debug("order window is now opened")
        self.coordination_flag = True
        time.sleep(self.execution_window)
        self.abandon_flag = True
        self.coordination_flag = False
        self.logger.debug("order window is now closed")

    def sell_order(self, bidbroker, tradingpair, orderprice, ordervolume):
        """ threading sell order task """
        while not self.coordination_flag:
            time.sleep(0.01)
            if self.abandon_flag == True:
                self.logger.warning("unable to order as window is closed")
                return

        if not self.abandon_flag:
            self.logger.debug("placing sell order...")
            limitsellorderID = bidbroker.client.sell_wait(tradingpair, orderprice, ordervolume)[0]

            self.last_sell_order = limitsellorderID

            if not limitsellorderID:
                self.logger.info("The return sell order id is None!")

    def buy_order(self, askbroker, tradingpair, orderprice, ordervolume):
        """ threading buy order task """
        while not self.coordination_flag:
            time.sleep(0.01)
            if self.abandon_flag == True:
                self.logger.warning("unable to order as window is closed")
                return

        if not self.abandon_flag:
            self.logger.debug("placing buy order...")
            limitbuyorderID = askbroker.client.buy_wait(tradingpair, orderprice, ordervolume)[0]
            self.last_buy_order = limitbuyorderID

            if not limitbuyorderID:
                self.logger.info("The return buy order id is None!")

    def update_fail_ratio(self, latest_result):
        """update fail ratio"""
        self.failratio = ((self.memory_window - 1) * self.failratio + latest_result/2.0 ) / self.memory_window
        self.logger.debug("The current failratio is %s" % self.failratio)

    def generate_pair_orders(self, bidbroker, askbroker, tradingpair, orderprice, ordervolume, commission=0.0, buyfirst=False):
        """
        :bidbroker: the broker takes our sell order
        :askbroker: the broker takes our buy order
        """
        self.logger.debug(f'generating pair orders')
        limitbuyorderID = None
        limitsellorderID = None
        sell_details, buy_details = None, None
        forced_cancel = False

        if self.execution_mode.upper() in ['MULTIIOC']:
            forced_cancel = True

        # Commission discount has been take out
        # if True send buy order first

        if self.orderBookTimeDelay > self.maxTimeDelay:
            return None

        if not self.safety_check(orderprice, ordervolume, askbroker, 'BUY') or not self.safety_check(orderprice, ordervolume, bidbroker, 'SELL'):
            self.logger.error(f"order of {ordervolume} {self.altcoin} @ {orderprice} {self.quotecoin} not placed as it will breach the reserve altcoin of {self.reservedaltcoin} and/or quotecoin {self.reservedquotecoin}")
            return None

        # patient mode
        if self.execution_mode == 'PATIENT':
            try:
                return self.generate_patient_order(bidbroker, askbroker, tradingpair, ordervolume)
            except:
                self.logger.error(traceback.format_exc())
                return
        if self.execution_mode == 'UNSAFE':
            try:
                return self.generate_unsafe_orders(bidbroker, askbroker, tradingpair, ordervolume)
            except:
                self.logger.error(traceback.format_exc())
                return
        # multi-threading mode
        if self.execution_mode.upper() in ['MULTI','MULTIIOC']:
            # threading, with separate coordination thread
            self.logger.debug(f'generating pair orders: MULTI / MULTIOC: starting threads')
            self.last_sell_order, self.last_buy_order = None, None
            self.coordination_flag, self.abandon_flag = False, False
            task1 = threading.Thread(target=self.sell_order, args=(bidbroker, tradingpair, orderprice, ordervolume))
            task2 = threading.Thread(target=self.buy_order, args=(askbroker, tradingpair, orderprice, ordervolume))
            task3 = threading.Thread(target=self.coordinator_process)
            task1.start()
            task2.start()
            task3.start()
            # wait for the tasks to complete, no timeout
            task1.join()
            task2.join()
            task3.join()
            self.logger.debug(f'generating pair orders: MULTI / MULTIOC: joined threads')
            # validate if orders have been placed and update fail ratio accordingly
            if forced_cancel:
                # forcefully cancel any remaining orders
                if self.last_sell_order is not None:
                    bidbroker.client.cancel_wait(self.last_sell_order, no_wait=True)
                if self.last_buy_order is not None:
                    askbroker.client.cancel_wait(self.last_buy_order, no_wait=True)
            elif self.last_buy_order is None and self.last_sell_order is not None:
                bidbroker.client.cancel_wait(self.last_sell_order, no_wait=True)
                self.update_fail_ratio(1)
            elif self.last_sell_order is None and self.last_buy_order is not None:
                askbroker.client.cancel_wait(self.last_buy_order, no_wait=True)
                self.update_fail_ratio(1)
            elif self.last_sell_order is None and self.last_buy_order is None:
                # both failed, nothing to cancel
                self.update_fail_ratio(2)
            # recording
            if self.clerk and config.RECORD_TO_DB:
                if self.last_sell_order:
                    sell_details = self.record_order_to_db(bidbroker, tradingpair, 'SELL_LIMIT', self.last_sell_order)
                if self.last_buy_order:
                    buy_details = self.record_order_to_db(askbroker, tradingpair, 'BUY_LIMIT', self.last_buy_order)
            if config.RECORD_TO_CSV:
                if self.last_sell_order:
                    self.record_order_to_csv(bidbroker, tradingpair, 'SELL_LIMIT', self.last_sell_order, sell_details)
                if self.last_buy_order:
                    self.record_order_to_csv(askbroker, tradingpair, 'BUY_LIMIT', self.last_buy_order, buy_details)
            # validate if orders have crossed and update fail ratio accordingly
            if self.last_sell_order is not None and self.last_buy_order is not None:
                if buy_details is not None and sell_details is not None and buy_details['Status'] == sell_details['Status']:
                    self.update_fail_ratio(0)
                else:
                    self.update_fail_ratio(1)
            return

        # default, single thread mode
        if buyfirst:
            # limitbuyorderID = askbroker.client.buy_wait(tradingpair, orderprice, ordervolume/(1-commission))
            limitbuyorderID = askbroker.client.buy_wait(tradingpair, orderprice, ordervolume)[0]
            # only place second order if first one succeeds
            if limitbuyorderID:
                if self.explicit_wait_time > 0: # force the order to appear on the book
                    random_factor = random.uniform(0,self.explicit_wait_random)
                    time.sleep(self.explicit_wait_time * (random_factor if random_factor > 0 else 1))
                limitsellorderID = bidbroker.client.sell_wait(tradingpair, orderprice, ordervolume)[0]

            time.sleep(self.internal_wait_time)

            if limitbuyorderID:
                if self.clerk and config.RECORD_TO_DB:
                    buy_details = self.record_order_to_db(askbroker, tradingpair, 'BUY_LIMIT', limitbuyorderID)
                if config.RECORD_TO_CSV:
                    self.record_order_to_csv(askbroker, tradingpair, 'BUY_LIMIT', limitbuyorderID, buy_details)
            else:
                self.logger.error("The return buy order id is None!")

            if limitsellorderID:
                if self.clerk and config.RECORD_TO_DB:
                    sell_details = self.record_order_to_db(bidbroker, tradingpair, 'SELL_LIMIT', limitsellorderID)
                if config.RECORD_TO_CSV:
                    self.record_order_to_csv(bidbroker, tradingpair, 'SELL_LIMIT', limitsellorderID, sell_details)
            else:
                self.logger.error("The return sell order id is None!")
        # if False send sell order first
        else:
            limitsellorderID = bidbroker.client.sell_wait(tradingpair, orderprice, ordervolume)[0]
            # limitbuyorderID = askbroker.client.buy_wait(tradingpair, orderprice, ordervolume/(1-commission))
            # only place second order if first one succeeds
            if limitsellorderID:
                if self.explicit_wait_time > 0: # force the order to appear on the book
                    random_factor = random.uniform(0,self.explicit_wait_random)
                    time.sleep(self.explicit_wait_time * (random_factor if random_factor > 0 else 1))
                limitbuyorderID = askbroker.client.buy_wait(tradingpair, orderprice, ordervolume)[0]

            time.sleep(self.internal_wait_time)

            if limitsellorderID:
                if self.clerk and config.RECORD_TO_DB:
                    sell_details = self.record_order_to_db(bidbroker, tradingpair, 'SELL_LIMIT', limitsellorderID)
                if config.RECORD_TO_CSV:
                    self.record_order_to_csv(bidbroker, tradingpair, 'SELL_LIMIT', limitsellorderID, sell_details)
            else:
                self.logger.error("The return sell order id is None!")

            if limitbuyorderID:
                if self.clerk and config.RECORD_TO_DB:
                    buy_details = self.record_order_to_db(askbroker, tradingpair, 'BUY_LIMIT', limitbuyorderID)
                if config.RECORD_TO_CSV:
                    self.record_order_to_csv(askbroker, tradingpair, 'BUY_LIMIT', limitbuyorderID, buy_details)
            else:
                self.logger.error("The return buy order id is None!")

    @property
    def safe_size(self):
        return self._safe_size

    @safe_size.setter
    def safe_size(self, safe_size):
        try:
            self._safe_size = float(safe_size)
            if self._safe_size < 0:
                self._safe_size = 0
        except (TypeError, ValueError):
            self.logger.error(f'invalid safe size: {safe_size}, default to autogen')
            self._safe_size = None

    def calc_safe_prices(self, orderbook: OrderBook, order_amount=0) -> tuple:
        safe_size = self.safe_size
        # use this as a default safe depth for now
        # higher of half the order amount OR half the (lower) average depth per level
        # should be safe enough to prevent easy manipulation
        safe_size = safe_size or 0.35 * min(
            sum(bid.amount for bid in orderbook.bids)/len(orderbook.bids),
            sum(ask.amount for ask in orderbook.asks)/len(orderbook.asks),
            order_amount
        )
        self.logger.debug(f'safe_size set to {safe_size}')
        self.logger.debug(f'order book {orderbook}')
        try:
            safe_buy = next(
                price_level
                for price_level in orderbook.bids
                if price_level.cumulative >= safe_size
            )
            safe_ask = next(
                price_level
                for price_level in orderbook.asks
                if price_level.cumulative >= safe_size
            )
        except StopIteration:
            self.logger.debug(f'mid price set to 0 (safe_size exceeds orderbook depth)')
            return 0, 0, 0
        mid_price = (safe_buy.price  +safe_ask.price)/2
        self.logger.debug(f'mid price set to {mid_price}')
        return safe_ask.price, mid_price, safe_buy.price

    def safe_side(self, order_amount=None):
        broker = self.brokers[0]
        ob = broker.orderbook
        _, mid_price, _ = self.calc_safe_prices(ob, order_amount)
        if not mid_price:
            self.logger.debug('safe_size exceeded orderbook depth, not placing an order')
            return None

        bid1 = ob.bids[0]['price']
        ask1 = ob.asks[0]['price']

        if self.probe_mode:
            bid1, ask1, self.hidden_orders = self.overlay_hidden_orders(bid1, ask1, self.hidden_orders)
            bid1 = bid1['price']
            ask1 = ask1['price']
        
        ask_distance = ask1 - mid_price
        bid_distance = mid_price - bid1
        self.logger.debug(f'bid/ask distance is {bid_distance}/{ask_distance}')
        if bid_distance > ask_distance:
            return BUY
        elif ask_distance > bid_distance:
            return SELL
        else:
            return random.choice([BUY, SELL])

    def generate_patient_order(
        self, 
        buy_broker: exchange_broker, 
        sell_broker: exchange_broker, 
        pair, 
        amount,
        aggressive=False,
    ):
        broker = self.brokers[0]
        self.update_order_book_meaningfully()

        def get_order(order_id, side) -> Order:
            order_details = (buy_broker if side == BUY else sell_broker).client.get_order_details(
                pair, 'SELL_LIMIT' if side == SELL else 'BUY_LIMIT', order_id
            )
            if not order_details:
                self.logger.error(f'failed retrieving details for {order_id}')
                return None

            return order_details

        def choose_price(side) -> float:
            # choosing the price here is fine
            # because a price was already generated
            # meaning that all the usual safeguards were followed
            # we're just ignoring the chosen price completely and making up
            # our own price 
            ob = broker.orderbook
            _, mid_price, _ = self.calc_safe_prices(ob, order_amount=amount)
            bid1 = ob.bids[0].price
            ask1 = ob.asks[0].price

            if self.probe_mode:
                bid1, ask1, self.hidden_orders = self.overlay_hidden_orders(bid1, ask1, self.hidden_orders)
                bid1 = bid1.price
                ask1 = ask1.price
        
            # triangle distribution for less boring klines
            buy_price = bid1 + self.minimumticksize 
            sell_price = ask1 - self.minimumticksize 
            self.logger.debug(f'calculated buy/sell prices {buy_price}/{sell_price}')
            if side == BUY:
                price = random.triangular(
                    buy_price,
                    min(sell_price, mid_price) if mid_price > buy_price else sell_price,
                    buy_price,
                )
            else:
                price = random.triangular(
                    max(buy_price, mid_price) if mid_price < sell_price else buy_price, 
                    sell_price, 
                    sell_price,
                )
            return round(price, self.orderpricerounding)

        def crossing_amounts() -> list:
            """ sizes of crossing orders to send """
            remaining = amount

            min_order_size = max(
                self.minimum_order_size, 
                self.minimum_order_notional/price, 
                10 ** -self.ordersizerounding,
                0.05 * amount,
            )

            crossing_amount = min_order_size
            crossing_amounts = []

            while remaining > 0:
                if crossing_amount > remaining:
                    crossing_amount = remaining
                crossing_amount = round(crossing_amount, self.ordersizerounding)
                #self.logger.debug(f'chose crossing amount {crossing_amount}')
                if crossing_amount == 0:
                    self.logger.error(f'cannot choose crossing amount of 0!')
                    self.logger.error(f'{self.minimum_order_size} / {self.minimum_order_notional/price} / {self.ordersizerounding} / {amount}')
                    raise ValueError
                remaining -= crossing_amount
                #self.logger.debug(f'remaining: {remaining}')
                # usually due to float arithmetic
                if remaining <= min_order_size:
                    crossing_amount += remaining
                    remaining = 0
                crossing_amounts.append(crossing_amount)
                crossing_amount *= 1 + (random.betavariate(0.5, 0.5) * 10)

            return crossing_amounts

        def crossing_order_delay() -> float:
            return random.uniform(self.explicit_wait_time, self.orderwaitingtime/random.uniform(4, 8))

        side = self.safe_side(order_amount=amount)
        if not side:
            # orderbook sizes too shallow
            return
        price = choose_price(side)

        if side == BUY:
            send_order = buy_broker.client.buy_wait
            send_order_opposite = sell_broker.client.sell_wait
            cancel_order = buy_broker.client.cancel_wait
            cancel_order_opposite = sell_broker.client.cancel_wait
        elif side == SELL:
            send_order = sell_broker.client.sell_wait
            send_order_opposite = buy_broker.client.buy_wait
            cancel_order = sell_broker.client.cancel_wait
            cancel_order_opposite = buy_broker.client.cancel_wait

        self.logger.debug(f'sending order {side} {pair} {price} {amount}')
        order_id = send_order(pair, price, amount)[0]
        self.log_order(order_id, side, buy_broker if side == BUY else sell_broker)
        crossing_order_amounts = crossing_amounts()
        self.logger.debug(f'created crossing order sequence {crossing_order_amounts}')
        for i, crossing_amount in enumerate(crossing_order_amounts):
            time.sleep(crossing_order_delay())
            order = get_order(order_id, side)

            if order is None:
                cancel_order(order_id)
                self.logger.debug('failed to retrieve order, canceling')
                self.order_ts = 0 #immediately retry
                return

            if order.completed:
                self.logger.debug(f'order {order_id} completed: {order.__dict__}')
                return

            if order.dealt < sum(crossing_order_amounts[:i]) and not isclose(order.dealt, sum(crossing_order_amounts[:i]), abs_tol=0.001):
                self.logger.debug(f'order dealt {order.dealt} is less than crossing orders {crossing_order_amounts[:i]}, canceling')
                self.order_ts = 0 #immediately retry
                cancel_order(order_id)
                return

            self.logger.debug(f'order {order_id} not complete, checking if safe to take')
            # check if it's safe to take own order
            self.update_order_book_meaningfully()
            ob = broker.orderbook
            l1 = (ob.bids if side == BUY else ob.asks)[0]
            self.logger.debug(f'top of book is now {l1}, {side}')
            remaining = order.amount - order.dealt
            self.logger.debug(f'order has price: {price} amount: {amount} dealt: {order.dealt} remaining: {remaining}')
            if crossing_amount > remaining:
                crossing_amount = remaining
            if l1.price == order.price and isclose(l1.amount, remaining):
                # the order is still the only one there, safe to clear it (hopefully)
                self.logger.debug(f'sending crossing order {i}: {-side}, {crossing_amount}')
                order_id_crossing = send_order_opposite(pair, price, crossing_amount)[0]
                self.log_order(order_id_crossing, -side, sell_broker if side == BUY else buy_broker)

                time.sleep(self.explicit_wait_time)
                cancel_order_opposite(order_id_crossing)
            else:
                # our order is no longer at the top of the book, no longer safe to clear it
                self.logger.debug('order not at top of book, canceling')
                self.order_ts = 0 #immediately retry
                cancel_order(order_id)
                return
        else:
            self.logger.debug('finished sending crossing orders, canceling')
            cancel_order(order_id)
            
    def generate_unsafe_orders(
        self, 
        buy_broker: exchange_broker, 
        sell_broker: exchange_broker, 
        pair, 
        amount,
        aggressive=False,
    ):
        broker = self.brokers[0]
        # everything is self-contained here so it's easier to follow
        # and to avoid name collisions; the names are long enough already
        def choose_price(side) -> float:
            # choosing the price here is fine
            # because a price was already generated
            # meaning that all the usual safeguards were followed
            # we're just ignoring the chosen price completely and making up
            # our own price 
            ob = broker.orderbook
            _, mid_price, _ = self.calc_safe_prices(ob, order_amount=amount)
            l1_spread_size = ob.asks[0].price - ob.bids[0].price
            if side == BUY:
                price_bound_upper = mid_price - self.minimumticksize
                price_bound_lower = round(mid_price - l1_spread_size / 3, self.orderpricerounding)
            else:
                price_bound_upper = mid_price + self.minimumticksize
                price_bound_lower = round(mid_price + l1_spread_size / 3, self.orderpricerounding)
            self.logger.debug(f'choosing {side} price between {price_bound_upper} and {price_bound_lower}')
            price = random.uniform(price_bound_lower, price_bound_upper)
            return round(price, self.orderpricerounding)

        def crossing_order_amount(side, price, amount):
            ob = broker.orderbook
            crossing_amount = amount
            for price_level in (ob.asks if side == BUY else ob.bids):
                if side == BUY and price_level.price > price:
                    break
                if side == SELL and price_level.price < price:
                    break
                crossing_amount -= price_level.amount
            return crossing_amount
            

        self.update_order_book_meaningfully()
        side = self.safe_side(order_amount=amount)
        opp_side = BUY if side == SELL else SELL
        if not side:
            return
        price = choose_price(side)
        crossing_amount = crossing_order_amount(side, price, amount)

        if side == BUY:
            send_order = buy_broker.client.buy
            send_order_opposite = sell_broker.client.sell
            cancel_order = buy_broker.client.cancel
            cancel_order_opposite = sell_broker.client.cancel
            get_order_details = buy_broker.client.get_order_details
            get_order_details_opposite = sell_broker.client.get_order_details
        elif side == SELL:
            send_order = sell_broker.client.sell
            send_order_opposite = buy_broker.client.buy
            cancel_order = sell_broker.client.cancel
            cancel_order_opposite = buy_broker.client.cancel
            get_order_details = sell_broker.client.get_order_details
            get_order_details_opposite = sell_broker.client.get_order_details

        self.logger.debug(f'sending order {side} {pair} {price} {amount}')
        self.logger.debug(f'sending crossing order {-side} {pair} {price} {crossing_amount}')
        order_id = send_order(pair, price, amount)
        order_id_crossing = send_order_opposite(pair, price, crossing_amount)

        cancel_order(order_id)
        cancel_order_opposite(order_id_crossing)
        time.sleep(self.orderwaitingtime/8)

        order_ref = get_order_details(order_id=order_id).get('order_ref')
        if order_ref:
            self.log_order(order_ref, side, buy_broker if side == BUY else sell_broker)
        order_ref_opposite=get_order_details_opposite(order_id=order_id_crossing).get('order_ref')
        if order_ref_opposite:
            self.log_order(order_ref_opposite, -side, sell_broker if side == BUY else buy_broker)

    def record_order_to_db(self, broker, tradingpair, ordertype, orderid):
        """record order details to database"""
        self.logger.debug(f'record_order_to_db: {tradingpair} : {orderid}')
        orderdetails, retries = None, 0
        #
        num_of_retries = { 'BIBOX':1 }.get(broker.get_exchangename(), 5)
        while orderdetails is None and retries < num_of_retries:
            if retries > 0: time.sleep(self.internal_wait_time)
            orderdetails = broker.client.get_order_details(tradingpair, ordertype, orderid, data_format='legacy')
            retries += 1
        #
        if orderdetails:
            self.logger.debug(f'add to db {orderdetails}')
            orderdetails['Source'] = 'Volume'
            orderdetails['BotID'] = self.botid
            self.clerk.insert_execution_order_sqla(orderdetails)
        else:
            self.logger.error(f'failed add to db for {orderid}')
        return orderdetails


    def record_order_to_csv(self, broker, tradingpair, ordertype, orderid, orderdetails=None):
        """record order results to a csv file"""
        # uncomment
        if orderdetails is None:
            orderdetails = broker.client.get_order_details(tradingpair, ordertype, orderid, data_format='legacy')
        else:
            try:
                del(orderdetails['Source'])
                del(orderdetails['BotID'])
            except Exception as e:
                self.logger.warning(f'{e}')

        # TODO save orderlist to a csv file
        if isinstance(orderdetails, dict) and len(orderdetails)>0:
            if orderdetails['Status'] == 'Not Completed':
                if ordertype == 'SELL_LIMIT':
                    broker.sellorderlist.append(orderid)
                    # TODO Save the orderlist to csv file 
                elif ordertype == 'BUY_LIMIT':
                    broker.buyorderlist.append(orderid)
                    # TODO Save the orderlist to csv file 
                else:
                    # TODO logging
                    return False
        else:
            return False

        if isinstance(orderdetails, dict) and len(orderdetails) > 0:
            filename = self.altcoin.upper() + '_' + self.quotecoin.upper() + '_' + self.brokers[0].get_exchangename() +'_Orders.csv'
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

    def update_database_records(self):
        """update database entries"""
        self.logger.debug(f"<update_database_records> Starting db query")
        exchange_name = self.brokers[0].get_exchangename()
        account_ids = [broker.get_account_id() for broker in self.brokers]
        self.logger.debug(f"<update_database_records> for {account_ids}. Starting db query")
        records = self.clerk.get_record_from_db(self.tradingpair, exchange_name, account_ids,
                                                source='Volume')
        self.logger.info(f'fetched {len(records)} not completed records from the database')

        # # use bulk fetch mode if supported
        # if len(records) > 0:
        #     bulk_records = broker.get_order_details_bulk(records)
        #     if bulk_records is not None:
        #         self.logger.debug(f'bulk fetch successful - {len(bulk_records)} records')
        #         for record in records:
        #             orderdetails = bulk_records.get(str(record.OrderID), None)
        #             if orderdetails is not None:
        #                 self.logger.info(f'updating database record #{record.ID}')
        #                 self.clerk.update_execution_order_sqla(orderdetails, record.ID)
        #             else:
        #                 self.logger.debug(f'details for database record #{record.ID} not found')
        #         return

        # default mode: individual record fetch & update
        for record in records:
            orderdetails, retries = None, 0
            side = 'BUY_LIMIT' if record.LongShort == 'Long' else 'SELL_LIMIT'
            num_of_retries = {'BIBOX': 1}.get(exchange_name, 5)
            required_broker = \
                [broker for broker in self.brokers if str(broker.get_account_id()) == str(record.AccountKey)][0]
            while orderdetails is None and retries < num_of_retries:
                self.logger.info(f'fetching order details for #{record.OrderID}')
                if retries > 0: time.sleep(self.internal_wait_time)
                orderdetails = required_broker.client.get_order_details(record.TradingPair, side, record.OrderID, data_format='legacy')
                retries += 1

            if orderdetails is not None and isinstance(orderdetails, dict) and orderdetails != {}:
                if self.execution_mode in ['PATIENT', 'UNSAFE']:
                    self.logger.debug(f"canceling {orderdetails['OrderID']}")
                    required_broker.client.cancel_wait(orderdetails['OrderID'], no_wait=True)
                self.logger.info(f'updating database record #{record.ID}')
                self.clerk.update_execution_order_sqla(orderdetails, record.ID)

    def pending_order_processing(self):
        """check and update unfilled orders in csv file and orderlist"""
        orderdetails = []
        orderdictlist = []

        for broker in self.brokers:
            if not broker.can_run_heavy_duty_process():
                self.logger.info(f'unable to run heavy process')
                return

        if self.clerk and config.RECORD_TO_DB:
            try:
                self.logger.debug(f"pending_order_processing: updating database records")
                self.update_database_records()
            except Exception as e:
                self.logger.error(traceback.format_exc())
                self.logger.error(f'pending_order_processing {e}')
                raise TimeoutError
            return

        filename = self.altcoin.upper() + '_' + self.quotecoin.upper() + '_' + self.brokers[0].get_exchangename() +'_Orders.csv'
        if os.path.isfile(filename):
            try:
                orderdictlist = self.read_order_from_csv(filename)
            except Exception as e:
                self.logger.warning("failed to read orders from csv. {e}")
                orderdictlist = []
        else:
            return False

        for broker in self.brokers:
            if len(broker.buyorderlist) > 0:
                # filename = self.altcoin.upper() + '_' + self.quotecoin.upper() + '_' + self.brokers[0].get_exchangename() +'_Orders.csv'
                # orderdictlist = self.read_order_from_csv(filename)
                for pendingorderid in reversed(list(broker.buyorderlist)):
                    orderdetails = broker.client.get_order_details(self.tradingpair, 'BUY_LIMIT', pendingorderid, data_format='legacy')
                    if self.execution_mode in ['PATIENT', 'UNSAFE']:
                        broker.client.cancel_wait(orderdetails['OrderID'], no_wait=True)
                    # remove the pending order id from buy order list if its status becomes Completed. 
                    if isinstance(orderdetails, dict) and len(orderdetails):
                        if orderdetails['Status'] == 'Completed' or orderdetails['Status'] == 'Canceled':
                            broker.buyorderlist.remove(pendingorderid)
                    # update the pending order record in csv file
                    if isinstance(orderdetails, dict) and isinstance(orderdictlist, list) and len(orderdictlist) and len(orderdetails):
                        for existingorder in orderdictlist:
                            if existingorder['OrderID'] == str(orderdetails['OrderID']):
                                existingorder['TimeStamp'] = orderdetails['TimeStamp']
                                existingorder['Amount'] = orderdetails['Amount']
                                existingorder['FilledAmount'] = orderdetails['FilledAmount']
                                existingorder['Status'] = orderdetails['Status']
                                existingorder['Price'] = orderdetails['Price']
                                break
                # with open(filename, 'w') as csvfile:
                #     headers = ['OrderID', 'TimeStamp', 'TradingPair', 'Exchange', 'AccountKey', 'LongShort', 'Amount', 'FilledAmount', 'Price', 'Status']
                #     csvwriter = csv.DictWriter(csvfile, delimiter=',', lineterminator='\n', fieldnames=headers)
                #     csvwriter.writeheader()
                #     csvwriter.writerows(orderdictlist)
            if len(broker.sellorderlist) > 0:
                # filename = self.altcoin.upper() + '_' + self.quotecoin.upper() + '_' + self.brokers[0].get_exchangename() +'_Orders.csv'
                # orderdictlist = self.read_order_from_csv(filename)
                for pendingorderid in reversed(list(broker.sellorderlist)):
                    orderdetails = broker.client.get_order_details(self.tradingpair, 'SELL_LIMIT', pendingorderid, data_format='legacy')
                    if self.execution_mode in ['PATIENT', 'UNSAFE']:
                        self.logger.debug(f"canceling {orderdetails['OrderID']}")
                        broker.client.cancel_wait(orderdetails['OrderID'], no_wait=True)
                    # remove the pending order id from sell order list if its status becomes Completed.
                    if isinstance(orderdetails, dict) and len(orderdetails):
                        if orderdetails['Status'] == 'Completed' or orderdetails['Status'] == 'Canceled':
                            broker.sellorderlist.remove(pendingorderid)
                    if isinstance(orderdetails, dict) and isinstance(orderdictlist, list) and len(orderdictlist) and len(orderdetails):
                        for existingorder in orderdictlist:
                            if existingorder['OrderID'] == str(orderdetails['OrderID']):
                                existingorder['TimeStamp'] = orderdetails['TimeStamp']
                                existingorder['Amount'] = orderdetails['Amount']
                                existingorder['FilledAmount'] = orderdetails['FilledAmount']
                                existingorder['Status'] = orderdetails['Status']
                                existingorder['Price'] = orderdetails['Price']
                                break
        # write the updated order records back to the csv file               
        with open(filename, 'w') as csvfile:
            headers = ['OrderID', 'TimeStamp', 'TradingPair', 'Exchange', 'AccountKey', 'LongShort', 'Amount', 'FilledAmount', 'Price', 'Status','FilledCashAmount', 'BuyCommission', 'NetFilledCashAmount', 'SellCommission', 'NetFilledAmount']
            csvwriter = csv.DictWriter(csvfile, delimiter=',', lineterminator='\n', fieldnames=headers)
            csvwriter.writeheader()
            csvwriter.writerows(orderdictlist)


    def safety_check(self, price, volume, broker, side):
        """check that reserve requirements are met"""
        self.logger.debug(f"safety_check: {side} {volume} @ {price}")
        quotecoinbalance = broker.get_coin_tradable_balance(self.quotecoin)
        altcoinbalance = broker.get_coin_tradable_balance(self.altcoin)
        if side == 'BUY':
            if quotecoinbalance - volume * price < self.reservedquotecoin:
                return False
        elif side == 'SELL':
            if altcoinbalance - volume < self.reservedaltcoin:
                return False
        return True

    def meets_minimum_size_and_notional_thresholds(self, price, volume):
        """ checks if the minimum size requirements are met """
        if volume < self.minimum_order_size:
            self.logger.warning(f"generated order size {volume} is below minimum order size {self.minimum_order_size}")
            return False
        if price * volume < self.minimum_order_notional:
            self.logger.warning(f"generated order notional {volume * price} is below minimum order notional {self.minimum_order_notional}")
            return False
        return True

    def wash_trading_buy_first(self, tradingpair):
        """Give me volume!!! Always submit buy order first"""
        if self.failratio < 0.5:
            orderprice = self.generate_order_price(tradingpair, "1_TICK_BELOW_ASK1" if self.conservative_mode else None)
        else:
            orderprice = self.generate_order_price(tradingpair, "1_TICK_BELOW_ASK1" if self.conservative_mode else "PERCENTAGE_ABOVE_BID1")
        ordervolume = self.generate_order_volume(tradingpair)
        if not orderprice or not ordervolume or not self.meets_minimum_size_and_notional_thresholds(orderprice, ordervolume):
            return False

        do_generate_order = False
        if int(time.time() - self.order_ts) > self.orderwaitingtime:
            self.order_ts = time.time()
            do_generate_order = True
            self.orderwaitingtime = (random.randint(self.minwaitingtime*1000, self.maxwaitingtime*1000))/1000

        if do_generate_order:
            commission = 0.0
            buyfirst = True

            if len(self.brokers) == 1:
                commission = self.brokers[0].get_commission()
                self.generate_pair_orders(self.brokers[0], self.brokers[0], tradingpair, orderprice, ordervolume, commission, buyfirst)

            elif len(self.brokers) == 2:
                # TODO need to add function which could adjust balance between two accounts
                broker0first = bool(random.getrandbits(1))

                if broker0first:
                    commission = self.brokers[0].get_commission()
                    self.generate_pair_orders(self.brokers[1], self.brokers[0], tradingpair, orderprice, ordervolume, commission, buyfirst)
                else:
                    commission = self.brokers[1].get_commission()
                    self.generate_pair_orders(self.brokers[0], self.brokers[1], tradingpair, orderprice, ordervolume, commission, buyfirst)

            else:
                # self.logger.error("Number of brokers is neither 1 nor 2, washing failed!" )
                # return False
                b0, b1 = self.pick_2_brokers()

                commission = self.brokers[b1].get_commission()
                self.generate_pair_orders(self.brokers[b0], self.brokers[b1], tradingpair, orderprice, ordervolume, commission, buyfirst)

    def pick_2_brokers(self, method=None):
        """ pick 2 brokers """
        b0 = b1 = 0
        if method != "random":
            num_of_brokers = len(self.brokers)
            total_altcoin = sum([broker.get_coin_tradable_balance(self.altcoin) for broker in self.brokers])
            total_quotecoin = sum([broker.get_coin_tradable_balance(self.quotecoin) for broker in self.brokers])

            # calculate relative strengths
            relative_altcoin_holdings = [broker.get_coin_tradable_balance(self.altcoin)/total_altcoin for broker in self.brokers]
            relative_quotecoin_holdings = [broker.get_coin_tradable_balance(self.quotecoin)/total_quotecoin for broker in self.brokers]
            # if there are no quotecoins, set relative strength to 1e10 (i.e. an aribitary large number)
            relative_altcoin_strengths = [(relative_altcoin_holdings[i] / relative_quotecoin_holdings[i]) if relative_quotecoin_holdings[i] > 0 else 1e10 for i in range(num_of_brokers)]
            sum_relative_altcoin_strength = sum(relative_altcoin_strengths)
            sell_altcoin_probabilties = [relative_altcoin_strength/sum_relative_altcoin_strength for relative_altcoin_strength in relative_altcoin_strengths]
            relative_quotecoin_strengths = [(relative_quotecoin_holdings[i] / relative_altcoin_holdings[i]) if relative_altcoin_holdings[i] > 0 else 1e10 for i in range(num_of_brokers)]
            sum_relative_quotecoin_strength = sum(relative_quotecoin_strengths)
            sell_quotecoin_probabilities = [relative_quotecoin_strength/sum_relative_quotecoin_strength for relative_quotecoin_strength in relative_quotecoin_strengths]

            # b0 sells altcoin, b1 sells quotecoin
            cycle_limit = 200
            cycle_count = 0
            while b0 == b1 and cycle_count < cycle_limit:
                b0 = choice(num_of_brokers, 1, p=sell_altcoin_probabilties)[0]
                b1 = choice(num_of_brokers, 1, p=sell_quotecoin_probabilities)[0]
                cycle_count += 1
            if cycle_count >= cycle_limit:
                method = "random"
        if method == "random":
            cycle_limit = 200
            cycle_count = 0
            while b0 == b1 and cycle_count < cycle_limit:
                b0 = random.randint(0,len(self.brokers)-1)
                b1 = random.randint(0,len(self.brokers)-1)
                cycle_count += 1
            if cycle_count >= cycle_limit:
                return 0, 1
        return b0, b1

    def wash_trading_sell_first(self, tradingpair):
        """Give me volume!!! Always submit sell order first"""
        if self.failratio < 0.5:
            orderprice = self.generate_order_price(tradingpair, "1_TICK_ABOVE_BID1" if self.conservative_mode else None)
        else:
            orderprice = self.generate_order_price(tradingpair, "1_TICK_ABOVE_BID1" if self.conservative_mode else "PERCENTAGE_BELOW_ASK1")
        ordervolume = self.generate_order_volume(tradingpair)
        if not orderprice or not ordervolume or not self.meets_minimum_size_and_notional_thresholds(orderprice, ordervolume):
            return False

        do_generate_order = False
        if int(time.time() - self.order_ts) > self.orderwaitingtime:
            self.order_ts = time.time()
            do_generate_order = True
            self.orderwaitingtime = (random.randint(self.minwaitingtime*1000, self.maxwaitingtime*1000))/1000

        if do_generate_order:
            commission = 0.0
            buyfirst = False

            if len(self.brokers) == 1:
                commission = self.brokers[0].get_commission()
                self.generate_pair_orders(self.brokers[0], self.brokers[0], tradingpair, orderprice, ordervolume, commission, buyfirst)

            elif len(self.brokers) == 2:
                # TODO need to add function which could adjust balance between two accounts
                broker0first = bool(random.getrandbits(1))

                if broker0first:
                    commission = self.brokers[1].get_commission()
                    self.generate_pair_orders(self.brokers[0], self.brokers[1], tradingpair, orderprice, ordervolume, commission, buyfirst)
                else:
                    commission = self.brokers[0].get_commission()
                    self.generate_pair_orders(self.brokers[1], self.brokers[0], tradingpair, orderprice, ordervolume, commission, buyfirst)

            else:
                # self.logger.error("Number of brokers is neither 1 nor 2, washing failed!")
                # return False
                b0, b1 = self.pick_2_brokers()

                commission = self.brokers[b1].get_commission()
                self.generate_pair_orders(self.brokers[b0], self.brokers[b1], tradingpair, orderprice, ordervolume, commission, buyfirst)

    def has_sufficient_balance(self, broker, asset, requirement):
        """ returns if there's sufficient balance """
        return broker.get_coin_tradable_balance(asset) >= requirement

    def wash_trading_random(self, tradingpair):
        """Give me volume!!! Randomly decide whether buy or sell order goes first"""
        orderprice = self.generate_order_price(tradingpair)
        ordervolume = self.generate_order_volume(tradingpair)
        self.logger.debug(f"wash_trading_random")

        if not orderprice or not ordervolume or not self.meets_minimum_size_and_notional_thresholds(orderprice, ordervolume):
            return False

        do_generate_order = False
        if int(time.time() - self.order_ts) > self.orderwaitingtime:
            # print("Give order: " + str(int(time.time())) + ", " + str(int(self.order_ts)) + ", " + str(int(time.time() - self.order_ts)) + " , " + str(self.orderwaitingtime))
            self.order_ts = time.time()
            do_generate_order = True
            self.orderwaitingtime = (random.randint(self.minwaitingtime*1000, self.maxwaitingtime*1000))/1000
            # print("next waiting time: " + str(self.orderwaitingtime))
        # else:
            # print("Wait: " + str(int(time.time())) +  ", " + str(int(self.order_ts)) + ", " + str(int(time.time() - self.order_ts)) + " , " + str(self.orderwaitingtime))

        if do_generate_order:
            self.logger.debug(f"wash_trading_random: do_generate_order = True")
            commission = 0.0 # commission
            buyfirst = bool(random.getrandbits(1))

            if len(self.brokers) == 1:
                commission = self.brokers[0].get_commission()
                self.generate_pair_orders(self.brokers[0], self.brokers[0], tradingpair, orderprice, ordervolume, commission, buyfirst)

            elif len(self.brokers) == 2:
                # TODO add function which could adjust balance between two accounts
                broker0first = bool(random.getrandbits(1))

                # flip if there's insufficent balance
                try:
                    if buyfirst:
                        if broker0first:
                            if not self.safety_check(orderprice, ordervolume, self.brokers[0], 'BUY') or not self.safety_check(orderprice, ordervolume, self.brokers[1], 'SELL'):
                                broker0first = not broker0first
                        else:
                            if not self.safety_check(orderprice, ordervolume, self.brokers[1], 'BUY') or not self.safety_check(orderprice, ordervolume, self.brokers[0], 'SELL'):
                                broker0first = not broker0first
                    else:
                        if broker0first:
                            if not self.safety_check(orderprice, ordervolume, self.brokers[1], 'BUY') or not self.safety_check(orderprice, ordervolume, self.brokers[0], 'SELL'):
                                broker0first = not broker0first
                        else:
                            if not self.safety_check(orderprice, ordervolume, self.brokers[0], 'BUY') or not self.safety_check(orderprice, ordervolume, self.brokers[1], 'SELL'):
                                broker0first = not broker0first
                except:
                    pass

                if buyfirst and broker0first: # first broker0 first buy
                    commission = self.brokers[0].get_commission()
                    self.generate_pair_orders(self.brokers[1], self.brokers[0], tradingpair, orderprice, ordervolume, commission, buyfirst)
                elif buyfirst and not broker0first: # second broker1 first buy
                    commission = self.brokers[1].get_commission()
                    self.generate_pair_orders(self.brokers[0], self.brokers[1], tradingpair, orderprice, ordervolume, commission, buyfirst)
                elif not buyfirst and broker0first: # first broker0 first sell
                    commission = self.brokers[1].get_commission()
                    self.generate_pair_orders(self.brokers[0], self.brokers[1], tradingpair, orderprice, ordervolume, commission, buyfirst)
                elif not buyfirst and not broker0first: # second broker1 frist sell
                    commission = self.brokers[0].get_commission()
                    self.generate_pair_orders(self.brokers[1], self.brokers[0], tradingpair, orderprice, ordervolume, commission, buyfirst)
                else:
                    pass
                self.logger.debug(f"wash_trading_random: generating orders complete")
            else:
                # self.logger.error("Number of brokers is neither 1 nor 2, washing failed!" )
                # return False
                b0, b1 = self.pick_2_brokers()

                commission = self.brokers[b1].get_commission()
                self.generate_pair_orders(self.brokers[b0], self.brokers[b1], tradingpair, orderprice, ordervolume, commission, buyfirst)


    def wash_trading_run(self, tradingpair):
        exchange_name = self.brokers[0].get_exchangename()

        if self.brokers[0].orderbook:
            askprice = self.brokers[0].get_lowest_ask_and_volume(self.tradingpair)
        else:
            self.update_order_book_meaningfully()
            raise ValueError('empty orderbook in last run cycle')

        for broker in self.brokers:
            if broker.get_exchangename() != exchange_name:
                self.logger.error("all accounts must be from the same exchange")
                return
            broker.clear_broker()

        if self.do_update_accout:
            self.update_account_balance_meaningfully()
            self.calculate_order_scalefactor(self.benchmarkpair)

        # balance check which prevents from one side ordering
        quotecoinbalance = sum([broker.get_coin_tradable_balance(self.quotecoin) for broker in self.brokers])
        altcoinbalance = sum([broker.get_coin_tradable_balance(self.altcoin) for broker in self.brokers])
        # ABOTS-95: no longer possible to pick up the prices here since they have already been cleared above
        # askprice = self.brokers[0].get_lowest_ask_and_volume(self.tradingpair)
        # break if there is not enough altcoin or quote coin
        if self.taskordersize > altcoinbalance or \
                (askprice and (self.taskordersize * askprice['price']) > quotecoinbalance):
            self.logger.error("There is not enough altcoin %s balance of %s or quote coin %s balance of %s."
            % (self.altcoin, altcoinbalance, self.quotecoin, quotecoinbalance))
            time.sleep(2)
            self.update_order_book_meaningfully()
            return

        if (altcoinbalance - self.taskordersize) < self.reservedaltcoin:
            self.logger.error("The %s tradable balance of altcoin %s is less than the minimum reserved altcoin balance of %s."
            % (altcoinbalance, self.altcoin, self.reservedaltcoin))
            time.sleep(2)
            self.update_order_book_meaningfully()
            return

        if (quotecoinbalance - self.taskordersize*askprice['price']) < self.reservedquotecoin:
            self.logger.error("The %s tradable balance of quote coin %s is less than the minimum reserved quote coin balance of %s."
            % (quotecoinbalance, self.quotecoin, self.reservedquotecoin))
            time.sleep(2)
            self.update_order_book_meaningfully()
            return

        # cast to float in case there are issues
        try:
            self.position['net'] = float(self.position['net'])
        except:
            self.position['net'] = 0

        # check if we have accumulated too large a position on the Long side
        if (self.max_altcoin_imbalance > 0 and (abs(self.position['net']) > self.max_altcoin_imbalance)):
            self.logger.error(f"The net position {self.position['net']} exceeds the max altcoin imbalance of {self.max_altcoin_imbalance}")
            time.sleep(2)
            self.update_order_book_meaningfully()
            return

        # check if we have accumulated too large a position on the Short side
        if (self.max_quotecoin_imbalance > 0 and (abs(self.position['filledcashamount']) > self.max_quotecoin_imbalance)):
            self.logger.error(f"The net position {self.position['filledcashamount']} exceeds the max quotecoin imbalance of {self.max_quotecoin_imbalance}")
            time.sleep(2)
            self.update_order_book_meaningfully()
            return

        self.update_order_book_meaningfully()
         
        if self.orderpriority == 'RANDOM':
            # Randomly decide whether buy or sell order goes first
            self.wash_trading_random(tradingpair)
        elif self.orderpriority == 'BUYFIRST':
            # Always submit buy order first
            self.wash_trading_buy_first(tradingpair)
        elif self.orderpriority == 'SELLFIRST':
            # Always submit sell order first
            self.wash_trading_sell_first(tradingpair)
        else:
            self.logger.error("Unknown order priority configuration value, launch washing algo failed!")
            return False


    def wash_trading(self, tradingpair):
        """Give me volume!!! This is an example function of wash trading."""
        warnings.warn("There is no random waiting time check in this example washing algo", DeprecationWarning)
        commission = 0.0 # commission
        buyfirst = False # prefer sell
        orderprice = self.generate_order_price(tradingpair)
        ordervolume = self.generate_order_volume(tradingpair)
        if not orderprice or not ordervolume or not self.meets_minimum_size_and_notional_thresholds(orderprice, ordervolume):
            return False

        if self.orderpriority == 'RANDOM':
            # Randomly decide whether buy or sell order goes first in this scenario
            buyfirst = bool(random.getrandbits(1))

            if len(self.brokers) == 1:
                commission = self.brokers[0].get_commission()
                self.generate_pair_orders(self.brokers[0], self.brokers[0], tradingpair, orderprice, ordervolume, commission, buyfirst)

            elif len(self.brokers) == 2:
                # TODO need to add function which could adjust balance between two accounts
                broker0first = bool(random.getrandbits(1))

                if buyfirst and broker0first:
                    commission = self.brokers[0].get_commission()
                    self.generate_pair_orders(self.brokers[1], self.brokers[0], tradingpair, orderprice, ordervolume, commission, buyfirst)
                elif buyfirst and not broker0first:
                    commission = self.brokers[1].get_commission()
                    self.generate_pair_orders(self.brokers[0], self.brokers[1], tradingpair, orderprice, ordervolume, commission, buyfirst)
                elif not buyfirst and broker0first:
                    commission = self.brokers[1].get_commission()
                    self.generate_pair_orders(self.brokers[0], self.brokers[1], tradingpair, orderprice, ordervolume, commission, buyfirst)
                elif not buyfirst and not broker0first:
                    commission = self.brokers[0].get_commission()
                    self.generate_pair_orders(self.brokers[1], self.brokers[0], tradingpair, orderprice, ordervolume, commission, buyfirst)
                else:
                    pass

            else:
                self.logger.error("Number of brokers is neither 1 nor 2, washing failed!" )
                return False

        elif self.orderpriority == 'BUYFIRST':
            # Always submit buy order first in this scenario
            buyfirst = True

            if len(self.brokers) == 1:
                commission = self.brokers[0].get_commission()
                self.generate_pair_orders(self.brokers[0], self.brokers[0], tradingpair, orderprice, ordervolume, commission, buyfirst)

            elif len(self.brokers) == 2:
                # TODO need to add function which could adjust balance between two accounts
                broker0first = bool(random.getrandbits(1))

                if broker0first:
                    commission = self.brokers[0].get_commission()
                    self.generate_pair_orders(self.brokers[1], self.brokers[0], tradingpair, orderprice, ordervolume, commission, buyfirst)
                else:
                    commission = self.brokers[1].get_commission()
                    self.generate_pair_orders(self.brokers[0], self.brokers[1], tradingpair, orderprice, ordervolume, commission, buyfirst)

            else:
                self.logger.warning("Number of brokers is neither 1 nor 2, washing failed!" )
                return False

        elif self.orderpriority == 'SELLFIRST':
            # Always submit sell order first in this scenario
            buyfirst = False

            if len(self.brokers) == 1:
                commission = self.brokers[0].get_commission()
                self.generate_pair_orders(self.brokers[0], self.brokers[0], tradingpair, orderprice, ordervolume, commission, buyfirst)

            elif len(self.brokers) == 2:
                # TODO need to add function which could adjust balance between two accounts
                broker0first = bool(random.getrandbits(1))

                if broker0first:
                    commission = self.brokers[1].get_commission()
                    self.generate_pair_orders(self.brokers[0], self.brokers[1], tradingpair, orderprice, ordervolume, commission, buyfirst)
                else:
                    commission = self.brokers[0].get_commission()
                    self.generate_pair_orders(self.brokers[1], self.brokers[0], tradingpair, orderprice, ordervolume, commission, buyfirst)

            else:
                self.logger.error("Number of brokers is neither 1 nor 2, washing failed!")
                return False

        else:
            self.logger.error("Unknown order priority configuration value, washing failed!")
            return False



    def reset_priority(self):
        """reset the order priority"""
        # TODO use callback function to implicitly change order priority without touching priority setting
        if self.failratio < 0.5:
            if float(self.position['net']) >= self.riskthreshold:
                self.orderpriority = 'SELLFIRST'
                self.logger.warning("Current position is: %s . Current fail ratio is %s. Order priority changed to SELLFIRST" % (self.position['net'],self.failratio))

            elif float(self.position['net']) <= -self.riskthreshold:
                self.orderpriority = 'BUYFIRST'
                self.logger.warning("Current position is: %s . Current fail ratio is %s. Order priority changed to BUYFIRST" % (self.position['net'],self.failratio))
            elif self.conservative_mode:
                # when operating in conservative mode, need to decide direction upfront
                self.orderpriority = choice(['BUYFIRST', 'SELLFIRST'])
                self.logger.debug(f"setting priority = {self.orderpriority} due to operation in conservative mode")
            elif self.riskthreshold * -0.05 < float(self.position['net']) < self.riskthreshold * 0.05:
                self.orderpriority = self.originalpriority
                self.logger.warning("Current position is: %s . Current fail ratio is %s. Order priority changed back to %s" % (self.position['net'],self.failratio,self.originalpriority))
            else:
                pass
        else:
            if float(self.position['net']) >= self.taskordersize * 20:
                self.orderpriority = 'SELLFIRST'
                self.logger.warning("Current position is: %s . Current fail ratio is %s. Order priority changed to SELLFIRST" % (self.position['net'],self.failratio))
            elif float(self.position['net']) <= self.taskordersize * -20:
                self.orderpriority = 'BUYFIRST'
                self.logger.warning("Current position is: %s . Current fail ratio is %s. Order priority changed to BUYFIRST" % (self.position['net'],self.failratio))
            elif self.conservative_mode:
                # when operating in conservative mode, need to decide direction upfront
                self.orderpriority = choice(['BUYFIRST', 'SELLFIRST'])
                self.logger.debug(f"setting priority = {self.orderpriority} due to operation in conservative mode")
            elif self.taskordersize * -20 < float(self.position['net']) < self.taskordersize * 20:
                self.orderpriority = self.originalpriority
                self.logger.warning("Current position is: %s . Current fail ratio is %s. Order priority changed back to %s" % (self.position['net'],self.failratio,self.originalpriority))
            else:
                pass


    #TODO: remove
    def pump_and_dump(self):
        """
        割韭菜
        """
        pass


    #TODO: remove
    def grid_computing(self):
        """
        深度(网格)算法
        """
        pass


    #TODO: remove
    def Spoofing(self):
        """
        submitting a genuine order on one side of the book and multiple orders at different prices on the other side of the book to give the impression of substantial supply/demand, 
        with a view to sucking in other orders to hit the genuine order. After the genuine order trades, the multiple orders on the other side are rapidly withdrawn
        """
        pass

    def update_account_balance_meaningfully(self):
        """update account balance (with retries built in)"""
        for broker in self.brokers:
            usable, retries = False, 0
            num_of_retries = { 'BIBOX':1 }.get(broker.get_exchangename(), 5)
            self.logger.debug(f"updating account balances")
            while not usable and retries < num_of_retries:
                if retries > 0: time.sleep(self.internal_wait_time)
                broker.update_account_balance(broker.get_account_id())
                usable = broker.get_coin_tradable_balance(self.quotecoin) >= 0.0 and broker.get_coin_tradable_balance(self.altcoin) >= 0.0
                retries += 1
            if not usable:
                self.logger.error(f"unable to get meaningful balance data after {retries} retries")

    def update_order_book_meaningfully(self):
        """update order book (with retries built in)"""
        self.brokers[0].clear_broker()
        usable, retries = False, 0
        num_of_retries = { 'BIBOX':1 }.get(self.brokers[0].get_exchangename(), 5)
        self.logger.debug(f"updating orderbook details")
        while not usable and retries < num_of_retries:
            if retries > 0: time.sleep(self.internal_wait_time)
            start_time = time.time()
            self.brokers[0].update_orderbook(self.tradingpair)
            self.brokers[0].orderbook = LegacyOrderBook(self.brokers[0].orderbook)
            end_time = time.time()
            self.orderBookTimeDelay = end_time - start_time
            lowestask = self.brokers[0].get_lowest_ask_and_volume(self.tradingpair)
            highestbid = self.brokers[0].get_highest_bid_and_volume(self.tradingpair)
            usable = lowestask is not None and highestbid is not None
            self.logger.warning(f"obtained {self.tradingpair} orderbook prices {lowestask}/{highestbid} in {self.orderBookTimeDelay} s")
            retries += 1
        if not usable:
            self.logger.error(f"unable to get meaningful bid ask data after {retries} retries")

    def clear_orders_aggressively(self, orders_to_clear):

        if len(orders_to_clear.get('bids')) == 0 and len(orders_to_clear.get('asks')) == 0:
            self.logger.debug("[AggressiveOrderClearing] No orders to clear aggressively.")
            return

        # List of objects that have been cleared with orderIDs
        tasks = []

        self.coordination_flag, self.abandon_flag = False, False

        # sell_order(self, bidbroker, tradingpair, orderprice, ordervolume):
        for i in range(len(orders_to_clear.get('bids'))):
            order = orders_to_clear['bids'][i]
            self.logger.debug("[AggressiveOrderClearing] Placing aggressive spread-clearing SELL order to clear BID: %s" %str(order))
            limitsellorderID = self.brokers[0].client.sell_wait(self.tradingpair, order.get('price'), order.get('volume'))[0]

            task = {
                'limitsellorderID': limitsellorderID,
                'volume': order.get('volume'),
                'price': order.get('price'),
                'side': 'sell'
            }
            tasks.append(task)

        self.coordination_flag, self.abandon_flag = False, False

        for i in range(len(orders_to_clear.get('asks'))):
            order = orders_to_clear['asks'][i]
            self.logger.debug("[AggressiveOrderClearing] Placing aggressive spread-clearing BUY order to clear ASK: %s" %str(order))
            limitbuyorderID = self.brokers[0].client.buy_wait(self.tradingpair, order.get('price'), order.get('volume'))[0]
            task = {
                'limitbuyorderID': limitbuyorderID,
                'volume': order.get('volume'),
                'price': order.get('price'),
                'side': 'buy'
            }
            tasks.append(task)

        self.logger.debug("[AggressiveOrderClearing] Cleared the following orders: %s" %str(tasks))

    def get_spread_to_clear(self):
        """Function to obtain bids and asks to clear if our spread is being manipulated. Description: https://altonomy.atlassian.net/browse/ABOTS-83"""
        orderbook = self.brokers[0].get_orderbook()
        orderbook_data = orderbook.get(self.tradingpair)

        orders_to_clear = {
            'bids': [],
            'asks': []
        }

        if not orderbook_data.get('bids') or not orderbook_data.get('asks'):
            self.logger.error(f"Orderbook for pair {self.tradingpair} not found! Will not proceed with aggressively clearing spreads.")
            return

        # Ensure orderbook is in ascending order
        asks = sorted(orderbook_data.get('asks'), key=lambda k: k['price'])

        # Bids should be in descending order
        bids = sorted(orderbook_data.get('bids'), key=lambda k: k['price'], reverse=True)

        self.current_round_vol_cleared = 0.0

        try:
            depth_to_check = self.spread_clearing_depth

            # Get lopsided asks
            if len(bids) >= 10:
                # Depths of bid1 to bid5, ask1 to ask5
                weighted_bids_front = self.calculate_weighted_average(bids, depth_to_check)
                weighted_bids_all = self.calculate_weighted_average(bids, 10)
                bids_ratio = weighted_bids_front/(weighted_bids_all + weighted_bids_front)

                # Clear bids if the bids are front-light
                if bids_ratio <= self.spread_clearing_threshold:
                    self.logger.debug("[AggressiveOrderClearing] weighted_bids_front is: %s, weighted_bids_all is: %s, bids_ratio is: %s" %(weighted_bids_front, weighted_bids_all, bids_ratio))
                    orders_to_clear['bids'] = self.get_orders_to_take(bids)

            # Get lopsided bids
            if len(asks) >= 10:
                weighted_asks_front = self.calculate_weighted_average(asks, depth_to_check)
                weighted_asks_all = self.calculate_weighted_average(asks, 10)
                asks_ratio = weighted_asks_front/(weighted_asks_all + weighted_asks_front)

                # Clear asks if the asks are front-light
                if asks_ratio <= self.spread_clearing_threshold:
                    self.logger.debug("[AggressiveOrderClearing] weighted_asks_front is: %s, weighted_asks_all is: %s, asks_ratio is: %s" %(weighted_asks_front, weighted_asks_all, asks_ratio))
                    orders_to_clear['asks'] = self.get_orders_to_take(asks)
        except Exception as e:
            self.logger.error("Error while trying to calculate orders. Asks is: %s, Bids is: %s. Error: %s" %(asks, bids, e))

        return orders_to_clear
            
    def get_orders_to_take(self, orders):
        """Check orders to aggressively take, given a list of bids or asks"""
        # We deem this to be a possibly suspicious order if we see more than one order <= self.aggressive * minimum order size
        
        if not orders:
            self.logger.error(f"Orderbook for bot {self.botid} is empty!")
            return 0

        small_orders = []
        i = 0
        while i < self.spread_clearing_depth and self.current_round_vol_cleared <= self.spread_clearing_max_vol_round and self.cummulative_cleared_vol < self.spread_clearing_max_vol_cummulative:
            vol = orders[i].get('volume')
            if orders[i].get('volume') < (self.spread_clearing_min_order_multiplier * self.spread_clearing_min_order_size):
                self.logger.debug("[AggressiveOrderClearing] Adding order %s to clear as its volume is less than minordersize %s * multiplier %s" %(orders[i], self.spread_clearing_min_order_size, self.spread_clearing_min_order_multiplier))
                small_orders.append(orders[i])
                self.current_round_vol_cleared += vol
                self.cummulative_cleared_vol += vol
            i += 1

        return small_orders

    def calculate_weighted_average(self, orders, depth = 10):
        """Calculate weighted average of orders, an array of {'price', 'volume'}"""
        self.logger.debug("Calculating weighted average for orders %s with depth of %s" %(orders, depth))

        # Return 0 if list is empty
        if not orders:
            self.logger.error(f"Orderbook for bot {self.botid} is empty!")
            return 0

        total_volume = 0.0
        total_price_volume = 0.0

        for i in range(depth):
            total_volume += orders[i].get('volume')
            total_price_volume += (orders[i].get('volume') * orders[i].get('price'))

        return total_price_volume/total_volume

    def update_data(self):
        return self.do_update_accout

    def tick(self):
        """Update orderbook in every tick and update account balance in every N minutes"""
        if time.time() - self.last_run_time < self.max_upd_frequency:
            time.sleep(self.sleep_time)
            return False
        else:
            self.last_run_time = time.time()
        # Setup condition to update account balance in every 300s
        self.do_update_accout = False
        if int(time.time() - self.account_ts) > config.ACCOUNT_TS: # 300s
            self.account_ts = time.time()
            self.do_update_accout = True

        if self.aggressive_spread_clearing:
            # Object containing array of bids and array of asks
            orders_to_clear = self.get_spread_to_clear()
            self.clear_orders_aggressively(orders_to_clear)

        return True

    def Go(self):
        """second version of Go, Go, Go"""
        if self.orderpriority == 'RANDOM':
            # TODO replace Ture with some kind of exception test result
            while True:
                self.tick()
                self.wash_trading_random(self.tradingpair)

        elif self.orderpriority == 'BUYFIRST':
            # TODO replace Ture with some kind of exception test result
            while True:
                self.tick()
                self.wash_trading_buy_first(self.tradingpair)

        elif self.orderpriority == 'SELLFIRST':
            # TODO replace Ture with some kind of exception test result
            while True:
                self.tick()
                self.wash_trading_sell_first(self.tradingpair)
        else:
            self.logger.error("Unknown order priority configuration value, launch washing algo failed!")
            return False


    def exit_handler(self, sig, frame):
        """proper exit handling"""
        print('exit with signal', sig)
        if platform.system() == 'Linux':
            if sig == signal.SIGHUP:
                return
        # do something
        self.exit_processing()
        raise SystemExit(0)


    def Go_run(self):
        """Go, Go, Go"""
        # testing
        endtime = time.time() + 960  #8h 28800, 16h 57600
        # tracemalloc.start(3)
        # # # # #

        if platform.system() == 'Linux':
            sig_list = [signal.SIGINT, signal.SIGTERM, signal.SIGABRT, signal.SIGHUP]
        else:
            sig_list = [signal.SIGINT, signal.SIGTERM]
        for sig in sig_list:
            signal.signal(sig, self.exit_handler)

        # TODO remove time.time() < endtime in production
        while 1 and time.time() < endtime:
            # testing
            # snapshot = tracemalloc.take_snapshot()
            # top_stats = snapshot.statistics('filename')
            # print(tracemalloc.get_traced_memory())
            # for stat in top_stats:
            #     print(stat)
            # objgraph.show_growth()
            # objgraph.show_most_common_types(limit=20)
            try:
                if self.tick():
                    self.wash_trading_run(self.tradingpair)
                # record....
                do_recording = False
                if int(time.time() - self.recording_ts) > config.RECORDING_TS: #900s
                    self.recording_ts = time.time()
                    do_recording = True

                if do_recording:
                    # self.remove_orders_over24h()
                    try:
                        self.logger.debug(f"updating pending orders")
                        self.pending_order_processing()
                    except:
                        self.logger.error(f"unable to update pending orders")
                    try:
                        self.logger.debug(f"updating trader position")
                        self.generate_trader_position()
                    except:
                        self.logger.error(f"unable to update pending orders")
                    try:
                        self.logger.debug(f"resetting priority")
                        self.reset_priority()
                    except:
                        self.logger.error(f"unable to reset priority")
                    # self.record_trader_position()

                # testing
                # objgraph.show_growth()

            except Exception as e:
                tracestr = traceback.format_exc()
                self.logger.error(tracestr)

        # record position along with time stamp before exiting
        self.exit_processing()
