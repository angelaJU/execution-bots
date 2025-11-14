from datetime import datetime
import functools
import logging
import math
import random
import re
import time
from contextlib import AbstractContextManager
import traceback
from typing import Iterable
from enum import Enum, IntEnum
from math import ceil, fabs
import json

import cachetools
from importlib import import_module

from .OrderMonitor import OrderMonitor
from . import config
from .HelmClient import HelmClient
from altonomy.core import OrderBook, client
from altonomy.core.Side import BUY, SELL, Side
from altonomy.core.exceptions import ErrorCode
from altonomy.ref_data_api.api import InstrumentDataSession, InstrumentData
import altonomy.price_server_api.api as psa
from altonomy.core.Order import Order

class Aggressiveness(IntEnum):
    UNKNOWN = 0
    LIMIT_PRICE = 1
    TICK_WORSE = 2
    ON_TOP = 3
    TICK_BETTER = 4
    MANUAL = 5
    SUPER_AGGR = 6
    TAKING = 7
    
class TradableBitMask(IntEnum):
    'Order tradable bitmask'
    ALL_GOOD = 0
    ManualOnOFF = 1 << 0
    MarketStatus = 1 << 1        # Detected from status in the callback
    MarketDataStale = 1 << 2     # Detested from the last timestamp
    PricerNotReady = 1 << 3
    ClientGwNotReady = 1 << 4
    ExchangeNotReady = 1 << 5
    AccountDisabled = 1 << 6
    MaxOrderFailure = 1 << 7
    # Business logic
    DependencyNotSatisfied = 1 << 8
    StartConditionNotMeet = 1 << 9
    CancelConditionMeet = 1 << 10
    PriceOutRange = 1 << 11

class ConditionType(IntEnum):
    NO_CONDITION = 0
    SPREAD_BIGGER_THAN = 1
    SPREAD_SMALLER_THAN = 2
    SPREAD_BIGGER_THAN_WITH_DIRECTION = 3
    SPREAD_SMALLER_THAN_WITH_DIRECTION = 4

class LegStatus(IntEnum):
    WAITING = 0
    ORDER_SUBMITTED = 1
    ORDER_PENDING = 2
    ORDER_CANCELLED = 3
    LEG_COMPLETED = 4
    ERROR = 5
    NOT_ENOUGH_BALANCE = 6
    ORDER_FAILED = 7

MIN_FORCE_BALANCE_CHECK_BACKOFF_SECONDS = 1
MAX_FORCE_BALANCE_CHECK_BACKOFF_SECONDS = 11
FORCE_BALANCE_CHECK_BACKOFF_RATE = 1.5
FAILED_ORDER_WAIT_SLEEP_SECONDS = 10
RATE_UPDATE_FREQUENCY_SECONDS = 60
CONSECUTIVE_CANCELLATION_WAITING_SECONDS = 1

    
ERR_INVALID_CONFIG_FORMAT = 'Invalid config format'
ERR_INVALID_CONFIG_PARAMETERS = 'Invalid config parameters'

ERR_LEG_BASE_AND_QUOTE_EMPTY = 'Base and Quote coins must be none empty'
ERR_LEG_ACCOUNT_ID_EMPTY = 'Account ID must be none empty'
ERR_LEG_INSTRUMENT_DATA_UNAVAILABLE = 'Cannot find ticker in instrument data service'
ERR_LEG_QTY_LESS_THAN_MIN_QTY = 'Total Qty({}) must be larger than Min Order Qty({})'
ERR_LEG_SLICE_SIZE_LESS_THAN_MIN_QTY = 'Slice Size({}) must be larger than Min Order Qty({})'
ERR_LEG_BASE_USDT_CONVERSION_UNAVAILABLE = 'Price cannot find base({}) to USDT conversion rate'
ERR_LEG_USD_USDT_CONVERSION_UNAVAILABLE = 'Price cannot find USD to USDT conversion rate'

ERR_LEG_UNABLE_TO_SET_AUTO_SIDE = 'Unable to set AUTO side'
ERR_LEG_NO_PRICE_QTY = 'Cannot decide Price/Qty'


class SpreadLeg():
    buy_price_func = {
        Aggressiveness.UNKNOWN: (lambda tob, toa, tick, multiplier: 0),
        Aggressiveness.TICK_WORSE: (lambda tob, toa, tick, multiplier: tob - tick),
        Aggressiveness.ON_TOP: (lambda tob, toa, tick, multiplier: tob),
        Aggressiveness.TICK_BETTER: (lambda tob, toa, tick, multiplier: tob + tick if toa - tob > tick else tob),
        Aggressiveness.MANUAL: (lambda tob, toa, tick, multiplier: toa - (multiplier * tick)),
        Aggressiveness.SUPER_AGGR: (lambda tob, toa, tick, multiplier: toa - tick if toa - tob > tick else toa),
        Aggressiveness.TAKING: (lambda tob, toa, tick, multiplier: toa)
    }
    sell_price_func = {
        Aggressiveness.UNKNOWN: (lambda tob, toa, tick, multiplier: 0),
        Aggressiveness.TICK_WORSE: (lambda tob, toa, tick, multiplier: toa + tick),
        Aggressiveness.ON_TOP: (lambda tob, toa, tick, multiplier: toa),
        Aggressiveness.TICK_BETTER: (lambda tob, toa, tick, multiplier: toa - tick if toa - tob > tick else toa),
        Aggressiveness.MANUAL: (lambda tob, toa, tick, multiplier: tob + (multiplier * tick)),
        Aggressiveness.SUPER_AGGR: (lambda tob, toa, tick, multiplier: tob + tick if toa - tob > tick else tob),
        Aggressiveness.TAKING: (lambda tob, toa, tick, multiplier: tob)
    }

    def __init__(self, alt_coin, qoute_coin, account_id, instrument_type, qty, slice_size, side, order_type, aggressiveness, tick_multiplier, pair_leg, primary_leg, logger, service_id, pricer, alt_client=None, position=None) -> None:
        self.alt_coin = alt_coin
        self.qoute_coin = qoute_coin
        self.account_id = account_id
        self.instrument_type = instrument_type
        self.qty : float = qty
        self.slice_size : float = slice_size
        self.auto_side = True if side =='AUTO' else False
        self.side = BUY if self.auto_side else Side(side)
        self.order_type = order_type
        self.aggressiveness = Aggressiveness[aggressiveness]
        self.tick_multiplier: float = tick_multiplier
        self.pair_leg : SpreadLeg = pair_leg
        self.primary_leg : bool = primary_leg
        self.initial_position = position
        self.remark = ''

        self.start_condition = ConditionType.NO_CONDITION
        self.start_threshold = 0.0
        self.suspend_condition = ConditionType.NO_CONDITION
        self.suspend_threshold = 0.0

        self.logger = logger
        self.service_id = service_id
        self.client = alt_client if alt_client else client(account_id=self.account_id, logger=self.logger, broadcast_chan=config.BROADCAST_CHANNEL)
        if service_id:
            self.client.service_id = service_id
        self.order_monitor = OrderMonitor(self.client, self.logger, try_cancels=25)
        self.initialise_order_monitor()

        self.orderbook = functools.partial(self.client.get_orderbook, pair=self.pair)
        self.pricer = pricer
        self.exchange_name = self.exchange_name_of(self.account_id)

        self.tob = None
        self.toa = None
        self.tob_size = None
        self.toa_size = None
        self.mid = None

        self.order_id = None
        self.instrument_data = None
        self.leverage = 1

        self.tradable_bit_mask = TradableBitMask.ALL_GOOD
        self.tradable_bit_mask |= TradableBitMask.PricerNotReady
        self.tradable_bit_mask |= TradableBitMask.MarketStatus

        self.instrument_data = None
        self.min_order_qty = self.slice_size
        
        self.base_usdt_convertion_rate = 0.0
        self.usd_usdt_convertion_rate = 0.0
        self.rate_qty_update_ts = time.time()
        
        self.order_monitor.start()

        self._last_error = None
        self.leg_status = LegStatus.WAITING

        self.last_force_rpc_balance_ts = time.time()
        self.balance_check_backoff = None

        self.continues_failed_order_count = 0
        self.continues_failed_order_count_ts = time.time()

        self.consecutive_cancel_count = 0
        self.consecutive_cancel_count_ts = time.time()
    
    def initialise_order_monitor(self):
        try:
            self.logger.info('Initializing order monitor')
            if not self.initial_position:
                return

            starting_pos = self.initial_position.get("Order Monitor Status", None)
            if not starting_pos:
                return

            self.logger.debug(f'Order Monitor Status in Redis - {starting_pos}')

            open_orders = {}
            total_dealt_by_side = {}
            total_dealt_notional_by_side = {}

            total_dealt_by_side = json.loads(starting_pos.get("total_dealt_by_side"))
            total_dealt_notional_by_side = json.loads(starting_pos.get("total_dealt_notional_by_side"))
            position_open_orders = json.loads(starting_pos.get("open_orders"))

            # Convert orders dict to Order Objects
            for key, order in position_open_orders.items():
                if order is not None:
                    open_orders[key] = Order(order)

            self.order_monitor.initialise_starting_position(
                open_orders,
                total_dealt_by_side,
                total_dealt_notional_by_side,
            )
        except Exception as e:
            self.logger.error(f'initialise_order_monitor - {e}')
            
    #this is a tempralay fix to get exchange symbol, since instrumnet service doesn't support old altonomy symbology
    def get_exchange_symbol(self):
        try:
            self.logger.info(f'exchange_name={self.exchange_name}')
            exchange = getattr(import_module(f"altonomy.exchanges.{self.exchange_name}"), self.exchange_name)('', '', logger=self.logger)
            if  self.instrument_type is not None and self.instrument_type.lower() in ["futures"] and self.exchange_name.lower() in ["bybit"]:
                exchange_symbol = self.pair
            else:
                exchange_symbol = exchange.symbol_convert_from_common(self.pair)
            self.logger.info(f'pair={self.pair}, exchange_symbol={exchange_symbol}')
            return exchange_symbol
        except Exception as e:
            self.logger.error(f'cannot convert to exchange symbol {e}')
            return self.pair

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

    def post_init(self):
        
        try:
            exchange_id = self.exchange_id_of(account_id=self.account_id)
            s = self.client.instrument_data()
            instruments = s.get_active_instruments_for_exchange(exchange_id)

            exchange_symbol = self.get_exchange_symbol()
            for ins in instruments:
                if ins.exchange_symbol == exchange_symbol:
                    self.instrument_data = ins

            if self.instrument_data is None:
                self.logger.error(f'post init - Instrument_data not found for {exchange_symbol}') 
                return

            self.logger.debug(f'post_init instrument_data - {self.instrument_data}')
            if self.is_futures_of():
                self.leverage = self.client.get_product_leverage(self.pair, account_id=self.account_id)
            self.min_order_qty = self.instrument_data.min_order_size or self.instrument_data.lot_size
        
            self.update_market_data()
            self.update_convertion_rates()   
            self.deduce_quantities()

            self.start_book_listener()

            self.logger.debug(f'{str(self)}')
        except Exception as e:
            self.logger.error(f'post init failed - {e}')    

    def start_book_listener(self):
        try:
            account = self.name_of(self.account_id)
            hc = HelmClient(endpoint=config.HELM_REPO, logger=self.logger)
            hc.start_orderbook_listener(account, self.exchange_name, self.alt_coin, self.qoute_coin)
            self.client.register_resource_usage("book", self.client.exchange_name(self.exchange_name), self.pair)
            self.logger.info(f'Register resource usage: resource_type=book, exchange=={self.exchange_name}, pair={self.pair}')
        except Exception as e:
            self.logger.error(f'Book listener startup failed - {e}')

    def update_convertion_rates(self):
        self.usd_usdt_convertion_rate = self.get_usd_conversion_rate()
        self.base_usdt_convertion_rate = self.get_base_conversion_rates()
        
    def get_usd_conversion_rate(self):
        rate = self.get_rate("USDC", "USDT")
        if rate and rate > 0:
            self.logger.info(f'USDC->USDT rate - {rate}')
            return rate

        rate = self.get_rate("USD", "USDT")
        if rate and rate > 0:
            self.logger.info(f'USD->USDT rate - {rate}')
            return rate
        
        ob = self.client.get_orderbook(pair="USDUSDT")
        if self.validate_orderbook(ob):
            rate = 0.5 * (ob.bids[0].price + ob.asks[0].price)
        
        self.logger.info(f'USDUSDT mid rate - {rate}')
        return rate

    def get_base_conversion_rates(self, retry_attempts = 0):
        try:
            rate = 0.0
            if self.instrument_data.quote_coin in ['USDT']:
                rate = self.mid
                self.logger.info(f'base_conversion USDT rate - {rate}')
            elif self.usd_usdt_convertion_rate and self.instrument_data.quote_coin in ['USD', 'USDC', 'BUSD']:
                rate = self.mid * self.usd_usdt_convertion_rate
                self.logger.info(f'base_conversion USD/USDC/BUSD rate - {rate}')
            else:
                ob = self.client.get_orderbook(pair=self.alt_coin+"USDT")
                if self.validate_orderbook(ob):
                    rate = 0.5 * (ob.bids[0].price + ob.asks[0].price)
                    self.logger.info(f'base_conversion mid rate - {rate}')
            if not (rate and rate > 0):
                if retry_attempts < 3:
                    return self.get_base_conversion_rates(retry_attempts + 1)
                rate = self.get_rate(self.alt_coin, 'USDT')
                self.logger.info(f'base_conversion get_rate - {rate}')
            return rate
        except Exception as e:
            self.logger.error(f'{e}')

    def __str__(self):
        return (
            "base=" + self.alt_coin + ", " 
            + "quote=" + self.qoute_coin + ", "
            + "qty=" + str(self.qty) + ", "
            + "slice_size=" + str(self.slice_size) + ", "
            + "pair_leg.slice_size=" + str(self.pair_leg.slice_size) + ", "
            + "remain_qty=" + str(self.remain_qty) + ", "
            + "min_order_qty=" + str(self.min_order_qty) + ", "
            + "pair_leg.min_order_qty=" + str(self.pair_leg.min_order_qty) + ", "
            + "base_usdt_convertion_rate=" + str(self.base_usdt_convertion_rate) + ", "
            + "pair_leg.base_usdt_convertion_rate=" + str(self.pair_leg.base_usdt_convertion_rate) + ", "
            + "usd_usdt_convertion_rate=" + str(self.usd_usdt_convertion_rate) + ", "
            + "pair_leg.usd_usdt_convertion_rate=" + str(self.pair_leg.usd_usdt_convertion_rate)
        )

    @property
    def usd_usdt_convertion_needed(self):
        return self.is_coin_margin or self.instrument_data.quote_coin in ['USD', 'USDC']

    def validate_leg_parameters(self):
        if self.is_futures_of():
            if self.qoute_coin == '':
                self.last_error = ERR_LEG_BASE_AND_QUOTE_EMPTY
                return False
        else:
            if self.alt_coin == '' or self.qoute_coin == '':
                self.last_error = ERR_LEG_BASE_AND_QUOTE_EMPTY
                return False

        if self.account_id == '':
            self.last_error = ERR_LEG_ACCOUNT_ID_EMPTY
            return False
        if self.instrument_data is None:
            self.last_error = ERR_LEG_INSTRUMENT_DATA_UNAVAILABLE
            return False
        if self.usd_usdt_convertion_needed and not(self.usd_usdt_convertion_rate and self.usd_usdt_convertion_rate > 0.0)  :
            self.last_error = ERR_LEG_USD_USDT_CONVERSION_UNAVAILABLE
            return False
        if not (self.base_usdt_convertion_rate and self.base_usdt_convertion_rate > 0.0):
            self.last_error = ERR_LEG_BASE_USDT_CONVERSION_UNAVAILABLE.format(self.alt_coin)
            return False
        if self.qty < self.min_order_qty:
            self.last_error = ERR_LEG_QTY_LESS_THAN_MIN_QTY.format(self.qty, self.min_order_qty)
            return False
        if self.slice_size < 0 or (self.slice_size > 0 and self.slice_size < self.min_order_qty):
            self.last_error = ERR_LEG_SLICE_SIZE_LESS_THAN_MIN_QTY.format(self.slice_size, self.min_order_qty)
            return False
        
        return True


    def deduce_quantities(self):
        if self.primary_leg:
            return
        if not(self.instrument_data and self.pair_leg.instrument_data):
            return

        pair_notional = self.pair_leg.get_notional(self.pair_leg.qty)
        self.qty = self.get_qty_for_notional(pair_notional)
        pair_slice_notional = self.pair_leg.get_notional(self.pair_leg.slice_size)
        self.slice_size = self.get_qty_for_notional(pair_slice_notional)
        self.logger.debug(
            f'Deduced Quantities - '
            f'pair_notional = {pair_notional} '
            f'pair_slice_notional = {pair_slice_notional} '
            f'self.qty = {self.qty} '
            f'self.slice_size  = {self.slice_size }')

    def get_rate(self, coin1, coin2, retry_attemps = 0):
        try:
            if not self.pricer:
                self.logger.info('pricer not available')
                return 0

            time.sleep(1)
            rate = self.pricer.get_rate(coin1, coin2)
            if rate is None and retry_attemps < 3:
                return self.get_rate(coin1, coin2, retry_attemps + 1)
            return rate
        except Exception as e:
            self.logger.error(f'get_rate - {e}')
            return 0

    @property
    def pair(self):
        return self.alt_coin + self.qoute_coin

    @functools.lru_cache(maxsize=None)
    def exchange_id_of(self, account_id):
        return self.client.get_account_config('exchange_id', account_id=account_id)

    @functools.lru_cache(maxsize=None)
    def name_of(self, account_id):
        return self.client.get_account_config('name', account_id=account_id)

    @functools.lru_cache(maxsize=None)
    def exchange_name_of(self, account_id):
        return self.client.get_account_config('exchange_name', account_id=account_id)
    
    @cachetools.cached(cache=cachetools.TTLCache(maxsize=64, ttl=60))
    def order_book_delay_threshold(self, account_id, default=0.5):
        delay_threshold = self.client.get_exchange_config(
            'book_refresh_rate', self.exchange_id_of(account_id)
        )
        if delay_threshold is None:
            self.logger.debug(
                f'book_refresh_rate for {self.name_of(account_id)} not set, '
                f'defaulting to {delay_threshold}'
            )
            return default
        else:
            return float(delay_threshold)

    def validate_orderbook(self, ob):
        if len(ob.bids) == 0 or len(ob.asks) == 0:
            # ignore empty books, probably an error
            self.tradable_bit_mask |= TradableBitMask.PricerNotReady
            return False
        if not ob.timestamp:
            # ignore books without timestamps, probably an error
            self.tradable_bit_mask |= TradableBitMask.MarketStatus
            self.logger.error(f'Ignoring books without timestamp')
            return False
        if time.time() - float(ob.timestamp) > config.BROKER_ORDERBOOK_REFRESH_MAX_TIME:
            self.logger.error(f'staled market data - timestamp={ob.timestamp}')
            self.tradable_bit_mask |= TradableBitMask.MarketDataStale
            return False
        
        delay_threshold = self.order_book_delay_threshold(self.account_id)
        if (
            ob.timestamp * 1000
            < max(
                (
                    o.update_time
                    for o in self.order_monitor.orders.values()
                    if o.account_id == self.account_id and o.update_time
                ),
                default=0,
            )
            + delay_threshold
        ):
            self.logger.debug(f'waiting for order book update for {self.account_id}, current ob timestamp={ob.timestamp}')
            self.tradable_bit_mask |= TradableBitMask.MarketDataStale
            return False
        return True

    def update_market_data(self):
        ob = self.orderbook()
        
        if not self.validate_orderbook(ob):
            return

        self.logger.debug(f'obtained orderbook for {self.account_id}')
        self.tob = ob.bids[0].price
        self.toa = ob.asks[0].price
        self.mid = 0.5 * (self.tob + self.toa)
        self.update_min_qty(self.mid)
        self.tob_size = ob.bids[0].amount
        self.toa_size = ob.asks[0].amount    

        if self.tradable_bit_mask & TradableBitMask.PricerNotReady > 0:
            self.tradable_bit_mask &= ~TradableBitMask.PricerNotReady
        if self.tradable_bit_mask & TradableBitMask.MarketStatus > 0:
            self.tradable_bit_mask &= ~TradableBitMask.MarketStatus
        if self.tradable_bit_mask & TradableBitMask.MarketDataStale > 0:
            self.tradable_bit_mask &= ~TradableBitMask.MarketDataStale

        self.logger.debug(f'tob={self.tob}, toa={self.toa}, timestamp={ob.timestamp}')

    def update_min_qty(self, price):
        if self.is_coin_margin:
            self.min_order_qty = 1.0
        elif self.instrument_data.min_order_notional:
            self.min_order_qty = self.instrument_data.min_order_notional / price + 1e-10
            self.min_order_qty = round(self.min_order_qty, self.instrument_data.size_precision)
            self.logger.debug(f'update_min_qty - with order notional = {self.min_order_qty}')
            if self.min_order_qty == 0.0 or self.min_order_qty < self.instrument_data.min_order_size + 1e-10:
                self.min_order_qty = round(self.instrument_data.min_order_size, self.instrument_data.size_precision)
                self.logger.debug(f'update_min_qty - with min order size = {self.min_order_qty} ')
        else:
            pass

    @property
    def is_coin_margin(self):
        if self.is_futures_of():
            alto_symbol = self.instrument_data.altonomy_symbol.split('/')
            return len(alto_symbol) >= 3 and alto_symbol[2] == 'COIN'
        return False

    @functools.lru_cache(maxsize=None)
    def is_futures_of(self):
        return self.instrument_type == "FUTURES"

    @functools.lru_cache(maxsize=None)
    def dual_side_position_of(self):
        dual_side_position = self.client.get_account_config('dual_side_position', account_id=self.account_id)
        return eval(dual_side_position) if dual_side_position else False

    def need_requote(self, new_price, new_qty):
        order = self.order_monitor.orders[self.order_id] if self.order_id in self.order_monitor.orders else None  
        if order is None:
            return False

        diff = fabs(new_price - order.price)
        if diff >= self.instrument_data.tick_size:
            return True
    
        if fabs(new_qty - order.amount) / order.amount > 0.5:
            return True
        
        return False


    def get_notional(self, qty):
        if self.is_coin_margin:
            return qty * self.instrument_data.contract_size * self.usd_usdt_convertion_rate
        return qty * self.base_usdt_convertion_rate

    def get_traded_notional(self, side):
        side = side if self.is_opening_position() else self.contra_side
        return self.get_notional(self.order_monitor.get_total_dealt_by_side(str(side))) 

    def get_average_price(self, side):
        side = side if self.is_opening_position() else self.contra_side
        price = self.order_monitor.get_average_price(str(side))
        if self.instrument_data:
            return round(price, self.instrument_data.price_precision)
        return -1

    @property
    def average_price(self):
        if self.auto_side:
            return f'Buy={self.get_average_price(BUY)} / Sell={self.get_average_price(SELL)}'

        return self.get_average_price(self.side)

    @property
    def contra_side(self):
        if self.side == BUY:
            return SELL
        elif self.side == SELL:
            return BUY
        else:
            raise ValueError

    def check_dependancy(self):
        notional = self.get_traded_notional(self.side)
        dep_notional = self.pair_leg.get_traded_notional(self.contra_side if self.auto_side else self.pair_leg.side)
        self.logger.debug(f'min_order_qty pair_leg.min_order_qty {self.min_order_qty} {self.pair_leg.min_order_qty}')
        if self.primary_leg:
            dep_min_notional = self.pair_leg.get_notional(self.pair_leg.min_order_qty)

            self.logger.debug(f'primary - notional={notional}, dep_notional={dep_notional}, dep_min_notional={dep_min_notional}')
            if notional - dep_notional >= dep_min_notional - 1e-10:
                return False
        else:
            min_notional = self.get_notional(self.min_order_qty)

            self.logger.debug(f'pair - notional={notional}, dep_notional={dep_notional}, min_notional={min_notional}')
            if dep_notional - notional < min_notional - 1e-10:
                return False

        return True

    def is_market_data_good(self):
        if self.tradable_bit_mask & TradableBitMask.PricerNotReady == 0 and \
                self.tradable_bit_mask & TradableBitMask.MarketStatus == 0:
            return True
        return False

    def check_spread_condition(self):
        start_flag = True
        suspend_flag = False

        if not self.primary_leg:
            return start_flag, suspend_flag

        if (self.start_condition == ConditionType.NO_CONDITION and \
                self.suspend_condition == ConditionType.NO_CONDITION):      
            return start_flag, suspend_flag
        
        if not self.pair_leg.is_market_data_good():
            self.logger.error('pair leg market data is not available')
            return not start_flag, not suspend_flag

        if self.side == BUY:
            # Use making price first
            ref_price = self.tob
            if self.aggressiveness >= Aggressiveness.SUPER_AGGR:
                ref_price = self.toa
        else:
            ref_price = self.toa
            if self.aggressiveness >= Aggressiveness.SUPER_AGGR:
                ref_price = self.tob
                
        if self.pair_leg.side == BUY:
                # Use making price first
            pair_ref_price = self.pair_leg.tob
            if self.pair_leg.aggressiveness >= Aggressiveness.SUPER_AGGR:
                pair_ref_price = self.pair_leg.toa
        else:
            pair_ref_price = self.pair_leg.toa
            if self.pair_leg.aggressiveness >= Aggressiveness.SUPER_AGGR:
                pair_ref_price = self.pair_leg.tob
        
        abs_price_diff = fabs(pair_ref_price - ref_price)
        price_diff = ref_price - pair_ref_price if self.side == SELL else pair_ref_price - ref_price
        price_diff_rev = pair_ref_price - ref_price if self.side == SELL else ref_price - pair_ref_price
        

        self.logger.debug(f'price diff = {price_diff}')
        if self.start_condition != ConditionType.NO_CONDITION :
            if self.start_condition == ConditionType.SPREAD_BIGGER_THAN:
                start_flag = (abs_price_diff > self.start_threshold)
            elif self.start_condition == ConditionType.SPREAD_SMALLER_THAN:
                start_flag = (abs_price_diff < self.start_threshold)
            elif self.start_condition == ConditionType.SPREAD_BIGGER_THAN_WITH_DIRECTION:
                start_flag = (price_diff > self.start_threshold)
            elif self.start_condition == ConditionType.SPREAD_SMALLER_THAN_WITH_DIRECTION:
                start_flag = (price_diff_rev < self.start_threshold)
            
        if self.suspend_condition != ConditionType.NO_CONDITION:
            if self.suspend_condition == ConditionType.SPREAD_BIGGER_THAN:
                suspend_flag = (abs_price_diff > self.suspend_threshold)
            elif self.suspend_condition == ConditionType.SPREAD_SMALLER_THAN:
                suspend_flag = (abs_price_diff < self.suspend_threshold)
            elif self.suspend_condition == ConditionType.SPREAD_BIGGER_THAN_WITH_DIRECTION:
                suspend_flag = (price_diff > self.start_threshold)
            elif self.suspend_condition == ConditionType.SPREAD_SMALLER_THAN_WITH_DIRECTION:
                suspend_flag = (price_diff_rev < self.start_threshold)
                
        return start_flag, suspend_flag 

    def check_dependancy_and_condition(self):
        dependancy_flag = self.check_dependancy()
        if not dependancy_flag:
            self.tradable_bit_mask |= TradableBitMask.DependencyNotSatisfied
        elif self.tradable_bit_mask & TradableBitMask.DependencyNotSatisfied > 0:
            self.tradable_bit_mask &= ~TradableBitMask.DependencyNotSatisfied

        start_condition = False 
        suspend_condition = True

        if self.is_market_data_good():
            start_condition, suspend_condition = self.check_spread_condition()
            if not start_condition:
                self.tradable_bit_mask |= TradableBitMask.StartConditionNotMeet
            elif self.tradable_bit_mask & TradableBitMask.StartConditionNotMeet > 0:
                self.tradable_bit_mask &= ~TradableBitMask.StartConditionNotMeet
                
            if suspend_condition:
                self.tradable_bit_mask |= TradableBitMask.CancelConditionMeet
            elif self.tradable_bit_mask & TradableBitMask.CancelConditionMeet > 0:
                self.tradable_bit_mask &= ~TradableBitMask.CancelConditionMeet
            

        return (dependancy_flag and start_condition and not suspend_condition)

    def get_price(self):
        if self.side == BUY:
            return self.buy_price_func[self.aggressiveness](self.tob, self.toa, self.instrument_data.tick_size, self.tick_multiplier)
        else:
            return self.sell_price_func[self.aggressiveness](self.tob, self.toa, self.instrument_data.tick_size, self.tick_multiplier)

    def get_qty_for_notional(self, notional):
        qty = 0.0
        if self.is_coin_margin:
            qty = notional / (self.instrument_data.contract_size * self.usd_usdt_convertion_rate) if self.usd_usdt_convertion_rate and self.usd_usdt_convertion_rate > 0.0 else 0.0
        else:
            qty = notional / self.base_usdt_convertion_rate if self.base_usdt_convertion_rate and self.base_usdt_convertion_rate > 0.0 else 0.0

        self.logger.debug(f'notional={notional}, qty={qty} ')
        return round(qty, self.instrument_data.size_precision)

    def get_delta_offset(self):
        notional = self.get_traded_notional(self.side)
        dep_notional = self.pair_leg.get_traded_notional(self.contra_side if self.auto_side else self.pair_leg.side)

        self.logger.debug(f'notional={notional}, dep_notional={dep_notional}')
        delta = max(dep_notional - notional, 0.0)
        return self.get_qty_for_notional(delta)

    @property
    def remain_qty(self):
        return self.qty - self.order_monitor.total_dealt

    def get_dep_order_price_qty(self):
        price = self.get_price()
        price = round(price, self.instrument_data.price_precision)
        if self.primary_leg:
            remain_qty = max(0.0, self.remain_qty)
            self.logger.debug(f'remaining qty = {remain_qty} slice_size = {self.slice_size}')
            if self.slice_size > 1e-10:
                qty = min(self.slice_size, remain_qty)
            else:
                qty = remain_qty
        else:
            qty = self.get_delta_offset()

        self.logger.debug(f'price = {price} qty = {qty}')
        return price, qty

    def is_opening_position(self) -> bool:
        if self.order_type in ['LIMIT_CLOSE']:
            return False
        return True

    def balance_can_meet_order(self, order_price, order_amount) -> bool:
        """ check if account has enough balance to place the order """
        try:
            if self.balance_check_backoff and time.time() - self.last_force_rpc_balance_ts > self.balance_check_backoff:
                self.logger.info(f'Force rpc balance check, balance_check_backoff={self.balance_check_backoff}')
                balance = self.client.get_account_balance(self.account_id, force_rpc=True)
                self.balance_check_backoff = min(MAX_FORCE_BALANCE_CHECK_BACKOFF_SECONDS, FORCE_BALANCE_CHECK_BACKOFF_RATE * self.balance_check_backoff)
                self.last_force_rpc_balance_ts = time.time()
            else:
                balance = self.client.get_account_balance(self.account_id, force_rpc=False)

            # Futures
            if self.is_futures_of():
                if self.instrument_data is None:
                    return False

                if self.leverage is None:
                    self.leverage = self.client.get_product_leverage(self.pair, account_id=self.account_id)
                    return False

                if self.is_coin_margin:
                    margin_balance = balance[self.instrument_data.settlement_asset].available * int(self.leverage) * order_price / float(self.instrument_data.contract_size)
                else:
                    margin_balance = balance[self.instrument_data.settlement_asset].available * int(self.leverage) / order_price
                
                self.logger.debug(f'margin balance: {margin_balance} leverage: {self.leverage}')

                if self.dual_side_position_of():
                    self.logger.debug(f'futures - dual side position:True')
                    if self.side == SELL:
                        if self.is_opening_position():
                            start_bal = margin_balance   
                        else:
                            start_bal = balance[f'{self.pair} Long'].available
                    elif self.side == BUY:
                        if self.is_opening_position():
                            start_bal = margin_balance
                        else:
                            start_bal = balance[f'{self.pair} Short'].available
                    else:
                        raise ValueError
                else:
                    self.logger.debug(f'futures - dual side position:False')
                    if self.side == SELL:
                        start_bal = balance[f'{self.pair} Long'].available + margin_balance
                    elif self.side == BUY:
                        start_bal = balance[f'{self.pair} Short'].available + margin_balance            
                    else:
                        raise ValueError
            # Spots
            else:
                if self.side == SELL:
                    start_bal = balance[self.alt_coin].available
                elif self.side == BUY:
                    start_bal = balance[self.qoute_coin].available / order_price
                else:
                    raise ValueError

            self.logger.debug(f'start_bal: {start_bal} order_amt: {order_amount} order_price={order_price}')
            
            is_enough_balance = start_bal >= order_amount

            if is_enough_balance:
                self.balance_check_backoff = None
            else:
                if self.balance_check_backoff is None:
                    self.balance_check_backoff = MIN_FORCE_BALANCE_CHECK_BACKOFF_SECONDS

            return is_enough_balance
        except Exception as e:
            self.logger.error(f'Error while getting balance - {e}')
            return False

    def get_order_type(self):
        if self.side == BUY:
            return f'BUY_{self.order_type}' if self.is_opening_position() else f'SELL_{self.order_type}'
        if self.side == SELL:
            return f'SELL_{self.order_type}' if self.is_opening_position() else f'BUY_{self.order_type}'

    def send_order(self, price, size, *args, **kwargs):
        func = self.client.buy if self.side == BUY else self.client.sell
        self.logger.debug(f'sending {self.side} order {size}@{price} for {self.pair}')
        return func(
            self.pair,
            price=price,
            size=size,
            order_type=self.get_order_type(),
            remark=self.remark,
            *args,
            **kwargs)

    def set_auto_side(self):
        if not self.auto_side:
            return True

        if not self.is_market_data_good() or not self.pair_leg.is_market_data_good():
            self.logger.error('Market data not available')
            return False
        
        if self.mid > self.pair_leg.mid:
            self.side = SELL

        if self.mid < self.pair_leg.mid:
            self.side = BUY

        if self.mid == self.pair_leg.mid:
            self.side = BUY if self.primary_leg else SELL

        self.logger.debug(f'side is deduced to be as {self.side}, mid={self.mid}, pair_mid={self.pair_leg.mid}')
        return True

    @property
    def balance(self):
        balance = self.client.get_account_balance(self.account_id, force_rpc=False)
        
        try:
            if self.is_futures_of():
                return {
                    self.instrument_data.settlement_asset : balance[self.instrument_data.settlement_asset].available,
                    f'{self.pair} Long' : balance[f'{self.pair} Long'].available,
                    f'{self.pair} Short' : balance[f'{self.pair} Short'].available
                }
            else:
                return {
                    self.alt_coin : balance[self.alt_coin].available,
                    self.qoute_coin : balance[self.qoute_coin].available
                }
        except Exception as e:
            self.logger.error(f'balance - {e}')
            return {}            


    @property
    def position(self):
        return {
            "Account Id" : self.account_id,
            "Sort Key" : self.primary_leg,
            "Details" : {
                "Account" : self.name_of(self.account_id),
                "Ticker" : self.pair,
                "Side" : "AUTO" if self.auto_side else str(self.side),
            },
            "Balance" : self.balance,
            "Progress" : {
                "Total Qty" : self.qty,
                "Remaining Qty" : self.remain_qty,
                "VWAP" : self.average_price
            },
            "Status" : {
                "Status" : self.leg_status.name,
                "Reason" : self.reason
            },
            "Order Monitor Status" : {
                "open_orders" : json.dumps(self.order_monitor.open_orders),
                "total_dealt_by_side" : json.dumps(self.order_monitor.total_dealt_by_side),
                "total_dealt_notional_by_side" : json.dumps(self.order_monitor.total_dealt_notional_by_side)
            }
        }

    @property
    def reason(self):
        if self.last_error is not None:
            return self.last_error
        
        if self.leg_status == LegStatus.WAITING:
            for bit in TradableBitMask:
                if self.tradable_bit_mask & bit > 0:
                    return f'{bit.name}'
        if self.leg_status == LegStatus.ORDER_FAILED:
            if self.order_id in self.order_monitor.failed_orders:
                error_code = self.order_monitor.failed_orders[self.order_id].reason
                if error_code in ErrorCode.code_reason:
                    return ErrorCode.code_reason[error_code]
                else:
                    error_code
        return ''

    def on_process_exit(self):
        for order_id, order in self.order_monitor.open_orders.items():
            self.logger.info(f'bot exiting, cancelling order {order_id}: {order}')
            self.client.cancel(order_id=order_id)
        self.order_monitor.stop()
        self.client.deregister_resource_usage("book", self.client.exchange_name(self.exchange_name), self.pair)
        self.logger.info(f'Deregister resource usage: resource_type=book, exchange=={self.exchange_name}, pair={self.pair}')

    @property
    def last_error(self):
        return self._last_error

    @last_error.setter
    def last_error(self, error):
        self._last_error = error
        if error is not None:
            self.leg_status = LegStatus.ERROR
            self.logger.error(error)

    def execute(self):
        self.last_error = None

        if self.remain_qty == 0.0 or self.remain_qty < self.min_order_qty:
            self.leg_status = LegStatus.LEG_COMPLETED
            self.logger.info('leg completed')
            return

        # Error handling
        if self.continues_failed_order_count > 0 and \
            time.time() - self.continues_failed_order_count_ts < min(self.continues_failed_order_count, FAILED_ORDER_WAIT_SLEEP_SECONDS):
            if self.order_id in self.order_monitor.failed_orders:
                self.leg_status = LegStatus.ORDER_FAILED
                return

        if self.order_id in self.order_monitor.failed_orders:
            order = self.order_monitor.failed_orders[self.order_id]
            self.logger.info(f'order failed, {order}')
            self.continues_failed_order_count += 1
            self.continues_failed_order_count_ts = time.time()
            self.leg_status = LegStatus.ORDER_FAILED
        else:
            self.continues_failed_order_count = 0
        
        self.logger.debug(f'updating market data')
        self.update_market_data()
        self.pair_leg.update_market_data()

        self.logger.debug(f'checking auto side')
        if not self.set_auto_side():
            self.last_error = ERR_LEG_UNABLE_TO_SET_AUTO_SIDE
            return

        order_pending = self.order_monitor.pending > 0.0
        self.logger.debug(f'Leg - {str(self)}')
        self.logger.debug(f'position - {self.position}')

        new_price, new_qty = 0.0, 0.0
        if self.check_dependancy_and_condition():
            new_price, new_qty = self.get_dep_order_price_qty()

        self.logger.debug(f'new_price, new_qty {new_price} {new_qty}')
        if self.tradable_bit_mask != TradableBitMask.ALL_GOOD:
            self.leg_status = LegStatus.WAITING
            self.logger.info(f'trading conditions did not meet, tradable_bit_mask={self.tradable_bit_mask}')
            return

        if new_price == 0.0 or new_qty == 0.0:
            self.last_error = ERR_LEG_NO_PRICE_QTY
            return

        if order_pending:
            self.logger.debug('order pending')
            self.leg_status = LegStatus.ORDER_PENDING
            if self.need_requote(new_price, new_qty):
                self.logger.debug('need requote')
                
                if self.consecutive_cancel_count > 0 and time.time() - self.consecutive_cancel_count_ts < CONSECUTIVE_CANCELLATION_WAITING_SECONDS:
                    self.logger.info(f'Consecutive order cancellations - {self.consecutive_cancel_count}')
                    return
                
                self.logger.info(f'Cancelling order id {self.order_id} due to requote')
                self.client.cancel(order_id=self.order_id)
                self.consecutive_cancel_count += 1
                self.consecutive_cancel_count_ts = time.time()
                self.leg_status = LegStatus.ORDER_CANCELLED
            return

        self.consecutive_cancel_count = 0
        self.logger.info(f'No pending orders. Checking Balance.')
        if not self.balance_can_meet_order(new_price, new_qty):
            self.leg_status = LegStatus.NOT_ENOUGH_BALANCE
            self.logger.info('not enough balance')
            return
        
        self.order_id = self.send_order(new_price, new_qty)
        self.order_monitor.add(self.order_id)
        self.logger.debug(f'Added to order monitor order_id = {self.order_id}')
        self.leg_status = LegStatus.ORDER_SUBMITTED

        #ABOTS-189: update convertion rate is disabled for  now. Needs to be reevaluated.
        # if time.time() - self.rate_qty_update_ts > RATE_UPDATE_FREQUENCY_SECONDS:
        #     self.rate_qty_update_ts = time.time()
        #     self.update_convertion_rates()
        #     self.deduce_quantities()
        
    
class PairTradingBot(AbstractContextManager):
    def __init__(
        self,
        account_ids: Iterable,
        base,
        quote,
        bot_id,
        config,
        *,
        logger=None,
        service_id=None,
        primary_client=None,
        pair_client=None,
        pricer=None
    ):
        self.logger = logger or logging.getLogger()
        self.legs : dict = {}
        self.altcoin = base
        self.quotecoin = quote
        self.account_id = account_ids[0]
        self.bot_id = bot_id

        self.client = client()
        self.primary_client = primary_client
        self.pair_client = pair_client
        self.service_id = service_id
        self.pricer = pricer
        self.set_pricer()
        self.config = config
        self.config_error = None
        self.last_error = None
        self.delay = 2

    def get_position(self, is_primary):
        try:
            position = self.client.get(f'{config.BOT_OUTPUT_REDIS_KEY}:{self.bot_id}:position')

            if position is None:
                return None

            leg_positions = position.get("Account Operations", [])
            if isinstance(leg_positions, str):
                leg_positions = json.loads(leg_positions)

            for pos in leg_positions:
                if pos['Sort Key'] == is_primary:
                    return pos
        except Exception as e:
            self.logger.error(f'get_position - {e}')
            return None
        
        return None

    @property
    def config(self):
        primary_leg = self.legs['primary']
        pair_leg = self.legs['pair']
        
        return {
            "instrument_type": primary_leg.instrument_type,
            "quantity": primary_leg.qty,
            "slice_size": primary_leg.slice_size,
            "side": str(primary_leg.side).upper(),
            "order_type": primary_leg.order_type,
            "distance_to_tob": primary_leg.aggressiveness.name,
            "tick_multiplier": primary_leg.tick_multiplier,
            'remark': primary_leg.remark,
            "pair_leg": json.dumps({
                "instrument_type": pair_leg.instrument_type,
                "account_id": pair_leg.account_id,
                "base": pair_leg.alt_coin,
                "quote": pair_leg.qoute_coin,
                "side": str(pair_leg.side).upper(),
                "order_type": pair_leg.order_type,
                "distance_to_tob": pair_leg.aggressiveness.name,
                "tick_multiplier": pair_leg.tick_multiplier
            }),
            "spread_params": json.dumps({
                "start_if": primary_leg.start_condition.name,
                "start_threshold": primary_leg.start_threshold,
                "suspend_if": primary_leg.suspend_condition.name,
                "suspend_threshold": primary_leg.suspend_threshold
            })
        }

    def set_pricer(self):
        if self.pricer:
            return
        try:
            self.pricer = psa.PriceServerSession()
        except Exception as e:
            self.pricer = None
            self.logger.error(f'set_pricer - {e}')

    @config.setter
    def config(self, config: dict):
        # primary leg
        self.logger.debug(f'applying config {config}')
        self.config_error = None

        try:
            if 'primary' in self.legs:
                primary = self.legs['primary']
                primary.instrument_type = config.get('instrument_type', 'SPOT')
                primary.qty = float(config.get('quantity'))
                primary.slice_size = float(config.get('slice_size'))
                primary.aggressiveness = Aggressiveness[config.get('distance_to_tob')]
                primary.tick_multiplier = float(config.get('tick_multiplier', "1"))
                primary.order_type = config.get('order_type')
            else:
                primary = SpreadLeg(self.altcoin, self.quotecoin, self.account_id, 
                                        config.get('instrument_type', 'SPOT'), 
                                        float(config.get('quantity') or 0), 
                                        float(config.get('slice_size') if config.get('slice_size') is not None else -1),
                                        config.get('side'), config.get('order_type'), config.get('distance_to_tob'),
                                        float(config.get('tick_multiplier', "1")),
                                        None, True, self.logger, self.service_id, self.pricer,
                                        alt_client = self.primary_client if self.primary_client else None, position=self.get_position(True))
                self.legs['primary'] = primary
            
            primary.post_init()
            
            pair_config = config.get('pair_leg')
            pair_config = json.loads(pair_config.replace("\'", "\"")) if isinstance(pair_config, str) else pair_config
            #pair leg
            if 'pair' in self.legs:
                pair = self.legs['pair']
                pair.instrument_type = config.get('instrument_type', 'SPOT')
                pair.order_type = pair_config.get('order_type')
                pair.tick_multiplier = float(pair_config.get('tick_multiplier', "1"))
                pair.aggressiveness = Aggressiveness[pair_config.get('distance_to_tob')]
            else:
                pair = config.get('pair_leg')
                pair = SpreadLeg(pair_config.get('base') or '', pair_config.get('quote') or '', pair_config.get('account_id') or '',
                                        pair_config.get('instrument_type', 'SPOT'), 0.0, 0.0,
                                        pair_config.get('side'), pair_config.get('order_type'), pair_config.get('distance_to_tob'),
                                        float(pair_config.get('tick_multiplier', "1")),
                                        primary, False, self.logger, self.service_id, self.pricer,
                                        alt_client = self.pair_client if self.pair_client else None, position=self.get_position(False))
                primary.pair_leg = pair
                self.legs['pair'] = pair

            pair.post_init()

            # update remark
            primary.update_remark(config.get('remark', ''))
            pair.update_remark(config.get('remark', ''))

            #spread parameters
            spread_params = config.get('spread_params')
            spread_params = json.loads(spread_params.replace("\'", "\"")) if isinstance(spread_params, str) else spread_params
            for _, leg in self.legs.items():
                leg.start_condition = ConditionType[spread_params.get('start_if')]
                leg.start_threshold = float(spread_params.get('start_threshold'))
                leg.suspend_condition = ConditionType[spread_params.get('suspend_if')]
                leg.suspend_threshold = float(spread_params.get('suspend_threshold'))
        except Exception as e:
            self.config_error = e
            self.logger.error('error when setting config')
            self.logger.error(traceback.format_exc())

    def validate_config_parameters(self):
        try:
            valid = True
            for _, leg in self.legs.items():
                valid &= leg.validate_leg_parameters()
            return valid
        except:
            self.logger.error(traceback.format_exc())
            return False

    def get_bot_status(self, bot_completed):
        if self.last_error:
            return self.last_error
        if bot_completed:
            return 'COMPLETED'
        
        return 'RUNNING'

    @property
    def position(self):
        account_positions = []
        bot_completed = True
        for _, leg in self.legs.items():
            account_positions.append(leg.position)
            bot_completed &= (leg.leg_status == LegStatus.LEG_COMPLETED)

        pos = {
            "Status" : self.get_bot_status(bot_completed),
            "Overall Progress" : {},
            "Account Operations" : json.dumps(account_positions)
        }
        self.logger.debug(pos)
        return pos
        
    def run(self):

        self.last_error = None

        if self.config_error:
            self.last_error = ERR_INVALID_CONFIG_FORMAT
            self.logger.error(f'invalid config due to error when setting config')
            time.sleep(self.delay)
            return
        
        if not self.validate_config_parameters():
            self.last_error = ERR_INVALID_CONFIG_PARAMETERS
            self.logger.error(f'not running due to invalid config parametes {self.config}')
            time.sleep(self.delay)
            return

        for name, leg in self.legs.items():
            self.logger.debug(f"running for {name} ")
            self.logger.debug('-----------------------')
            leg.execute()
        
    def __enter__(self):
        # self.order_monitor.start()
        return self

    def __exit__(
        self, exc_type, exc_value, traceback,
    ):
        for name, leg in self.legs.items():
            self.logger.info(f'{name} leg cleanup on bot exit')
            leg.on_process_exit()
        self.logger.info(
            f'bot exiting'
        )
