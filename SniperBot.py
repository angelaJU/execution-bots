from datetime import datetime
import functools
import logging
import math
import random
import json
import re
import time
from contextlib import AbstractContextManager
import traceback
from typing import Iterable
from altonomy.core.Order import Order
import operator

import cachetools

from .OrderMonitor import OrderMonitor
from altonomy.core import OrderBook, client
from altonomy.core.Side import BUY, SELL, Side
from altonomy.ref_data_api.api import InstrumentDataSession, InstrumentData

from . import config
from .HelmClient import HelmClient


class SniperBot(AbstractContextManager):
    def __init__(
        self,
        account_ids: Iterable,
        base,
        quote,
        bot_id,
        config,
        *,
        service_id=None,
        alt_client=None,
        logger=None,
    ):
        self._total_amount = 0
        self.logger = logger or logging.getLogger()
        self.accounts = set(account_ids)
        self.bot_id = bot_id
        self.max_price = math.inf
        self.min_price = 0
        self.client = alt_client or client(logger=self.logger)
        if service_id:
            self.client.service_id = service_id
        self.ref_client = client(logger=self.logger)
        self._base = ''
        self._quote = ''
        self.remark = ''
        self._large_order_threshold = None
        self._cumulative_order_threshold = None
        self.trigger_condition = ' '
        self.stop_condition = ' '
        self.base = base
        self.quote = quote
        self.config = config
        self.service_id = None
        self.side = None
        self.order_type = 'LIMIT'
        self._max_slippage_threshold = None
        self.config_error = None
        self.order_monitor = OrderMonitor(self.client, self.logger, try_cancels=25)
        self.delay = 2
        self.instrument_data = {}
        self.leverages = {}
        self.load_positions()
        self.load_futures_data()
        self.start_book_listener()
        self.logger.info(f'SniperBot started on accounts {account_ids} with bot_id {self.bot_id}')

    def load_positions(self):
        try:
            info = self.client.redis.hgetall(f'output:AltonoAPLBots:{self.bot_id}:position')
            if info == {}:
                self.logger.info(f'No position data found at startup')
            else:
                self.logger.info(f"Loaded positions data from Redis : {info}")
                self.total_amount = info['total']
                position_open_orders = json.loads(info['open_orders'])
                position_completed_orders = json.loads(info['completed_orders'])

                for key, order in position_open_orders.items():
                    if order is not None:
                        self.order_monitor.open_orders[key] = Order(order)

                for key, order in position_completed_orders.items():
                    if order is not None:
                        self.order_monitor.completed_orders[key] = Order(order)

                self.logger.info(f'open_orders = {self.order_monitor.open_orders}')
                self.logger.info(f'completed_orders = {self.order_monitor.completed_orders}')
                self.logger.info(f'dealt_price = {self.order_monitor.dealt_price}')
                self.logger.info(f'dealt = {self.order_monitor.dealt}')
                self.logger.info(f'total_amount = {self.total_amount}')
        except Exception as e:
            self.logger.error(f'load_positions failed - {e}')

    def validate_ref_orderbook(self, ob):
        if len(ob.bids) == 0 or len(ob.asks) == 0:
            # ignore empty books, probably an error
            self.logger.error('ref_orderbook - empty book')
            return False
        if not ob.timestamp:
            # ignore books without timestamps, probably an error
            self.logger.error('ref_orderbook - book without timestamps')
            return False
        if time.time() - float(ob.timestamp) > (
                config.REFERENCE_ORDERBOOK_REFRESH_MAX_TIME):
            self.logger.error(
                f'ref_orderbook - staled market data - {ob.timestamp}')
            return False
        return True

    def get_reference_price(self, ref_pair, ref_exchange):
        ref_price = None
        try:
            ob = self.ref_client.get_orderbook(
                pair=ref_pair, exchange=ref_exchange)
            if not self.validate_ref_orderbook(ob):
                return ref_price

            self.logger.debug(
                f'Got reference orderbook for {ref_pair} - {ref_exchange}')
            ref_price = ob.get(
                ref_pair, {}).get(
                    {"BUY": "asks", "SELL": "bids"}.get(str(self.side).upper()),
                    [{}])[0].get("price", 0)
        except Exception as e:
            self.logger.error(f'Error in get_reference_price - {e}')

        self.logger.debug(f'ref_price = {ref_price}')
        return ref_price

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

    def get_coin_tradable_balance(self, currency, account_id):
        """ Gte account balance for given coin """
        balance = self.client.get_account_balance(
                    account_id, force_rpc=False)
        if not balance or currency not in balance.keys():
            self.logger.info(f'No Balance found for - {currency}')
            return -1.0
        return balance[currency].get('available', 0.0)

    def check_stop_condition(self, account_id):
        if self.stop_condition != ' ':
            try:
                stop_asset, stop_direction, stop_value = \
                    [value.strip() for value in self.stop_condition.split(";")]
                available_balance = self.get_coin_tradable_balance(
                    stop_asset, account_id)
                self.logger.info(
                    f"Evaluating stop condition {stop_asset} "
                    f"{available_balance} {stop_direction} "
                    f"{float(stop_value)} ")
                if getattr(operator, stop_direction)(
                        available_balance, float(stop_value)):
                    self.logger.info(
                        f"stop condition {available_balance} {stop_direction} "
                        f"{stop_value} has been met ")
                    return True
            except Exception as e:
                self.logger.debug(f"failed to evaluation stop condition due to {e}")
        return False

    def check_trigger_condition(self):
        if self.trigger_condition != ' ':
            try:
                ref_exchange, ref_pair, trigger_direction, trigger_price = \
                    [value.strip() for value in self.trigger_condition.split(
                        ";")]
                ref_exchange = self.client.exchange_name(
                    ref_exchange.capitalize())
                ref_price = self.get_reference_price(ref_pair, ref_exchange)
                self.logger.info(
                    f"Evaluating trigger condition {ref_price} "
                    f"{trigger_direction} {float(trigger_price)} ")
                if not ref_price or not getattr(operator, trigger_direction)(
                        ref_price, float(trigger_price)):
                    self.logger.info(
                        f"trigger condition {ref_price} {trigger_direction} "
                        f"{trigger_price} is not met")
                    return False
            except Exception as e:
                self.logger.debug(
                    f"failed to evaluation trigger condition due to {e}")
                return False
        return True

    def start_book_listener(self):
        try:
            for account_id in self.accounts:
                account = self.name_of(account_id)
                exchange_name = self.exchange_name_of(account_id)
                hc = HelmClient(endpoint=config.HELM_REPO, logger=self.logger)
                hc.start_orderbook_listener(account, exchange_name, self.base, self.quote)
                self.client.register_resource_usage("book", self.client.exchange_name(exchange_name), self.pair)
                self.logger.info(f'Register resource usage: resource_type=book, exchange=={exchange_name}, pair={self.pair}')
        except Exception as e:
            self.logger.error(f'Book listener startup failed - {e}')

    def load_futures_data(self):
        for account_id in self.accounts:
            if not self.is_futures_of():
                continue

            self.logger.debug(f'loading futures data for {account_id} ')
            #leverage
            leverage = self.client.get_product_leverage(self.pair, account_id=account_id)
            self.leverages[account_id] = int(leverage) if leverage else 1

            #instrument data
            exchange_id = self.exchange_id_of(account_id=account_id)
            s = self.client.instrument_data()
            instruments = s.get_active_instruments_for_exchange(exchange_id)
            for ins in instruments:
                if ins.exchange_symbol == self.pair:
                    self.instrument_data[account_id] = ins
                    self.logger.info(f'Adding instrument - {ins} for {account_id}')


    @property
    def total_amount(self):
        return self._total_amount

    @total_amount.setter
    def total_amount(self, amount):
        self.logger.debug(f'setting amount {amount}')
        amount = float(amount)
        if amount > 0:
            self._total_amount = amount
        else:
            raise ValueError(f'total amount {amount} must be a number great than 0')

    @property
    def remaining_amount(self):
        return self.total_amount - self.order_monitor.dealt

    @property
    def max_price(self):
        return self._max_price

    @max_price.setter
    def max_price(self, value):
        value = float(value)
        if value < 0:
            self._max_price = math.inf
        else:
            self._max_price = value

    @property
    def min_price(self):
        return self._min_price

    @min_price.setter
    def min_price(self, value):
        self._min_price = float(value)

    @property
    def side(self):
        return self._side

    @side.setter
    def side(self, value):
        self._side = Side(value)

    @property
    def max_slippage_threshold(self):
        return self._max_slippage_threshold

    @max_slippage_threshold.setter
    def max_slippage_threshold(self, value):
        self._max_slippage_threshold = float(value)

    @property
    def large_order_threshold(self):
        if self._large_order_threshold:
            return min(self._large_order_threshold, self.remaining_amount)
        else:
            return self._large_order_threshold

    @large_order_threshold.setter
    def large_order_threshold(self, value):
        if value is None or float(value) == -1:
            self._large_order_threshold = -1
            return
        value = float(value)
        if value > 0:
            self._large_order_threshold = value
        else:
            raise ValueError(
                f'large order {value} must be a number great than 0, or -1'
            )

    @property
    def cumulative_order_threshold(self):
        if self._cumulative_order_threshold:
            return min(self._cumulative_order_threshold, self.remaining_amount)
        else:
            return self._cumulative_order_threshold

    @cumulative_order_threshold.setter
    def cumulative_order_threshold(self, value):
        if value is None or float(value) == -1:
            self._cumulative_order_threshold = -1
            return
        value = float(value)
        if value > 0:
            self._cumulative_order_threshold = value
        else:
            raise ValueError(
                f'cumulative order {value} must be a number great than 0, or -1'
            )

    @property
    def pair(self):
        return self.base + self.quote

    @property
    def base(self):
        return self._base

    @property
    def quote(self):
        return self._quote

    @base.setter
    def base(self, value):
        self._base = value
        self.orderbook = functools.partial(self.client.get_orderbook, pair=self.pair,)

    @quote.setter
    def quote(self, value):
        self._quote = value
        self.orderbook = functools.partial(self.client.get_orderbook, pair=self.pair)

    @property
    def config(self):
        return {
            'instrument_type': self.instrument_type,
            'totalAmount': self.total_amount,
            'maxPrice': -1 if self.max_price == math.inf else self.max_price,
            'minPrice': self.min_price,
            'side': self.side,
            'orderType': self.order_type,
            'maxSlippageThreshold': self.max_slippage_threshold,
            'largeOrderThreshold': self.large_order_threshold,
            'cumulativeOrderThreshold': self.cumulative_order_threshold,
            'remark': self.remark,
        }

    @config.setter
    def config(self, config: dict):
        self.logger.debug(f'applying config {config}')
        self.config_error = None
        try:
            self.instrument_type = config.get('instrument_type', 'SPOT')
            self.total_amount = config.get('totalAmount')
            self.max_price = config.get('maxPrice')
            self.min_price = config.get('minPrice')
            self.side = config.get('side')
            self.order_type = config.get('orderType', 'LIMIT')
            self.max_slippage_threshold = config.get('maxSlippageThreshold')
            self.large_order_threshold = config.get('largeOrderThreshold')
            self.cumulative_order_threshold = config.get('cumulativeOrderThreshold')
            self.update_remark(config.get('remark', ''))
            self.update_trigger_condition(config.get('trigger_condition', ' '))
            self.update_stop_condition(config.get('stop_condition', ' '))
        except Exception as e:
            self.config_error = e
            self.logger.error('error when setting config')
            self.logger.error(traceback.format_exc())

    @property
    def config_is_valid(self):
        if self.config_error:
            self.logger.error('invalid config due to error when setting config')
            return False
        try:
            if self.total_amount <= 0:
                self.logger.error('total amount must be more than 0')
                return False
            if not self.accounts:
                self.logger.error('accounts must be defined')
                return False
            if self.max_price == 0 or self.max_price < self.min_price:
                self.logger.error('invalid min/max prices')
                return False
            if self.max_slippage_threshold < 0:
                self.logger.error('max slippage threshold must be positive')
                return False
            if self.side != BUY and self.side != SELL:
                self.logger.error('order side must be buy or sell')
                return False
            if self.order_type not in ['LIMIT', 'LIMIT_CLOSE']:
                self.logger.error('order type must be LIMIT or LIMIT_CLOSE')
                return False
            if self._large_order_threshold != -1 and self._large_order_threshold <= 0:
                self.logger.error('order threshold must be positive or -1')
                return False
            if (
                self._cumulative_order_threshold != -1
                and self._cumulative_order_threshold <= 0
            ):
                self.logger.error('order threshold must be positive or -1')
                return False
            return True
        except:
            self.logger.error(traceback.format_exc())
            return False

    @property
    def info(self):
        return {
            'timestamp': datetime.utcnow(),
            'total': self.total_amount,
            'open_orders': json.dumps(self.order_monitor.open_orders),
            'completed_orders': json.dumps(self.order_monitor.completed_orders),
            'dealt_price' : str(self.order_monitor.dealt_price or 0.0),
            'dealt' : str(self.order_monitor.dealt or 0.0),
        }

    @property
    def completed(self):
        return self.order_monitor.dealt > self.total_amount or math.isclose(
            self.total_amount, self.order_monitor.dealt, abs_tol=0.0001
        )

    @property
    def progress(self):
        return round(self.order_monitor.dealt / self.total_amount * 100, 2)

    @functools.lru_cache(maxsize=None)
    def exchange_name_of(self, account_id):
        return self.client.get_account_config('exchange_name', account_id=account_id)

    @functools.lru_cache(maxsize=None)
    def exchange_id_of(self, account_id):
        return self.client.get_account_config('exchange_id', account_id=account_id)

    @functools.lru_cache(maxsize=None)
    def name_of(self, account_id):
        return self.client.get_account_config('name', account_id=account_id)

    @functools.lru_cache(maxsize=None)
    def is_futures_of(self):
        return self.instrument_type == "FUTURES"

    @functools.lru_cache(maxsize=None)
    def dual_side_position_of(self, account_id):
        dual_side_position = self.client.get_account_config('dual_side_position', account_id=account_id)
        return eval(dual_side_position) if dual_side_position else False

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

    def is_opening_position(self) -> bool:
        if self.order_type in ['LIMIT_CLOSE']:
            return False
        return True

    def is_coin_margin(self, altonomy_symbol):
        alto_symbol = altonomy_symbol.split('/')
        return len(alto_symbol) >= 3 and alto_symbol[2] == 'COIN'
    
    def balance_can_meet_order(self, account_id, order_price, order_amount) -> bool:
        """ check if account has enough balance to place the order """
        balance = self.client.get_account_balance(account_id, force_rpc=True)
        
        # Futures
        if self.is_futures_of():
            instrument_data = self.instrument_data[account_id]
            if instrument_data is None:
                return False

            leverage = self.leverages[account_id]

            if self.is_coin_margin(instrument_data.altonomy_symbol):
                margin_balance = balance[instrument_data.settlement_asset].available * leverage * order_price / float(instrument_data.contract_size)
            else:
                margin_balance = balance[instrument_data.settlement_asset].available * leverage / order_price

            self.logger.debug(f'margin balance: {margin_balance} leverage: {leverage}')

            if self.dual_side_position_of(account_id=account_id):
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
                start_bal = balance[self.base].available
            elif self.side == BUY:
                start_bal = balance[self.quote].available / order_price
            else:
                raise ValueError
        self.logger.debug(f'start_bal: {start_bal} order amt: {order_amount}')
        if math.isclose(start_bal, order_amount, rel_tol=0.001):
            return False
        else:
            return start_bal > order_amount

    def get_order_type(self):
        if self.side == BUY:
            return f'BUY_{self.order_type}' if self.is_opening_position() else f'SELL_{self.order_type}'
        if self.side == SELL:
            return f'SELL_{self.order_type}' if self.is_opening_position() else f'BUY_{self.order_type}'

    def send_order(self, price, size, *args, **kwargs):
        func = self.client.buy if self.side == BUY else self.client.sell
        self.logger.debug(f'sending {self.side} order {size}@{price} for {self.pair}')
        return func(self.pair, price=price, size=size, order_type=self.get_order_type(), *args, **kwargs)

    def run(self):
        if not self.config_is_valid:
            self.logger.error(f'not running due to invalid config {self.config}')
            time.sleep(self.delay)
            return
        if self.completed:
            self.logger.info(
                f'sniper bot completed: cleared'
                f'{self.order_monitor.dealt}/{self.total_amount} {self.base if self.side == SELL else self.quote}'
            )
            time.sleep(self.delay)
            return
        self.logger.debug(
            f'pending {self.order_monitor.pending}, dealt {self.order_monitor.dealt}'
        )
        if (
            math.isclose(
                self.order_monitor.pending + self.order_monitor.dealt,
                self.total_amount,
                abs_tol=0.00001,
            )
            or self.order_monitor.pending + self.order_monitor.dealt > self.total_amount
        ):
            self.logger.info(
                f'possibly completed; awaiting update from open orders {self.order_monitor.open_orders}, not placing new order'
            )
            time.sleep(self.delay)
            return
        self.logger.debug('starting run cycle')

        # start checking a different account each time
        # TODO: aggregate order books and send the most favorable order first
        for account_id in random.sample(self.accounts, len(self.accounts)):
            if account_id in (
                o.account_id for o in self.order_monitor.open_orders.values()
            ):
                self.logger.debug(
                    f'ignoring {account_id} due to outstanding pending orders'
                )
                continue
            ob = self.orderbook(account_id=account_id)
            if len(ob.bids) == 0 and len(ob.asks) == 0:
                # ignore empty books, probably an error
                continue
            if not ob.timestamp:
                # ignore books without timestamps, probably an error
                continue
            delay_threshold = self.order_book_delay_threshold(account_id)
            if (
                ob.timestamp
                < max(
                    (
                        o.update_time
                        for o in self.order_monitor.orders.values()
                        if o.account_id == account_id and o.update_time
                    ),
                    default=0,
                )
                + delay_threshold
            ):
                self.logger.debug(f'waiting for order book update for {account_id}')
                continue
            self.logger.debug(f'obtained orderbook {ob}')

            if self.side == BUY:
                price_levels = ob.asks
            else:
                price_levels = ob.bids

            for price_level in price_levels:
                if price_level.amount < self.large_order_threshold:
                    continue
                if price_level.cumulative < self.cumulative_order_threshold:
                    continue
                self.logger.debug(
                    f'{price_level} meets large order threshold {self.large_order_threshold} '
                    f'and cumulative order threshold {self.cumulative_order_threshold}'
                )
                if (
                    price_level.price < self.min_price
                    or price_level.price > self.max_price
                ):
                    self.logger.debug(
                        f'{price_level} not in price range {self.min_price}, {self.max_price}'
                    )
                    break
                if price_level.slippage > self.max_slippage_threshold:
                    break

                order_amount = min(
                    self.total_amount
                    - self.order_monitor.pending
                    - self.order_monitor.dealt,
                    price_level.cumulative,
                )

                if self.check_stop_condition(account_id):
                    break

                if not self.check_trigger_condition():
                    break

                if not self.balance_can_meet_order(
                    account_id, price_level.price, order_amount
                ):
                    continue

                order_id = self.send_order(
                    price=price_level.price, size=order_amount, account_id=account_id, remark=self.remark
                )
                self.order_monitor.add(order_id)
                return
            else:
                self.logger.debug(f'no price level matches conditions')
            time.sleep(self.delay)

    def __enter__(self):
        self.order_monitor.start()
        return self

    def __exit__(
        self, exc_type, exc_value, traceback,
    ):
        self.order_monitor.stop()
        self.logger.debug(
            f'bot exiting, remaining open orders are {self.order_monitor.open_orders}'
        )
        
        for account_id in self.accounts:
            exchange_name = self.exchange_name_of(account_id)
            self.client.deregister_resource_usage(
                "book",
                self.client.exchange_name(exchange_name),
                self.pair)
            self.logger.info(f'Deregister resource usage: resource_type=book, \
                exchange={exchange_name}, pair={self.pair}')
