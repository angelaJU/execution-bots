###############################################################################
# Github: https://bitbucket.org/altonomy/altonomy-apl-bots/
# Description: TWAP Trading Bot
###############################################################################

import functools
import logging
import time
import json
import operator
from importlib import import_module
from contextlib import AbstractContextManager
import traceback
from altonomy.core.Order import Order

import cachetools

from .OrderMonitor import OrderMonitor
from altonomy.core import client
from altonomy.core.Side import BUY, SELL, Side

from . import config
from .HelmClient import HelmClient

from enum import IntEnum

BOT_ACTION_START = "START"
BOT_ACTION_PAUSE = "PAUSE"
BOT_ACTION_RESUME = "RESUME"

MIN_FORCE_BALANCE_CHECK_BACKOFF_SECONDS = 1
MAX_FORCE_BALANCE_CHECK_BACKOFF_SECONDS = 11
FORCE_BALANCE_CHECK_BACKOFF_RATE = 1.5
FAILED_ORDER_WAIT_SLEEP_SECONDS = 10
CONSECUTIVE_CANCELLATION_WAITING_SECONDS = 2

ERR_INVALID_CONFIG_FORMAT = 'Invalid config format'
ERR_INVALID_CONFIG_PARAMETERS = 'Invalid config parameters'

ERR_BASE_AND_QUOTE_EMPTY = 'Base and Quote coins must be none empty'
ERR_ACCOUNT_ID_EMPTY = 'Account ID must be none empty'
ERR_INSTRUMENT_DATA_UNAVAILABLE = 'Cannot find ticker in instrument data'
ERR_QTY_LESS_THAN_MIN_QTY = 'Total Qty({}) must be gt Min Order Qty({})'
ERR_SLICE_SIZE_LESS_THAN_MIN_QTY = 'Slice Size({}) lt Min Order Qty({})'
ERR_MAX_ORDER_SIZE_BREACH = 'Order Size({}) >= Slice size ({}) x {}'
ERR_INVALID_DURATION = 'Invalid Duration ({})'

ERR_NO_PRICE_QTY = 'Cannot decide Price/Qty'


class Aggressiveness(IntEnum):
    TICK_BETTER = 0
    TAKING = 1


class TradableBitMask(IntEnum):
    'Order tradable bitmask'
    ALL_GOOD = 0
    MarketStatus = 1 << 0        # Detected from status in the callback
    MarketDataStale = 1 << 1     # Detested from the last timestamp
    PricerNotReady = 1 << 2


class BotStatus(IntEnum):
    WAITING = 0
    ORDER_SUBMITTED = 1
    ORDER_PENDING = 2
    ORDER_CANCELLED = 3
    STRATEGY_COMPLETED = 4
    ERROR = 5
    NOT_ENOUGH_BALANCE = 6
    ORDER_FAILED = 7
    THRESHOLD_PRICE_BREACH = 8
    MAX_ORDER_SIZE_BREACH = 9
    TRIGGER_CONDITION_BREACH = 10
    STOP_CONDITION_MET = 11


class TWAPBot(AbstractContextManager):
    def __init__(
        self,
        account_id,
        base,
        quote,
        bot_id,
        config,
        *,
        service_id=None,
        alt_client=None,
        logger=None,
    ):
        self.logger = logger or logging.getLogger()
        self.account_id = account_id
        self.bot_id = bot_id
        self.service_id = service_id
        if alt_client:
            self.client = alt_client
        else:
            self.client = client(
                account_id=self.account_id,
                logger=self.logger
            )
        if service_id:
            self.client.service_id = service_id
        self.ref_client = client(logger=self.logger)
        self.base = base
        self.quote = quote
        self.pair = self.base + self.quote
        self.side = None
        self.total_quantity = 0.0
        self.total_duration = 0.0
        self.order_type = 'LIMIT'
        self.threshold_price = -1
        self.remark = ''
        self.max_slice_size_multiplier = 5
        self.default_post_frequency = 10  # twap will send orders every 10sec
        self.trigger_condition = ' '
        self.stop_condition = ' '
        self._action = BOT_ACTION_START
        self.config = config
        self.config_error = None
        self.last_error = None
        self.bot_status = BotStatus.WAITING
        self.orderbook = functools.partial(
            self.client.get_orderbook,
            pair=self.pair
        )
        self.exchange_name = self.exchange_name_of(self.account_id)

        self.tob = None
        self.toa = None
        self.mid = None

        self.order_id = None
        self.order_quantity = 0
        self.instrument_data = None
        self.leverage = 1

        self.tradable_bit_mask = TradableBitMask.ALL_GOOD
        self.tradable_bit_mask |= TradableBitMask.PricerNotReady
        self.tradable_bit_mask |= TradableBitMask.MarketStatus

        self.slice_size = 0
        self.post_frequency = self.default_post_frequency
        self.total_no_of_posts = 0
        self.posts_completed = 0
        self.bot_progress_duration = 0
        self.last_post_ts = 0
        self.last_min_order_qty = 0
        self.min_order_qty = 0

        self.last_force_rpc_balance_ts = time.time()
        self.balance_check_backoff = None

        self.continues_failed_order_count = 0
        self.continues_failed_order_count_ts = time.time()

        self.instrument_load_ts = time.time()
        self.update_redis_ts = time.time()

        self.start_time = time.time()

        self.account_operation = None

        self.order_monitor = OrderMonitor(
            self.client,
            self.logger,
            refresh_interval=0.5,
            try_cancels=10,
            try_cancel_interval=0.2
        )
        self.get_account_operation()

        self.initialise_order_monitor()

        self.start_book_listener()

        self.load_instruments_data()

        self.load_startegy_params()

        self.check_strategy_params()

        self.logger.info(
            f'TWAPBot started on account {account_id} '
            f'with bot_id {self.bot_id}'
        )

    def reset_twap_strategy(self):
        self.total_quantity = 0.0
        self.total_duration = 0.0
        self.threshold_price = -1
        self.remark = ''
        self.max_slice_size_multiplier = 5
        self._action = BOT_ACTION_START
        self.config_error = None
        self.last_error = None
        self.bot_status = BotStatus.WAITING
        self.order_id = None
        self.order_quantity = 0

        self.tradable_bit_mask = TradableBitMask.ALL_GOOD
        self.tradable_bit_mask |= TradableBitMask.PricerNotReady
        self.tradable_bit_mask |= TradableBitMask.MarketStatus

        self.slice_size = 0
        self.post_frequency = self.default_post_frequency
        self.total_no_of_posts = 0
        self.posts_completed = 0
        self.bot_progress_duration = 0
        self.last_post_ts = 0
        self.last_min_order_qty = self.min_order_qty
        self.start_time = time.time()

    def load_startegy_params(self):
        try:
            if not self.account_operation:
                return

            self.logger.debug(
                f'Load Strategy Params - {self.account_operation}')

            details = self.account_operation.get(
                "Details", None)

            self.logger.debug(f'Details - {details}')

            status = self.account_operation.get(
                "Status", None)
            self.logger.debug(f'Loaded Bot status - {status}')
            status = status.get("Status", "WAITING")

            self.post_frequency = float(details.get("Post_Frequency"))
            self.slice_size = float(details.get("Slice_Size"))
            self.posts_completed = int(details.get("Posts_Completed"))
            self.total_no_of_posts = int(details.get("Total_No_Of_Posts"))
            self.bot_progress_duration = float(
                details.get("Bot_Progress_Duration"))
            self.last_min_order_qty = float(details.get("Last_Min_Order_Qty"))

            if status != BotStatus.MAX_ORDER_SIZE_BREACH.name:
                self.order_id = int(details.get("Last_Order_Id"))
                self.order_quantity = float(details.get("Last_Order_Qty"))

            self.logger.info(
                f'Loaded - order_id = {self.order_id} '
                f'order_quantity = {self.order_quantity} '
                f'posts_completed = {self.posts_completed} '
                f'total_no_of_posts = {self.total_no_of_posts} '
                f'bot_progress_duration = {self.bot_progress_duration} ')
        except Exception as e:
            self.logger.error(f'load_startegy_params - {e}')

    def check_strategy_params(self):
        if self.slice_size and self.last_min_order_qty >= self.min_order_qty:
            self.logger.info("Strategy params remain same as stored in Redis.")
            return

        self.logger.info(
            f'New min_order_qty - {self.min_order_qty} '
            f'Last min_order_qty - {self.last_min_order_qty}')
        qty = self.remain_qty
        remaining_duration = (
            self.total_duration * (self.remain_qty / self.total_quantity))

        self.logger.debug(
            f'qty = {qty} '
            f'remaining_duration = {remaining_duration} '
        )
        if qty and remaining_duration:
            self.calculate_strategy_params(qty, remaining_duration)
        self.last_min_order_qty = self.min_order_qty

    def calculate_strategy_params(self, qty, duration):
        """ Default slice_size to 2 * min_order_qty,
            Recalculate if post_frequency goes below Default Post Frequency
        """
        self.logger.info("Calculating Strategy Praams.")
        size = self.min_order_qty * 2
        self.post_frequency = round(duration * size / qty, 2)

        if self.post_frequency < self.default_post_frequency:
            self.post_frequency = self.default_post_frequency
            size = qty * self.post_frequency / duration

        self.slice_size = round(size, self.instrument_data.size_precision)

        no_of_posts = int(duration / self.post_frequency)
        self.total_no_of_posts = no_of_posts + self.posts_completed

        self.logger.debug(
            f'calculate_strategy_params - '
            f'post_frequency = {self.post_frequency} '
            f'slice_size = {self.slice_size } '
            f'min_order_qty = {self.min_order_qty} '
            f'remain_qty = {self.remain_qty} '
            f'posts_completed = {self.posts_completed} '
            f'total_no_of_posts = {self.total_no_of_posts} '
            f'qty = {self.total_quantity}'
        )

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
                ref_pair, {}).get({"BUY": "asks", "SELL": "bids"}.get(
                    str(self.side).upper()), [{}])[0].get("price", 0)
        except Exception as e:
            self.logger.error(f'Error in get_reference_price - {e}')

        self.logger.debug(f'ref_price = {ref_price}')
        return ref_price

    def get_account_operation(self):
        try:
            self.logger.info('Get account Operation from Redis')
            initial_position = self.get_position()
            if not initial_position:
                return

            account_operation = initial_position.get("Account_Operation", None)
            if not account_operation:
                return

            self.account_operation = json.loads(account_operation)
            self.logger.debug(f'Account_Operation - {self.account_operation}')
        except Exception as e:
            self.logger.error(f'get_account_operation - {e}')

    def initialise_order_monitor(self):
        try:
            self.logger.info('Initializing order monitor')
            stored_orders = self.get_stored_orders()
            if not stored_orders:
                return

            self.logger.debug(f'Stored Orders - {stored_orders}')

            open_orders = {}
            completed_orders = {}

            om_open_orders = json.loads(
                stored_orders.get("open_orders"))
            om_completed_orders = json.loads(
                stored_orders.get("completed_orders"))

            # Convert orders dict to Order Objects
            for key, order in om_open_orders.items():
                if order is not None:
                    open_orders[int(key)] = Order(order)

            # Convert orders dict to Order Objects
            for key, order in om_completed_orders.items():
                if order is not None:
                    completed_orders[int(key)] = Order(order)

            self.order_monitor.completed_orders = completed_orders
            self.order_monitor.open_orders = open_orders
            self.logger.debug(f'total_dealt = {self.order_monitor.dealt}')
        except Exception as e:
            self.logger.error(f'initialise_order_monitor - {e}')

    def start_book_listener(self):
        try:
            account = self.name_of(self.account_id)
            hc = HelmClient(endpoint=config.HELM_REPO, logger=self.logger)
            hc.start_orderbook_listener(
                account, self.exchange_name, self.base, self.quote)
            self.client.register_resource_usage(
                "book",
                self.client.exchange_name(self.exchange_name),
                self.pair)
            self.logger.info(
                f'Register resource usage: '
                f'resource_type=book exchange={self.exchange_name} '
                f'pair={self.pair}')
        except Exception as e:
            self.logger.error(f'Book listener startup failed - {e}')

    def update_min_qty(self, price):
        if self.is_coin_margin:
            self.min_order_qty = 1.0
        elif self.instrument_data.min_order_notional:
            self.min_order_qty = (
                self.instrument_data.min_order_notional / price + 1e-10)
            self.min_order_qty = round(
                self.min_order_qty,
                self.instrument_data.size_precision)
            if self.min_order_qty == 0.0 or (
                    self.min_order_qty
                    < self.instrument_data.min_order_size + 1e-10):
                self.min_order_qty = round(
                    self.instrument_data.min_order_size,
                    self.instrument_data.size_precision)
        else:
            self.min_order_qty = round(
                self.instrument_data.min_order_size,
                self.instrument_data.size_precision)

    def get_bot_status(self, bot_completed):
        if self.last_error:
            return self.last_error
        if bot_completed:
            return 'COMPLETED'

        return 'RUNNING'

    def validate_orderbook(self, ob):
        if len(ob.bids) == 0 or len(ob.asks) == 0:
            # ignore empty books, probably an error
            self.tradable_bit_mask |= TradableBitMask.PricerNotReady
            return False
        if not ob.timestamp:
            # ignore books without timestamps, probably an error
            self.tradable_bit_mask |= TradableBitMask.MarketStatus
            return False
        if time.time() - float(ob.timestamp) > (
                config.BROKER_ORDERBOOK_REFRESH_MAX_TIME):
            self.logger.error(f'staled market data - timestamp={ob.timestamp}')
            self.tradable_bit_mask |= TradableBitMask.MarketDataStale
            return False

        delay_threshold = self.order_book_delay_threshold(self.account_id)
        if (
            ob.timestamp * 1000
            < max(
                self.order_monitor.get_latest_update_time(self.account_id),
                default=0,
            )
            + delay_threshold
        ):
            self.logger.debug(
                f'waiting for order book update for {self.account_id}, '
                f'current ob timestamp={ob.timestamp}')
            self.tradable_bit_mask |= TradableBitMask.MarketDataStale
            return False
        return True

    def update_market_data(self):
        try:
            ob = self.orderbook()
            self.logger.debug(f'ob={ob}')

            if not self.validate_orderbook(ob):
                return

            self.logger.debug(f'obtained orderbook for {self.account_id}')
            self.tob = ob.bids[0].price
            self.toa = ob.asks[0].price
            self.mid = 0.5 * (self.tob + self.toa)

            if self.tradable_bit_mask & TradableBitMask.PricerNotReady > 0:
                self.tradable_bit_mask &= ~TradableBitMask.PricerNotReady
            if self.tradable_bit_mask & TradableBitMask.MarketStatus > 0:
                self.tradable_bit_mask &= ~TradableBitMask.MarketStatus
            if self.tradable_bit_mask & TradableBitMask.MarketDataStale > 0:
                self.tradable_bit_mask &= ~TradableBitMask.MarketDataStale

            self.logger.debug(
                f'tob={self.tob}, toa={self.toa}, timestamp={ob.timestamp}')
        except Exception as e:
            self.logger.error(f'Error in update_market_data - {e}')

    def load_instruments_data(self):
        try:
            exchange_id = self.exchange_id_of(account_id=self.account_id)
            s = self.client.instrument_data()
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
            if self.is_futures_of():
                self.leverage = self.client.get_product_leverage(
                    self.pair, account_id=self.account_id)
            self.min_order_qty = (
                self.instrument_data.min_order_size
                or self.instrument_data.lot_size)

            self.update_market_data()
            self.update_min_qty(self.mid)
        except Exception as e:
            self.logger.error(f'Error in load_instruments_data - {e}')

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

    def update_trigger_condition(self, trigger_condition):
        """
        :trigger_condition (str) in the form "exchange;pair;direction;value"
        :update trigger condition
        """
        if isinstance(trigger_condition, str) \
                and trigger_condition.count(";") == 3:
            self.trigger_condition = trigger_condition
        else:
            self.logger.debug(
                "invalid trigger condition format - "
                "needs to be 'exchange;pair;direction;value'")
            self.trigger_condition = ' '

    def update_stop_condition(self, stop_condition):
        """
        :stop_condition (str) in the form "asset;direction;value"
        :update stop_condition
        """
        if isinstance(stop_condition, str) and stop_condition.count(";") == 2:
            self.stop_condition = stop_condition
        else:
            self.logger.debug(
                "invalid stop condition format - "
                "needs to be 'asset;direction;value'")
            self.stop_condition = ' '

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

    @property
    def remain_qty(self):
        return self.total_quantity - self.order_monitor.dealt

    @property
    def side(self):
        return self._side

    @side.setter
    def side(self, value):
        self._side = Side(value)

    @property
    def action(self):
        return self._action

    def update_action(self, value):
        if value == BOT_ACTION_PAUSE:
            # pause updating the bot duration
            current_time_spent = time.time() - self.start_time
            self.bot_progress_duration += current_time_spent
            self.logger.debug(f'update_action - {self.bot_progress_duration}')
            self.start_time = 0
        else:
            # resume the bot duration calculation
            self.start_time = time.time()
        self._action = value

    @property
    def config(self):
        return {
            'instrument_type': self.instrument_type,
            'side': self.side,
            'quantity': self.total_quantity,
            'duration': self.total_duration,
            'threshold_price': self.threshold_price,
            'default_post_frequency': self.default_post_frequency,
            'remark': self.remark,
            'max_slice_size_multiplier': self.max_slice_size_multiplier
        }

    @config.setter
    def config(self, config: dict):
        self.logger.debug(f'applying config {config}')
        self.config_error = None
        try:
            self.instrument_type = config.get('instrument_type', 'SPOT')
            self.side = config.get('side')
            self.total_quantity = float(config.get('quantity'))
            self.total_duration = float(config.get('duration', '10.0'))
            self.threshold_price = float(config.get('threshold_price', '-1'))
            self.default_post_frequency = int(float(
                config.get('default_post_frequency', '10')))
            self.update_remark(config.get('remark', ''))
            self.update_trigger_condition(config.get('trigger_condition', ' '))
            self.update_stop_condition(config.get('stop_condition', ' '))
            self.max_slice_size_multiplier = float(
                config.get('max_slice_size_multiplier', '5'))
        except Exception as e:
            self.config_error = e
            self.logger.error('error when setting config')
            self.logger.error(traceback.format_exc())

    @property
    def balance(self):
        try:
            balance = self.client.get_account_balance(
                self.account_id, force_rpc=False)
            if self.is_futures_of():
                asset = self.instrument_data.settlement_asset
                return {
                    asset:
                        balance[asset].available,
                    f'{self.pair} Long':
                        balance[f'{self.pair} Long'].available,
                    f'{self.pair} Short':
                        balance[f'{self.pair} Short'].available
                }
            else:
                return {
                    self.base: balance[self.base].available,
                    self.quote: balance[self.quote].available
                }
        except Exception as e:
            self.logger.error(f'balance - {e}')
            return {}

    @property
    def reason(self):
        if self.last_error is not None:
            return self.last_error

        if self.bot_status == BotStatus.WAITING:
            for bit in TradableBitMask:
                if self.tradable_bit_mask & bit > 0:
                    return f'{bit.name}'

        if self.bot_status == BotStatus.ORDER_FAILED:
            return self.order_monitor.get_failed_order_error_code(
                self.order_id)

    @functools.lru_cache(maxsize=None)
    def exchange_name_of(self, account_id):
        return self.client.get_account_config(
            'exchange_name', account_id=account_id)

    @functools.lru_cache(maxsize=None)
    def exchange_id_of(self, account_id):
        return self.client.get_account_config(
            'exchange_id', account_id=account_id)

    @functools.lru_cache(maxsize=None)
    def name_of(self, account_id):
        return self.client.get_account_config(
            'name', account_id=account_id)

    @functools.lru_cache(maxsize=None)
    def is_futures_of(self):
        return self.instrument_type == "FUTURES"

    @functools.lru_cache(maxsize=None)
    def dual_side_position_of(self):
        dual_side_position = self.client.get_account_config(
            'dual_side_position',
            account_id=self.account_id)
        return eval(dual_side_position) if dual_side_position else False

    @cachetools.cached(cache=cachetools.TTLCache(maxsize=64, ttl=60))
    def order_book_delay_threshold(self, account_id, default=0.5):
        delay_threshold = self.client.get_exchange_config(
            'book_refresh_rate', self.exchange_id_of(account_id))
        if delay_threshold is None:
            self.logger.debug(
                f'book_refresh_rate for {self.name_of(account_id)} not set, '
                f'defaulting to {delay_threshold}')
            return default
        else:
            return float(delay_threshold)

    def is_opening_position(self) -> bool:
        if self.order_type in ['LIMIT_CLOSE']:
            return False
        return True

    @property
    def is_coin_margin(self):
        if self.is_futures_of():
            alto_symbol = self.instrument_data.altonomy_symbol.split('/')
            return len(alto_symbol) >= 3 and alto_symbol[2] == 'COIN'
        return False

    def is_market_data_good(self):
        if self.tradable_bit_mask & TradableBitMask.MarketDataStale == 0 \
                and self.tradable_bit_mask \
                & TradableBitMask.PricerNotReady == 0 \
                and self.tradable_bit_mask & TradableBitMask.MarketStatus == 0:
            return True
        return False

    def threshold_price_breached(self, price):
        if self.threshold_price <= 0:
            return False

        if self.side == BUY:
            return price > self.threshold_price  # Ceiling for the Buy
        else:
            return price < self.threshold_price  # Floor for the sell

    def max_order_size_breached(self, size):
        if size >= self.slice_size * self.max_slice_size_multiplier:
            return True
        return False

    def get_coin_tradable_balance(self, currency):
        """ Gte account balance for given coin """
        balance = self.client.get_account_balance(
                    self.account_id, force_rpc=False)
        if not balance or currency not in balance.keys():
            self.logger.info(f'No Balance found for - {currency}')
            return -1.0
        return balance[currency].get('available', 0.0)

    def balance_can_meet_order(self, order_price, order_amount) -> bool:
        """ check if account has enough balance to place the order """
        try:
            if self.balance_check_backoff and \
                    (time.time() - self.last_force_rpc_balance_ts) > \
                    self.balance_check_backoff:
                self.logger.info(
                    f'Force rpc balance check, balance_check_backoff='
                    f'{self.balance_check_backoff}')
                balance = self.client.get_account_balance(
                    self.account_id, force_rpc=True)
                self.balance_check_backoff = min(
                    MAX_FORCE_BALANCE_CHECK_BACKOFF_SECONDS,
                    FORCE_BALANCE_CHECK_BACKOFF_RATE
                    * self.balance_check_backoff
                )
                self.last_force_rpc_balance_ts = time.time()
            else:
                balance = self.client.get_account_balance(
                    self.account_id, force_rpc=False)

            # Futures
            if self.is_futures_of():
                if self.instrument_data is None:
                    return False
                start_bal = self.get_future_balance(balance, order_price)
            # Spots
            else:
                if self.side == SELL:
                    start_bal = balance[self.base].available
                elif self.side == BUY:
                    start_bal = balance[self.quote].available / order_price
                else:
                    raise ValueError

            self.logger.debug(
                f'start_bal: {start_bal} '
                f'order_amt: {order_amount} '
                f'order_price: {order_price}')

            is_enough_balance = start_bal >= order_amount

            if is_enough_balance:
                self.balance_check_backoff = None
            else:
                if self.balance_check_backoff is None:
                    self.balance_check_backoff = \
                        MIN_FORCE_BALANCE_CHECK_BACKOFF_SECONDS

            return is_enough_balance
        except Exception as e:
            self.logger.error(f'Error while getting balance - {e}')
            return False

    def get_future_balance(self, balance, order_price):
        if self.leverage is None:
            self.leverage = self.client.get_product_leverage(
                self.pair, account_id=self.account_id)
            return False

        ins = self.instrument_data
        if self.is_coin_margin:
            margin_balance = (
                balance[ins.settlement_asset].available
                * int(self.leverage)
                * order_price
                / float(ins.contract_size))
        else:
            margin_balance = (
                balance[self.instrument_data.settlement_asset].available
                * int(self.leverage)
                / order_price)

        self.logger.debug(
            f'margin balance: {margin_balance} '
            f'leverage: {self.leverage}')

        if self.dual_side_position_of():
            self.logger.debug('futures - dual side position:True')
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
            self.logger.debug('futures - dual side position:False')
            if self.side == SELL:
                start_bal = balance[f'{self.pair} Long'].available \
                    + margin_balance
            elif self.side == BUY:
                start_bal = balance[f'{self.pair} Short'].available \
                    + margin_balance
            else:
                raise ValueError
        return start_bal

    def push_bot_data_to_redis(self):
        """ push twap trading bot data to redis """
        if time.time() < (
                self.update_redis_ts
                + config.UPDATE_REDIS_FREQUENCY):
            return

        self.update_redis_ts = time.time()
        _position = self.position
        if _position:
            self.client.post(
                f'{config.BOT_OUTPUT_REDIS_KEY}:{self.bot_id}:position',
                _position)

        _orders = self.om_orders
        if _orders:
            self.client.post(
                f'{config.BOT_OUTPUT_REDIS_KEY}:{self.bot_id}:orders',
                _orders)
        self.logger.debug(f'updated redis for service {self.service_id}')

    def get_order_type(self):
        if self.side == BUY:
            if self.is_opening_position():
                return f'BUY_{self.order_type}'
            else:
                return f'SELL_{self.order_type}'
        if self.side == SELL:
            if self.is_opening_position():
                return f'SELL_{self.order_type}'
            else:
                return f'BUY_{self.order_type}'

    def send_order(self, price, size, *args, **kwargs):
        func = self.client.buy if self.side == BUY else self.client.sell
        self.logger.debug(
            f'sending {self.side} order {size}@{price} for {self.pair}')
        return func(
            self.pair,
            price=price,
            size=size,
            order_type=self.get_order_type(),
            remark=self.remark,
            *args,
            **kwargs)

    def get_position(self):
        try:
            position = self.client.get(
                f'{config.BOT_OUTPUT_REDIS_KEY}:{self.bot_id}:position')
            self.logger.debug(f'get_position - {position}')
            if position is None:
                return None

            return position
        except Exception as e:
            self.logger.error(f'get_position - {e}')
            return None

    def get_stored_orders(self):
        try:
            orders = self.client.get(
                f'{config.BOT_OUTPUT_REDIS_KEY}:{self.bot_id}:orders')
            self.logger.debug(f'get_stored_orders - {orders}')
            if orders is None:
                return None
            return json.loads(orders.get("Order_Monitor_Status", None))
        except Exception as e:
            self.logger.error(f'get_stored_orders - {e}')
            return None

    def get_bot_progress_duration(self):
        if self.action == BOT_ACTION_PAUSE:
            return self.bot_progress_duration

        current_time_spent = time.time() - self.start_time
        return self.bot_progress_duration + current_time_spent

    def get_theorotical_progress(self):
        if self.total_duration:
            progress = self.get_bot_progress_duration() / self.total_duration
            return 100 if progress > 1 else round(progress * 100, 1)
        return 0

    def get_actual_progress(self):
        if self.total_quantity:
            return round(
                (self.total_quantity - self.remain_qty)
                * 100 / self.total_quantity, 1)
        return 0

    @property
    def bot_position(self):
        return {
            "Account Id": self.account_id,
            "Details": {
                "Account": self.name_of(self.account_id),
                "Ticker": self.pair,
                "Side": str(self.side),
                "Slice_Size": str(self.slice_size),
                "Post_Frequency": str(self.post_frequency),
                "Total_No_Of_Posts": self.total_no_of_posts,
                "Posts_Completed": self.posts_completed,
                "Bot_Progress_Duration": self.get_bot_progress_duration(),
                "Last_Min_Order_Qty": self.last_min_order_qty,
                "Last_Order_Id": self.order_id,
                "Last_Order_Qty": self.order_quantity,
            },
            "Balance": self.balance,
            "Progress": {
                "Total_Qty": str(self.total_quantity),
                "Remaining_Qty": str(self.remain_qty),
                "TWAP": str(self.order_monitor.dealt_price or 0.0),
                "Theorotical_Progress": (
                    self.get_theorotical_progress()),
                "Actual_Progress": (self.get_actual_progress())
            },
            "Status": {
                "Status": self.bot_status.name,
                "Reason": self.reason
            }
        }

    @property
    def position(self):
        bot_completed = True
        bot_completed &= (self.bot_status == BotStatus.STRATEGY_COMPLETED)

        pos = {
            "Status": self.get_bot_status(bot_completed),
            "Account_Operation": json.dumps(self.bot_position)
        }
        self.logger.debug(pos)
        return pos

    @property
    def om_orders(self):
        om = self.order_monitor
        ords = {
            "Order_Monitor_Status": json.dumps({
                "open_orders":
                    json.dumps(om.open_orders),
                "completed_orders":
                    json.dumps(om.completed_orders),
            })
        }
        # self.logger.debug(ords)
        return ords

    def validate_config_parameters(self):
        if self.is_futures_of():
            if self.quote == '':
                self.last_error = ERR_BASE_AND_QUOTE_EMPTY
                return False
        else:
            if self.base == '' or self.quote == '':
                self.last_error = ERR_BASE_AND_QUOTE_EMPTY
                return False

        if self.account_id == '':
            self.last_error = ERR_ACCOUNT_ID_EMPTY
            return False
        if self.instrument_data is None:
            self.last_error = ERR_INSTRUMENT_DATA_UNAVAILABLE
            return False
        if self.total_quantity < self.min_order_qty:
            self.last_error = ERR_QTY_LESS_THAN_MIN_QTY.format(
                self.total_quantity, self.min_order_qty)
            return False
        if self.total_duration <= 0:
            self.last_error = ERR_INVALID_DURATION.format(
                self.total_duration)
            return False

        return True

    def adjusted_post_time(self):
        current_time = time.time()
        elapsed_time = current_time - self.last_post_ts
        if elapsed_time < 2 * self.post_frequency:
            self.logger.debug(f'Elapsed time - {elapsed_time}')
            return current_time - (elapsed_time - self.post_frequency)
        else:
            self.logger.info(
                f'Elapsed time is more than double the post frequency - '
                f'{elapsed_time}')
            return current_time

    def cancel_pending_order(self):
        # Check the previous order. cancel it if still not filled and
        self.consecutive_cancel_count = 0
        while self.order_monitor.pending > 0.0:
            if self.consecutive_cancel_count > 0 and \
                time.time() - self.consecutive_cancel_count_ts \
                    < CONSECUTIVE_CANCELLATION_WAITING_SECONDS:
                self.logger.info(
                    f'Consecutive order cancellations - '
                    f'{self.consecutive_cancel_count}')
                time.sleep(0.5)
            else:
                self.logger.info(
                    f'Cancelling the pending order id {self.order_id}'
                    f' with pending  {self.order_monitor.pending}')
                self.client.cancel(order_id=self.order_id, remark=self.remark)
                self.consecutive_cancel_count += 1
                self.consecutive_cancel_count_ts = time.time()
                self.bot_status = BotStatus.ORDER_CANCELLED

            if self.consecutive_cancel_count > 5:
                self.logger.info(
                    f'Giving up after {self.consecutive_cancel_count} '
                    f'tries. Deleting cancelled order - {self.order_id}')
                self.order_monitor.delete(self.order_id)

    def get_placed_orders_volume(self):
        """
        Calculate total volume of orders that have been placed.
        Uses OrderMonitor as the authoritative source.
        Returns the sum of original order sizes for all open orders + completed orders.
        """
        open_orders_volume = 0.0
        completed_orders_volume = 0.0

        # Sum up original order sizes of all open orders
        for order_id, order in self.order_monitor.open_orders.items():
            if order and hasattr(order, 'size'):
                open_orders_volume += order.size

        # Sum up original order sizes of all completed orders
        for order_id, order in self.order_monitor.completed_orders.items():
            if order and hasattr(order, 'size'):
                completed_orders_volume += order.size

        placed_volume = open_orders_volume + completed_orders_volume

        self.logger.debug(
            f'Placed orders volume: {placed_volume} '
            f'(open: {open_orders_volume}, completed: {completed_orders_volume})')
        return placed_volume

    def get_last_unfilled_qty(self):
        if self.order_id:
            if self.order_monitor.is_completed_order(self.order_id):
                self.logger.debug(f'Checking last sent order {self.order_id}')
                return self.order_monitor.get_remaining_qty(self.order_id)
            elif self.order_monitor.is_failed_order(self.order_id):
                self.bot_status = BotStatus.ORDER_FAILED
                self.logger.error(
                    f'Last sent order {self.order_id} '
                    f'is failed without any fills.')
                return self.order_quantity
            else:
                self.logger.debug(
                    f'The last order {self.order_id} with '
                    f'{self.order_quantity} didnt get executed')
                return self.order_quantity
        else:
            self.logger.info('About to prepare the first order')
            return 0

    def get_price(self, aggressiveness):
        self.logger.debug(
            f'get_price - Aggressivenes {aggressiveness}')
        if self.side == BUY:
            # Best Bid on the book
            if aggressiveness == Aggressiveness.TICK_BETTER:
                price = self.tob
            else:
                # Best Ask Price , as Taker
                price = self.toa
        else:
            # Best Ask on the book
            if aggressiveness == Aggressiveness.TICK_BETTER:
                price = self.toa
            else:
                # Best Bid Price , as Taker
                price = self.tob
        return price

    def check_stop_condition(self):
        if self.stop_condition != ' ':
            try:
                stop_asset, stop_direction, stop_value = \
                    [value.strip() for value in self.stop_condition.split(";")]
                available_balance = self.get_coin_tradable_balance(stop_asset)
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
                self.logger.debug(
                    f"failed to evaluation stop condition due to {e}")
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

    def run(self):
        self.last_error = None
        current_date_time = time.time()
        self.logger.debug(
            f'Running TWAP startegy cycle @ {current_date_time}')

        if self.bot_status == BotStatus.STRATEGY_COMPLETED:
            self.logger.info('Bot strategy is completed !!!')
            return

        if self.config_error:
            self.last_error = ERR_INVALID_CONFIG_FORMAT
            self.logger.error(
                'invalid config due to error when setting config')
            time.sleep(1)
            return

        if not self.validate_config_parameters():
            self.bot_status = BotStatus.ERROR
            self.logger.error(
                f'not running due to invalid config parametes {self.config}')
            time.sleep(1)
            return

        if self.remain_qty == 0.0 or self.remain_qty < self.min_order_qty:
            self.bot_status = BotStatus.STRATEGY_COMPLETED
            self.logger.info(f'Not much qty remaining - {self.remain_qty}')
            return

        if current_date_time > (
                self.instrument_load_ts
                + config.RELOAD_INSTRUMENT_DATA_FREQUENCY):
            self.logger.info(
                'Reloading Instrumentdata and calculating startegy params')
            self.instrument_load_ts = current_date_time
            self.load_instruments_data()
            self.check_strategy_params()

        # Error handling
        if self.continues_failed_order_count > 0 and \
            time.time() - self.continues_failed_order_count_ts \
                < min(
                    self.continues_failed_order_count,
                    FAILED_ORDER_WAIT_SLEEP_SECONDS):
            if self.order_monitor.is_failed_order(self.order_id):
                self.bot_status = BotStatus.ORDER_FAILED
                return

        if self.order_monitor.is_failed_order(self.order_id):
            self.logger.info(f'order_id failed, {self.order_id}')
            self.continues_failed_order_count += 1
            self.continues_failed_order_count_ts = time.time()
            self.bot_status = BotStatus.ORDER_FAILED
        else:
            self.continues_failed_order_count = 0

        # Wait till the next Post cycle
        if current_date_time < (self.last_post_ts + self.post_frequency):
            self.bot_status = BotStatus.WAITING
            wait_for = self.last_post_ts \
                + self.post_frequency - current_date_time
            self.logger.debug(
                f'waiting for next slot interval - {wait_for} seconds')
            return

        # if completed all posts, mark status as complete
        if self.posts_completed > self.total_no_of_posts:
            self.logger.info(f'Completed {self.posts_completed} posts.')
            # send cancellation of any pending orders.
            self.order_monitor.cancel_all_open_orders()
            # Mark Bot strategy as completed
            self.bot_status = BotStatus.STRATEGY_COMPLETED
            self.logger.info('Bot strategy is completed !!!')
            return

        # Cancel previuos unfilled order
        self.cancel_pending_order()

        # Get the latest market data
        self.update_market_data()
        if not self.is_market_data_good():
            self.logger.error('Market data is not good')
            return

        if self.tradable_bit_mask != TradableBitMask.ALL_GOOD:
            self.leg_status = BotStatus.WAITING
            self.logger.info(
                f'trading conditions did not meet - {self.tradable_bit_mask}')
            return

        last_unfilled_qty = self.get_last_unfilled_qty()
        self.logger.info(f'Last unfilled qty - {last_unfilled_qty}')

        # set Aggressivenes to Taker if anything pending from last post
        if last_unfilled_qty > 0:
            target_price = self.get_price(Aggressiveness.TAKING)
        else:
            target_price = self.get_price(Aggressiveness.TICK_BETTER)

        if self.threshold_price_breached(target_price):
            self.logger.debug(
                f'Target Price breached for the  order '
                f'{self.account_id} {target_price} {self.threshold_price}')
            self.bot_status = BotStatus.THRESHOLD_PRICE_BREACH
            return

        order_size = min(self.slice_size + last_unfilled_qty, self.remain_qty)
        order_size = round(
            order_size,
            self.instrument_data.size_precision)

        self.logger.debug(
            f'self.order_size = {order_size} '
            f'total_quantity = {self.total_quantity} '
            f'dealt = {self.order_monitor.dealt}')

        if self.max_order_size_breached(order_size):
            self.last_error = ERR_MAX_ORDER_SIZE_BREACH.format(
                order_size, self.slice_size, self.max_slice_size_multiplier)
            self.logger.error(
                f'Max Order Size breached for the  order. '
                f'- {self.last_error} '
                f'Bot will stop sending orders!!')
            self.bot_status = BotStatus.MAX_ORDER_SIZE_BREACH
            return

        if self.check_stop_condition():
            self.bot_status = BotStatus.STOP_CONDITION_MET
            return

        if not self.check_trigger_condition():
            self.bot_status = BotStatus.TRIGGER_CONDITION_BREACH
            return

        if not self.balance_can_meet_order(target_price, order_size):
            self.logger.debug(
                f'Balance cannot meet the order '
                f'{self.account_id} {target_price} {order_size}')
            self.bot_status = BotStatus.NOT_ENOUGH_BALANCE
            return

        self.order_quantity = order_size

        # Safeguard: Final check before placing order to prevent overshooting
        # This prevents overshooting when order listener is down
        current_placed_volume = self.get_placed_orders_volume()
        total_exposure = current_placed_volume + self.order_quantity

        if total_exposure > self.total_quantity:
            max_allowed_order_size = self.total_quantity - current_placed_volume
            if max_allowed_order_size <= 0:
                self.logger.warning(
                    f'Safeguard: Cannot place order. Already placed {current_placed_volume} '
                    f'out of {self.total_quantity} total quantity. Preventing overshoot.')
                return
            else:
                original_size = self.order_quantity
                self.order_quantity = max_allowed_order_size
                self.logger.warning(
                    f'Safeguard: Reducing order size from {original_size} to {max_allowed_order_size} '
                    f'to prevent overshooting. Already placed: {current_placed_volume}')

        self.logger.debug(
            f'Final order check - placing order of size {self.order_quantity}, '
            f'current_placed_volume = {current_placed_volume}, '
            f'total_exposure = {current_placed_volume + self.order_quantity}')

        # Send order to exchange
        self.order_id = self.send_order(
            price=target_price,
            size=self.order_quantity,
            account_id=self.account_id
            )

        self.last_post_ts = self.adjusted_post_time() \
            if self.last_post_ts else time.time()
        self.logger.debug(f' Order sent at - {self.last_post_ts}')

        # Add to Order Monitor
        self.order_monitor.add(self.order_id)
        self.bot_status = BotStatus.ORDER_SUBMITTED
        cancel_attempt = self.last_post_ts + self.post_frequency
        self.order_monitor.last_cancel_attempt[self.order_id] = cancel_attempt

        self.posts_completed += 1
        return

    def __enter__(self):
        self.order_monitor.start()
        return self

    def __exit__(
        self, exc_type, exc_value, traceback,
    ):
        self.logger.debug(
            f'bot exiting, '
            f'remaining open orders are {self.order_monitor.open_orders}')

        self.order_monitor.cancel_all_open_orders()
        self.order_monitor.stop()
        self.client.deregister_resource_usage(
            "book",
            self.client.exchange_name(self.exchange_name),
            self.pair)
        self.logger.info(
            f'Deregister resource usage: resource_type=book, '
            f'exchange={self.exchange_name}, pair={self.pair}')
