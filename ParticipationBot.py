###############################################################################
# Date: 2022.05
# Author: Rajesh Ladwa
# Email: rajesh.ladwa@altonomy.com
# Github: https://github.com/blockchain/sg-altonomy-apl-bots
# Description: Participation Trading Bot
###############################################################################

import functools
import logging
import random
import time
import json
from contextlib import AbstractContextManager
import traceback

from altonomy.core import client
from altonomy.core.Side import Side

from . import config

from .TWAPBot import TWAPBot, BotStatus

BOT_ACTION_START = "START"
BOT_ACTION_PAUSE = "PAUSE"
BOT_ACTION_RESUME = "RESUME"

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


class ParticipationBot(AbstractContextManager):
    def __init__(
        self,
        account_id,
        base,
        quote,
        bot_id,
        config_param,
        *,
        service_id=None,
        alt_client=None,
        logger=None,
        alt_twap_bot=None,
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
        self.base = base
        self.quote = quote
        self.pair = self.base + self.quote
        self.side = None
        self.total_quantity = 0.0
        self.threshold_price = -1
        self.max_slice_size_multiplier = 5
        self.percentage_of_volume = 7.0
        self.kline_data_duration_sec = 60  # Kline data duration of 1minute
        self.default_post_frequency = 5  # twap will send orders for every 5sec
        self.order_size_rand_range = 0.05
        self.remark = ''
        self.target_quantity = 0.0
        self.target_duration = 0
        self.trigger_condition = ' '
        self.stop_condition = ' '
        self.dealt_qty = 0.0
        self.deal_price = 0.0
        self._action = BOT_ACTION_START
        self.instrument_data = None
        self.om = None
        self.twap_bot = None
        self.config_error = None
        self.last_error = None
        self.bot_status = BotStatus.WAITING
        self.start_time = time.time()
        self.bot_progress_duration = 0
        self.update_redis_ts = time.time()

        self.config = config_param

        self.exchange_name = self.exchange_name_of(self.account_id)
        self.account_position = None
        self.get_account_position()

        if not self.config_error:
            self.initialize_twap_bot(alt_twap_bot)

        self.logger.info(
            f'ParticipationBot started on account {account_id} '
            f'with bot_id {self.bot_id}'
        )

    def initialize_twap_bot(self, alt_twap_bot=None):
        self._update_twap_config()
        if alt_twap_bot:
            self.twap_bot = alt_twap_bot
        else:
            self.twap_bot = TWAPBot(
                self.account_id,
                self.base,
                self.quote,
                self.bot_id,
                self.twap_config,
                alt_client=self.client,
                logger=self.logger,
                service_id=self.service_id)

        self.instrument_data = self.twap_bot.instrument_data
        self.om = self.twap_bot.order_monitor

        self.twap_bot.bot_status = BotStatus.STRATEGY_COMPLETED

        self.om.cancel_all_open_orders()

    def get_account_position(self):
        try:
            self.logger.info('Get account position from Redis')
            initial_position = self.get_position()
            if not initial_position:
                return

            self.account_position = json.loads(
                initial_position.get("Account_Position", None)
            )
            self.logger.debug(f'Account_Position - {self.account_position}')

            if not self.account_position:
                return

            self.logger.debug(
                f'Load POV position Params - {self.account_position}')

            details = self.account_position.get(
                "Progress", None)

            self.logger.debug(f'Progress - {details}')

            status = self.account_position.get(
                "Status", None)
            self.logger.debug(f'Loaded Bot status - {status}')
            status = status.get("Status", "WAITING")

            dealt_qty = float(details.get("Dealt_Qty"))
            total_dealt_qty = float(details.get("Total_Dealt_qty"))
            self.logger.info(
                f'Loaded - dealt_qty = {dealt_qty}'
                f'realtime_dealt_qty = {total_dealt_qty}')
            self.dealt_qty = dealt_qty
        except Exception as e:
            self.logger.error(f'get_account_position - {e}')

    def get_market_history(self):
        try:
            with self.client.zerorpc() as zrpc:
                self.logger.debug(f'zrpc = {zrpc}')
                history = zrpc.get_market_history(
                    self.pair, self.kline_data_duration_sec, 10)
                self.logger.debug(f'history={history}')
                return history
        except Exception as e:
            self.logger.error(f'Error in get_market_history - {e}')
            return None

    def update_market_history_data(self):
        try:
            self.kline_data = []
            history = self.get_market_history()
            if not history or \
                not isinstance(history, list) or \
                    len(history) <= 0:
                return False

            self.kline_data = history
            return True
        except Exception as e:
            self.logger.error(f'Error in update_market_history_data - {e}')
            return False

    def compute_target_qty_duration(self):
        try:
            self.target_quantity = 0.0
            self.target_duration = 0.0

            time_open = self.kline_data[0]['time_open']
            current_time = int(time.time())
            time_difference = current_time - time_open
            self.logger.debug(
                f'current time - {current_time} '
                f'time_open - {time_open} '
                f'time_diff - {time_difference} ')
            if time_difference < self.kline_data_duration_sec:
                # kline data for current min in progress
                total_trd_vol = self.kline_data[1]['amount']
                execution_time_window = \
                    self.kline_data_duration_sec - time_difference
            elif time_difference >= 2 * self.kline_data_duration_sec:
                # kline data for last min is not recieved
                self.logger.warning(
                    'No trades/candle in last session. retrying!!')
                return
            else:
                # kline data for last min
                total_trd_vol = self.kline_data[0]['amount']
                execution_time_window = \
                    self.kline_data_duration_sec * 2 - time_difference

            self.logger.debug(f'total_trd_vol={total_trd_vol}')
            self.logger.debug(f'execution_time_window={execution_time_window}')

            # Pro rate the total traded volume based on execution_time_window
            total_trd_vol = total_trd_vol * execution_time_window \
                / self.kline_data_duration_sec
            self.logger.info(f'pro rated total_trd_vol = {total_trd_vol}')

            # Get pov(defaulted to 7%) of the historic kline data
            if total_trd_vol > 0.0:
                total_trd_vol = \
                    total_trd_vol * self.percentage_of_volume / 100
                self.logger.info(f'pov of total_trd_vol = {total_trd_vol}')

                self.target_quantity = self.apply_randomization(total_trd_vol)
                self.target_duration = execution_time_window
                self.logger.info(
                    f'Target Quantity = {self.target_quantity} '
                    f'Target Duration = {self.target_duration} ')
                return
        except Exception as e:
            self.logger.error(f'Error in compute_target_qty_duration - {e}')
            return

    def apply_randomization(self, quantity):
        self.logger.debug(f'quantity = {quantity}')
        orderquantity = quantity * random.uniform(
            1.0-self.order_size_rand_range, 1.0+self.order_size_rand_range)

        if orderquantity:
            return round(
                orderquantity, self.instrument_data.size_precision)
        else:
            self.logger.error(f'orderquantity in not valid - {orderquantity}')
            return round(quantity, self.instrument_data.size_precision)

    def get_bot_status(self, bot_completed):
        if self.last_error:
            return self.last_error
        if bot_completed:
            return 'COMPLETED'

        return 'RUNNING'

    @property
    def remain_qty(self):
        return self.total_quantity - self.dealt_qty

    @property
    def total_dealt(self):
        if self.om:
            return self.dealt_qty + self.om.dealt
        else:
            return self.dealt_qty

    @property
    def side(self):
        return self._side

    @side.setter
    def side(self, value):
        self._side = Side(value)

    @property
    def action(self):
        return self._action

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
            'threshold_price': self.threshold_price,
            'percentage_of_volume': self.percentage_of_volume,
            'kline_data_duration': int(self.kline_data_duration_sec / 60),
            'default_post_frequency': self.default_post_frequency,
            'remark': self.remark,
            'max_slice_size_multiplier': self.max_slice_size_multiplier,
            'trigger_condition': self.trigger_condition,
            'stop_condition': self.stop_condition
        }

    @config.setter
    def config(self, config: dict):
        self.logger.debug(f'applying config {config}')
        self.config_error = None
        try:
            self.instrument_type = config.get('instrument_type', 'SPOT')
            self.side = config.get('side')
            self.total_quantity = float(config.get('quantity'))
            self.percentage_of_volume = float(
                config.get('percentage_of_volume', '7.0'))
            self.kline_data_duration_sec = int(float(
                config.get('kline_data_duration', '1')) * 60)
            self.default_post_frequency = int(float(
                config.get('default_post_frequency', '5')))
            self.threshold_price = float(config.get('threshold_price', '-1'))
            self.remark = config.get('remark', '')
            self.trigger_condition = config.get('trigger_condition', ' ')
            self.stop_condition = config.get('stop_condition', ' ')
            self.max_slice_size_multiplier = float(
                config.get('max_slice_size_multiplier', '5'))
            self._update_twap_config()  # Update TWAP config is config is updated as well
        except Exception as e:
            self.config_error = e
            self.logger.error('error when setting config')
            self.logger.error(traceback.format_exc())

    @property
    def reason(self):
        if self.last_error is not None:
            return self.last_error

    def _update_twap_config(self):
        self.twap_config = {
            'instrument_type': self.instrument_type,
            'side': self.side,
            'quantity': self.total_quantity,
            'remark': self.remark,
            'threshold_price': self.threshold_price,
            'default_post_frequency': self.default_post_frequency,
            'max_slice_size_multiplier': self.max_slice_size_multiplier,
            'trigger_condition': self.trigger_condition,
            'stop_condition': self.stop_condition,
        }

    def push_bot_data_to_redis(self):
        """ push Participation trading bot data to redis """
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

        if not self.twap_bot:
            return

        _orders = self.twap_bot.om_orders
        if _orders:
            self.client.post(
                f'{config.BOT_OUTPUT_REDIS_KEY}:{self.bot_id}:orders',
                _orders)
        self.logger.debug(f'updated redis for service {self.service_id}')

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

    def get_actual_progress(self):
        if self.total_quantity:
            return round(self.total_dealt * 100 / self.total_quantity, 2)
        return 0

    @property
    def average_price(self):
        if not self.om:
            return -1

        price = self.om.dealt_price or 0.0
        if self.deal_price and self.dealt_qty:
            price = (
                self.deal_price * self.dealt_qty
                + price * self.om.dealt) / (
                self.dealt_qty + self.om.dealt)

        if self.instrument_data:
            return round(price, self.instrument_data.price_precision)
        return -1

    @property
    def bot_position(self):
        return {
            "Balance": self.twap_bot.balance if self.twap_bot else {},
            "Details": {
                "Account": self.name_of(self.account_id),
                "Ticker": self.pair,
                "Side": str(self.side),
            },
            "Progress": {
                "Dealt_Qty": str(self.dealt_qty or 0.0),
                "Total_Qty": str(self.total_quantity),
                "Total_Dealt_qty": str(self.total_dealt or 0.0),
                "Avg_Price": str(self.average_price or -1),
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
        self.logger.debug(f'self.bot_position = {self.bot_position}')

        pos = {
            "Status": self.get_bot_status(bot_completed),
            "Account_Position": json.dumps(self.bot_position)
        }
        self.logger.debug(pos)
        return pos

    def clear_order_monitor(self):
        self.om.completed_orders = {}
        if self.om.open_orders:
            self.logger.error(
                f'Clearing open orders - {self.om.open_orders}')
        self.om.open_orders = {}
        if self.om.failed_orders:
            self.logger.info(
                f'Clearing failed orders - {self.om.failed_orders}')
        self.om.failed_orders = {}

    def run(self):
        if self.bot_status == BotStatus.STRATEGY_COMPLETED:
            self.logger.info('POV Bot strategy is completed !!!')
            time.sleep(5)
            return

        if self.config_error:
            self.last_error = ERR_INVALID_CONFIG_FORMAT
            self.logger.error(f'Error when setting config - {self.config}')
            time.sleep(5)
            return

        if not self.twap_bot.validate_config_parameters():
            self.bot_status = BotStatus.ERROR
            self.last_error = self.twap_bot.last_error
            self.logger.error(
                f'Invalid config parametes !!! - {self.twap_config}')
            time.sleep(5)
            return

        self.logger.debug(
            f'POV bot last dealt - {self.dealt_qty}'
            f' with {self.remain_qty} remaining')

        self.logger.debug(
            f'self.target_duration - {self.target_duration}'
            f'self.twap_bot.bot_status - {self.twap_bot.bot_status}')
        if ((self.twap_bot.bot_status == BotStatus.STRATEGY_COMPLETED)
                or
                (self.twap_bot.start_time
                    + self.target_duration < time.time())):
            self.twap_bot.cancel_pending_order()
            self.logger.info(
                f'TWAP dealt - {self.om.dealt}'
                f'TWAP quantity - {self.twap_config.get("quantity")}'
                f'POV Total dealt - {self.dealt_qty}')
            self.dealt_qty += self.om.dealt
            self.deal_price = self.average_price

            self.clear_order_monitor()

            self.logger.info(
                f'Current POV Total dealt - {self.dealt_qty} '
                f'Current POV Remaining Qty - {self.remain_qty} ')
            if self.remain_qty == 0.0 or \
                    self.remain_qty < self.twap_bot.min_order_qty:
                self.bot_status = BotStatus.STRATEGY_COMPLETED
                self.logger.info(f'Not much qty remaining - {self.remain_qty}')
                return

            if not self.update_market_history_data():
                self.bot_status = BotStatus.ERROR
                self.last_error = "Error in Market History (kline) Data"
                self.logger.error(
                    f'Invalid KLine data  - {self.kline_data}')
                time.sleep(1)
                return

            self.compute_target_qty_duration()
            if not self.target_quantity or not self.target_duration:
                self.logger.warning('Target qty / duration not computed')
                time.sleep(1)
                return

            # Start TWAP startegy with new Qty
            if self.remain_qty < self.target_quantity:
                # Pro rate the target duration
                self.target_duration = self.remain_qty * self.target_duration \
                    / self.target_quantity
                self.target_quantity = self.remain_qty
            self.target_duration = max(
                    self.target_duration, self.default_post_frequency)
            self.twap_config['quantity'] = self.target_quantity
            self.twap_config['duration'] = self.target_duration
            self.twap_bot.reset_twap_strategy()
            self.logger.info(
                f'Restarting twap startegy '
                f' @ {int(time.time())}'
                f' for qty - {self.twap_config.get("quantity")}'
                f' duration - {self.twap_config.get("duration")}')
            self.twap_bot.config = self.twap_config
            self.twap_bot.calculate_strategy_params(
                self.twap_config.get('quantity'),
                self.twap_config.get('duration'))
            self.twap_bot.bot_status = BotStatus.WAITING
            self.last_error = None

        self.bot_status = self.twap_bot.bot_status
        self.twap_bot.run()
        self.last_error = self.twap_bot.reason
        time.sleep(1)

    def __enter__(self):
        self.logger.debug('Participation bot entring!!')
        if self.om:
            self.om.start()
        return self

    def __exit__(
        self, exc_type, exc_value, traceback,
    ):
        self.logger.debug('Participation bot exiting!!')
        self.om.cancel_all_open_orders()
        self.om.stop()
        self.client.deregister_resource_usage(
            "book",
            self.client.exchange_name(self.exchange_name),
            self.pair)
        self.logger.info(
            f'Deregister resource usage: resource_type=book, '
            f'exchange={self.exchange_name}, pair={self.pair}')
