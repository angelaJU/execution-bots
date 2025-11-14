from collections import defaultdict
import functools
import logging
import math
import time
import traceback
from contextlib import AbstractContextManager, contextmanager
from datetime import datetime
from threading import Lock
from typing import Iterable

import cachetools

from .OrderMonitor import OrderMonitor
import altonomy.core.Streams as Streams
from altonomy.core.OrderBook import OrderBook, UDSOrderBook
from altonomy.core import Streams as Streams
from altonomy.core import client
from altonomy.core.Side import BUY, SELL, Side
# from graphenebase import account


class SweeperBot(AbstractContextManager):
    def __init__(
        self,
        account_ids: Iterable,
        base,
        quote,
        *,
        service_id=None,
        alt_client=None,
        logger=None,
    ):
        self._total_amount = None
        self.logger = logger or logging.getLogger()
        self.accounts = account_ids
        self.max_price = None
        self.min_price = None
        self.base = base
        self.quote = quote
        self.client = alt_client or client(logger=self.logger)
        if service_id:
            self.client.service_id = service_id
        self.side = None
        self.max_slippage_threshold = None
        self.delay = 2
        self.order_monitor = OrderMonitor(self.client, self.logger, try_cancels=25)
        self.streams = self.subscribe_order_books()
        self.logger.info(f'SweeperBot started on accounts {account_ids}')

    @property
    def total_amount(self):
        return self._total_amount

    @total_amount.setter
    def total_amount(self, amount):
        amount = float(amount)
        if amount > 0:
            self._total_amount = amount
        else:
            raise ValueError(f'total amount {amount} must be a number great than 0')

    @property
    def remaining_amount(self):
        return self.total_amount - self.order_monitor.dealt

    @property
    def side(self):
        return self._side

    @side.setter
    def side(self, value):
        self._side = value

    @property
    def remark(self):
        return self._remark

    @remark.setter
    def remark(self, value):
        self._remark = value

    @property
    def pair(self):
        return self.base + self.quote

    @property
    def config(self):
        return {
            'totalAmount': self.total_amount,
            'side': self.side,
            'remark': self.remark,
        }

    @config.setter
    def config(self, config: dict):
        self.logger.debug(f'applying config {config}')
        self.config_error = None
        try:
            self.total_amount = config.get('totalAmount')
            self.side = Side(config.get('side'))
            self.remark = config.get('remark')
        except Exception as e:
            self.config_error = e
            self.logger.error('error when setting config')
            self.logger.error(traceback.format_exc())

    @property
    def info(self):
        return {
            'timestamp': datetime.utcnow(),
            'total': self.total_amount,
            'dealt': self.order_monitor.dealt,
            'dealt_price': self.order_monitor.dealt_price,
            'open_orders': self.order_monitor.open_orders,
            'completed_orders': self.order_monitor.completed_orders,
        }

    def send_order(self, price, size, *args, account_id, **kwargs):
        func = self.client.buy if self.side == BUY else self.client.sell
        self.logger.debug(
            f'sending {self.side} order {size}@{price} for {self.pair} on {account_id}'
        )
        return func(
            self.pair, price=price, size=size, *args, account_id=account_id, **kwargs
        )

    @contextmanager
    def subscribe_order_books(self):
        cached_order_books = {}

        def subscribe_order_book(account_id):
            def cache_order_book(*args):
                try:
                    was_invalid_book = cache_order_book.was_invalid_book
                except AttributeError:
                    cache_order_book.was_invalid_book = False
                    was_invalid_book = False
                lock = Lock()
                ob = UDSOrderBook(args, source=account_id)
                if ob.status != 1:
                    if not was_invalid_book:
                        self.logger.error(
                            f'order book feed for {account_id} is not valid'
                        )
                        cache_order_book.was_invalid_book = True
                    return
                with lock:
                    cache_order_book.was_invalid_book = False
                    cached_order_books[account_id] = ob

            self.logger.debug(
                f'subscribing to order book of {self.exchange_name_of(account_id)}'
            )
            return self.client.subscribe_streams(
                [
                    [
                        self.exchange_name_of(account_id),
                        self.pair,
                        Streams.l2_detailed,
                        cache_order_book,
                        # {'refresh_rate': 0.2},
                    ]
                ]
            )

        exit_flags = [subscribe_order_book(account_id) for account_id in self.accounts]
        try:
            while len(cached_order_books) < max(len(self.accounts) // 2, 1):
                self.logger.debug(
                    f'waiting for at least {max(len(self.accounts) // 2, 1)}'
                    f'streams to be ready, currently {len(cached_order_books)}'
                )
                time.sleep(0.05)
            yield cached_order_books
        finally:
            for exit_flag in exit_flags:
                exit_flag.set()

    @functools.lru_cache(maxsize=None)
    def exchange_name_of(self, account_id):
        return self.client.get_account_config('exchange_name', account_id=account_id)

    @functools.lru_cache(maxsize=None)
    def exchange_id_of(self, account_id):
        return self.client.get_account_config('exchange_id', account_id=account_id)

    @functools.lru_cache(maxsize=None)
    def name_of(self, account_id):
        return self.client.get_account_config('name', account_id=account_id)

    @property
    def completed(self):
        return self.order_monitor.dealt > self.total_amount or math.isclose(
            self.total_amount, self.order_monitor.dealt, abs_tol=0.0001
        )

    def remaining_balance_for_order(self, balance, order_price, order_amount):
        """ remaining account balance for the order denoted in base currency """
        if self.side == SELL:
            start_bal = balance[self.base].available
        elif self.side == BUY:
            start_bal = balance[self.quote].available / order_price
        else:
            raise ValueError
        self.logger.debug(f'start_bal: {start_bal} order amt: {order_amount}')
        if math.isclose(start_bal, order_amount, rel_tol=0.001):
            return 0
        else:
            return start_bal - order_amount

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

    @property
    def config_is_valid(self):
        if self.config_error:
            self.logger.error('invalid config due to error when setting config')
            return False
        try:
            if self.total_amount <= 0:
                self.logger.error('total amount must be more than 0')
                return False
            if self.side != BUY and self.side != SELL:
                self.logger.error('order side must be buy or sell')
                return False
            return True
        except:
            self.logger.error(traceback.format_exc())
            return False

    def run(self):
        if not self.config_is_valid:
            self.logger.error(f'not running due to invalid config {self.config}')
            time.sleep(self.delay)
            return

        if self.completed:
            time.sleep(self.delay)
            self.logger.info(f'bot finished sending orders')
            self.order_monitor.stop()
            self.streams.__exit__(None, None, None)
            return

        self.logger.info('starting run cycle')
        time.sleep(0.05)
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
            time.sleep(0.05)
            return
        self.logger.debug(
            f'pending {self.order_monitor.pending}, dealt {self.order_monitor.dealt}'
        )
        lock = Lock()
        with lock:
            # check order books if they have timed out
            order_books_valid = {}
            for account_id in list(self.cached_order_books.keys()):
                # pop the order book so it's not re-used without an update from the same exchange
                ob = self.cached_order_books.pop(account_id)
                self.logger.debug(f'account_id {account_id} order book {ob}')
                if ob.source in (
                    o.account_id for o in self.order_monitor.open_orders.values()
                ):
                    self.logger.debug(f'ignoring book due to pending orders')

                delay_threshold = self.order_book_delay_threshold(account_id)
                if len(ob.bids) == 0 and len(ob.asks) == 0:
                    # ignore empty books, probably an error
                    continue
                if not ob.timestamp:
                    # ignore books without timestamps, probably an error
                    continue
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
                    self.logger.debug(
                        f'waiting for order book update for {account_id} for recently completed orders'
                    )
                    continue
                delay = datetime.utcnow().timestamp() - ob.timestamp

                self.logger.debug(f'book delay is {delay}')

                if delay < delay_threshold:
                    order_books_valid[account_id] = ob
                else:
                    self.logger.debug(
                        f'order book for account_id {self.name_of(account_id)} invalid as '
                        f'order book delay of {delay} '
                        f'is more than {delay_threshold} '
                    )
            if not order_books_valid:
                self.logger.debug(f'no order books within threshold, not progressing')
                return
            else:
                self.logger.debug(f'valid books: {order_books_valid}')
                merged_book = OrderBook.merge(*order_books_valid.values())
                self.logger.debug(f'merged book {merged_book}')

        if self.side == BUY:
            price_levels = merged_book.asks
        elif self.side == SELL:
            price_levels = merged_book.bids
        else:
            raise ValueError
        account_balances = {
            account_id: self.client.get_account_balance(account_id)
            for account_id in self.accounts
        }

        order_amounts = defaultdict(float)
        order_prices = {}

        total_order_size = self.remaining_amount - self.order_monitor.pending

        for price_level in price_levels:
            if math.isclose(
                sum(order_amounts.values()), total_order_size, abs_tol=0.001
            ):
                self.logger.debug(f'finished generating all orders')
                break
            elif sum(order_amounts.values()) >= total_order_size:
                self.logger.error(
                    f'illegal state, somehow queued more order amounts than required'
                )
                raise Exception
            else:
                self.logger.debug(f'queued order size {sum(order_amounts.values())}')

            self.logger.debug(f'processing {price_level}')

            remaining_order_capacity = self.remaining_balance_for_order(
                account_balances[price_level.source],
                order_price=order_prices.get(price_level.source, price_level.price),
                order_amount=order_amounts[price_level.source],
            )

            if remaining_order_capacity == 0:
                self.logger.debug(
                    f'account balance of {price_level.source} assumed depleted '
                    f'at {account_balances[price_level.source]} '
                    f'with order size {order_amounts[price_level.source]}@{order_prices.get(price_level.source)} queued'
                )
                continue
            self.logger.debug(
                f'using lowest amount from {price_level.amount}/'
                f'{remaining_order_capacity}/'
                f'{total_order_size - sum(order_amounts.values())}'
            )
            order_amounts[price_level.source] += min(
                price_level.amount,
                remaining_order_capacity,
                total_order_size - sum(order_amounts.values()),
            )
            order_prices[price_level.source] = price_level.price

        for account_id, amount in order_amounts.items():
            if account_id not in order_prices or amount == 0:
                continue
            price = order_prices[account_id]
            order_id = self.send_order(
                price, amount, account_id=account_id, remark=self.remark
            )
            self.order_monitor.add(order_id)

    def __enter__(self):
        self.order_monitor.start()
        self.cached_order_books = self.streams.__enter__()
        return self

    def __exit__(
        self, exc_type, exc_value, traceback,
    ):
        self.streams.__exit__(None, None, None)
        self.order_monitor.stop()
        self.logger.debug(
            f'bot exiting, remaining open orders are {self.order_monitor.open_orders}'
        )
