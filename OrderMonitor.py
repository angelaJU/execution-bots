from collections import defaultdict
import contextlib
import threading
import time
import traceback
from altonomy.core.client import client
from altonomy.core.Order import OrderState, Order
from altonomy.core.Side import BUY, SELL
from altonomy.core.exceptions import ErrorCode


class OrderMonitor(threading.Thread, contextlib.AbstractContextManager):
    def __init__(
        self,
        alt_client: client,
        logger,
        try_cancels=0,
        refresh_interval=0.2,
        try_cancel_interval=10,
    ):
        super().__init__(name='OrderMonitor')
        self.open_orders = {}
        self.completed_orders = {}
        self.failed_orders = {}
        self.total_dealt_by_side = {str(BUY): 0.0, str(SELL): 0.0}
        self.total_dealt_notional_by_side = {str(BUY): 0.0, str(SELL): 0.0}
        self.starting_dealt = 0.0
        self.starting_price = 0.0
        self.client = alt_client
        self.logger = logger
        self.stop_flag = threading.Event()
        self.refresh_interval = refresh_interval
        self.try_cancels = try_cancels
        self.try_cancel_interval = try_cancel_interval
        self.cancel_count = defaultdict(int)
        self.last_cancel_attempt = defaultdict(int)
        self.lock = threading.Lock()

    def initialise_starting_position(self, open_orders, total_dealt_by_side, total_dealt_notional_by_side):
        if open_orders:
            self.open_orders = open_orders
        if total_dealt_by_side:
            self.total_dealt_by_side = total_dealt_by_side
        if total_dealt_notional_by_side:
            self.total_dealt_notional_by_side = total_dealt_notional_by_side
        if total_dealt_by_side and total_dealt_notional_by_side:
            self.starting_dealt = sum(total_dealt_by_side.values())
            self.starting_price = sum(total_dealt_notional_by_side.values())/sum(total_dealt_by_side.values()) if sum(total_dealt_by_side.values()) else 0.0
        
        self.logger.info(f'self.open_orders = {self.open_orders}')
        self.logger.info(f'self.total_dealt_by_side = {self.total_dealt_by_side}')
        self.logger.info(f'self.total_dealt_notional_by_side = {self.total_dealt_notional_by_side}')
        self.logger.info(f'self.starting_dealt = {self.starting_dealt}')
        self.logger.info(f'self.starting_price = {self.starting_price}')

    def run(self):
        self.logger.info('Thread for OrderMonitor started')
        while not self.stop_flag.is_set():
            try:
                for order_id in list(self.open_orders):
                    self.logger.debug(f'OrderMonitor checking {order_id}')
                    order = self.client.get_order_details(
                        order_id=order_id, force_refresh=True
                    )
                    order.pop('raw', None) # raw message detail is not required
                    self.logger.debug(f'OrderMonitor got order {order_id}: {order}')
                    with self.lock:
                        if order.completed or order.canceled:
                            self.logger.debug(
                                f'OrderMonitor deem {order_id} as complete'
                            )
                            self.open_orders.pop(order_id, None)
                            if order.dealt > 0:
                                self.completed_orders[order_id] = self.completed_order(order)
                                self.logger.debug(
                                    f'Added in completed orders {order_id}'
                                )
                                self.total_dealt_by_side[str(order.side)] += order.dealt
                                self.total_dealt_notional_by_side[str(order.side)] += order.dealt * order.price
                        elif (
                            order.failed(exchange_response_timeout=300)
                            or self.cancel_count[order_id] > self.try_cancels
                        ):
                            self.logger.debug(
                                f'OrderMonitor deem {order_id} as in invalid state'
                            )
                            self.open_orders.pop(order_id, None)
                            self.failed_orders[order_id] = order
                            if order.dealt > 0:
                                self.completed_orders[order_id] = self.completed_order(order)
                                self.total_dealt_by_side[str(order.side)] += order.dealt
                                self.total_dealt_notional_by_side[str(order.side)] += order.dealt * order.price
                        else:
                            if order:
                                self.open_orders[order_id] = order
                            if (
                                self.try_cancels > 0
                                and order.state != OrderState.SENDING
                                and time.time()
                                > self.last_cancel_attempt[order_id]
                                + self.try_cancel_interval
                            ):
                                self.logger.debug(f'OrderMonitor canceling {order_id}')
                                self.client.cancel(order_id)
                                self.last_cancel_attempt[order_id] = time.time()
                                self.cancel_count[order_id] += 1
                    time.sleep(self.refresh_interval)
            except:
                self.logger.error(f'OrderMonitor error: {traceback.format_exc()}')
                time.sleep(5)
            time.sleep(self.refresh_interval)

    @property
    def dealt(self):
        with self.lock:
            return self.starting_dealt + sum(
                order.dealt for order in self.completed_orders.values() if order is not None
            )

    def _get_partially_dealt_by_side(self, side):
        return sum(
            order.dealt for order in self.open_orders.values() if order is not None and str(order.side) == side
        )

    def get_total_dealt_by_side(self, side):
        with self.lock:
            return self.total_dealt_by_side[side] + self._get_partially_dealt_by_side(side)

    def get_average_price(self, side):
        with self.lock:
            return self.total_dealt_notional_by_side[side] / self.total_dealt_by_side[side] if self.total_dealt_by_side[side] > 0 else -1

    def get_failed_order_error_code(self, order_id):
        with self.lock:
            if order_id in self.failed_orders:
                error_code = self.failed_orders[order_id].reason
                if error_code in ErrorCode.code_reason:
                    return ErrorCode.code_reason[error_code]
                else:
                    return error_code
            return ''

    def is_failed_order(self, order_id):
        with self.lock:
            return order_id in self.failed_orders

    def is_completed_order(self, order_id):
        with self.lock:
            return order_id in self.completed_orders

    def get_remaining_qty(self, order_id):
        with self.lock:
            order = self.completed_orders[order_id]
            self.logger.info(
                f' dealt {order.dealt} , remaining {order.remaining}')
            return order.remaining

    def cancel_all_open_orders(self):
        with self.lock:
            for order_id, order in self.open_orders.items():
                self.logger.info(
                    f'OrderMonitor - Cancelling order {order_id} : {order}')
                self.client.cancel(order_id=order_id)

    def get_latest_update_time(self, account_id):
        with self.lock:
            return (
                o.update_time
                for o in self.orders.values() if o.account_id == account_id and
                o.update_time
                )

    @property
    def _partially_dealt(self):
        return sum(
            order.dealt for order in self.open_orders.values() if order is not None
        )

    @property
    def total_dealt(self):
        with self.lock:
            return sum(dealt for dealt in self.total_dealt_by_side.values()) + self._partially_dealt

    @property
    def dealt_price(self):
        _dealt = self.dealt
        with self.lock:
            return (
                (self.starting_dealt * self.starting_price + sum(
                    order.price * order.dealt
                    for order in self.completed_orders.values()
                    if order is not None
                ))
                / _dealt
                if _dealt > 0
                else None
            )

    @property
    def pending(self):
        with self.lock:
            return sum(
                order.amount for order in self.open_orders.values() if order is not None
            )

    @property
    def orders(self):
        return {**self.open_orders, **self.completed_orders}

    def set_try_cancel_interval(self, value):
        self.try_cancel_interval = value

    def completed_order(self, order):
        return Order({
            "price": order.price,
            "amount": order.amount,
            "dealt": order.dealt,
            "update_time": order.update_time,
            "account_id": order.account_id,
        })

    def add(self, order_id):
        with self.lock:
            self.logger.debug(f'OrderMonitor added order {order_id}')
            self.open_orders[order_id] = self.client.get_order_details(
                order_id=order_id
            )
            self.open_orders[order_id].pop('raw', None) # raw message detail is not required
        self.last_cancel_attempt[order_id] = time.time()

    def delete(self, order_id):
        with self.lock:
            self.logger.debug(f'OrderMonitor deleting order {order_id}')
            self.open_orders.pop(order_id, None)

    def stop(self):
        self.stop_flag.set()

    def __enter__(self):
        self.start()
        return self

    def __exit__(
        self,
        exc_type,
        exc_value,
        traceback,
    ):
        print('OrderMonitor exiting')
        self.logger.error(traceback.format_exc())
        self.stop()
