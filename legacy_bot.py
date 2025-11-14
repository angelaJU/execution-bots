import gc
import json
import time
import traceback
import atexit, signal
from datetime import datetime, timedelta, timezone
from sqlalchemy import orm, schema, types

from altonomy.models import Service
from altonomy.services.background_process import background_process
from numpy.core.fromnumeric import trace

from . import config
from .ExecutionBot import ExecutionBot
from .LiquidityEnhancerBot import LiquidityEnhancerBot
from .LiquidityEnhancerPlusBot import LiquidityEnhancerPlusBot
from .MarketMakerBot import MarketMakerBot
from .SniperBot import SniperBot
from .SweeperBot import SweeperBot
from .VolumeBot import VolumeBot
from .PairTradingBot import PairTradingBot
from .TWAPBot import TWAPBot
from .ParticipationBot import ParticipationBot


def json_serialise(data_object: dict) -> dict:
    d = {}
    for k, v in data_object.items():
        if isinstance(v, dict) or isinstance(v, list):
            d[k] = json.dumps(v)
        elif v is None or isinstance(v, datetime):
            d[k] = str(v)
        else:
            d[k] = v
    return d


class legacy_bot(background_process):
    def __init__(self, broker=None, logger=None):
        super().__init__(logger=logger)
        self.pair = ''
        self.altcoin = ''
        self.quotecoin = ''
        self.exchange_name = ''
        self.bid_layers = 0
        self.ask_layers = 0
        self.start_redis()
        self.start_mysql()
        self.session = orm.sessionmaker(bind=self.db, autoflush=True, autocommit=False, expire_on_commit=True)
        self.broker = broker
        self.configuration = {}
        self.bot = object
        self.method = ''
        self.is_stopping = False
        self.bot_action_config_refresh_ts = 0
        signal.signal(signal.SIGTERM, self.terminate_handler)
        self.logger.debug("Initialized legacy_bot.")

    def terminate_handler(self, signum=None, frame=None):
        self.logger.info("Stopping the bot upon terminate signal.")
        self.is_stopping = True

    def _load_data(self, parent_input):
        """ Load session data """
        self.pair = parent_input.get('pair')
        self.altcoin = parent_input.get('altcoin','')
        self.quotecoin = parent_input.get('quotecoin','')
        self.exchange_name = parent_input.get('exchange_name', '')
        self.configuration = parent_input.get('config', '')
        self.service_id = parent_input.get('service_id', 0)
        self.bot_id = parent_input.get('bot_id', 0)
        self.logger.info(f"In Load session data with {self.bot_id}")

    """
    Execution Bot Methods

    """
    def _push_execution_data_to_redis(self):
        """ push execution bot data to redis """
        r, bot, service_id = self.redis, self.bot, self.service_id

        _balance = {}
        _market = {}
        _position = {}

        altcoinfrozenbalance = bot.broker.get_coin_frozen_balance(bot.altcoin)
        altcointradablebalance = bot.broker.get_coin_tradable_balance(bot.altcoin)
        quotecoinfrozenbalance = bot.broker.get_coin_frozen_balance(bot.quotecoin)
        quotecointradablebalance = bot.broker.get_coin_tradable_balance(bot.quotecoin)
        _account_id = bot.broker.get_account_id()
        _balance.update({'timestamp': str(datetime.utcnow()),
                    str(_account_id):
                        {'altfree': altcointradablebalance,
                        'altfrozen': altcoinfrozenbalance,
                        'quotefree': quotecointradablebalance,
                        'quotefrozen': quotecoinfrozenbalance}})
        if _balance: r.hset(f'{config.BOT_OUTPUT_REDIS_KEY}:{bot.bot_id}:balance', mapping=json_serialise(_balance))

        bid1 = bot.broker.get_highest_bid_and_volume(bot.tradingpair)
        ask1 = bot.broker.get_lowest_ask_and_volume(bot.tradingpair)
        _market.update({'timestamp': str(datetime.utcnow()),
            'orderbook': {
                'bid1': {
                    'price' : bid1.price,
                    'amount' : bid1.amount,
                    'cumulative' : bid1.cumulative,
                    'slippage' : bid1.slippage,
                    'source' : bid1.source
                    }, 
                'ask1': {
                    'price' : ask1.price,
                    'amount' : ask1.amount,
                    'cumulative' : ask1.cumulative,
                    'slippage' : ask1.slippage,
                    'source' : ask1.source
                    }, 
            }
        })
        if _market: r.hset(f'{config.BOT_OUTPUT_REDIS_KEY}:{bot.bot_id}:market', mapping=json_serialise(_market))

        _position = bot.position
        if _position: r.hset(f'{config.BOT_OUTPUT_REDIS_KEY}:{bot.bot_id}:position', mapping=json_serialise(_position))

        self.logger.debug(f'updated redis for service {service_id} (bot {bot.bot_id}) - {_balance} {_market} {_position}')

    def execution_bot(self, parent_input):
        """ run legacy execution bot """
        _method_internal_name = f'legacy bot (execution)'
        self.method = 'execution_bot'
        try:
            self.logger.debug(f'starting {_method_internal_name} with parent_input = {parent_input}')
            self._load_data(parent_input)
            self.logger.debug(f'loaded configuration')

            exchange_name = self.exchange_name
            input_prefix, output_prefix, broker = 'legacy_bot', 'legacy_bot', self.broker
            altcoin, quotecoin, configuration, bot_id = self.altcoin, self.quotecoin, self.configuration, self.bot_id
            pair = altcoin + quotecoin

            r, logger, service_id = self.redis, self.logger, self.service_id

            broker.reconfig()
            broker.client.broadcast_chan = f"{config.BROADCAST_CHANNEL}-input"
            bot = ExecutionBot(broker, altcoin, quotecoin, bot_id, configuration, logger=logger)

            self.bot = bot
            self.logger.info(f'{_method_internal_name} for {output_prefix}:{exchange_name}:{pair} started with {self.bot_id}')
            self.action = "START"
            self.write_status_to_db("Running")
            try:
                while not self.is_stopping:
                    gc.collect()
                    if int(time.time() - self.bot_action_config_refresh_ts) > config.BOT_ACTION_CONFIG_REFRESH_TS:
                        try:      
                            current_time = time.time()
                            self.action = r.get(f'{config.BOT_ACTION_COMMAND_KEY}:{self.bot_id}').upper() or "START"
                            self.logger.debug(f'action from redis = {self.action} for bot_id {self.bot_id}')
                            configuration = r.hgetall(f'{config.BOT_CONFIG_KEY}:{self.bot_id}')
                            if configuration != {}:
                                if float(configuration["last_updated"]) > self.bot_action_config_refresh_ts:
                                    logger.info(f'received updated configuration: {configuration}')
                                    bot.load_execution_configuration(configuration)
                            else:
                                logger.error(f'aborting update as no configuration received')
                        except Exception as e:
                            logger.error(f"failed to update configuration")
                            logger.error(f"{e}")
                        finally:
                            self.bot_action_config_refresh_ts = current_time

                    if self.action == "PAUSE":
                        logger.info(f'{_method_internal_name} is currently paused')
                        time.sleep(1)
                    else:
                        try:
                            if bot.tick():

                                if configuration.get('order_direction') == 'BUY' and\
                                        configuration.get('execution_strategy') == 'VANILLA':
                                    execution_send_order = bot.execute_buyorder_vanilla
                                elif configuration.get('order_direction') == 'SELL' and \
                                        configuration.get('execution_strategy') == 'VANILLA':
                                    execution_send_order = bot.execute_sellorder_vanilla
                                elif configuration.get('order_direction') == 'BUY' and \
                                        configuration.get('execution_strategy') == 'STRAWBERRY':
                                    execution_send_order = bot.execute_buyorder_strawberry
                                elif configuration.get('order_direction') == 'SELL' and \
                                        configuration.get('execution_strategy') == 'STRAWBERRY':
                                    execution_send_order = bot.execute_sellorder_strawberry
                                else:
                                    logger.error("Invalid order direction or strategy!\n")
                                    return False

                                execution_send_order()
                                bot.generate_execution_report()
                                self._push_execution_data_to_redis()
                        except Exception as e:
                            logger.warning(f"encountered error during regular tick cycle - {e}")
                            logger.warning(traceback.format_exc())
                        finally:
                            pass
                    time.sleep(0.01)
            finally:
                logger.debug(f'terminating {_method_internal_name} for {output_prefix}:{exchange_name}:{pair}')

                # carry out any actions here
                bot.exit_processing()

        except Exception as e:
            self.logger.error(f"{e}")
        finally:
            self.write_status_to_db("Exited")
            self.logger.info(f'{_method_internal_name} for {output_prefix}:{exchange_name}:{pair} terminated')


    """
    Sniper Bot
    """
    
    def sniper_bot(self, parent_input):
        try:
            self.method = 'sniper_bot'
            self.logger.info('starting sniper bot')
            self._load_data(parent_input)
            with SniperBot(self.account_ids, self.altcoin, self.quotecoin, self.bot_id, self.configuration, service_id=self.service_id, logger=self.logger) as bot:
                self.logger.info(f'started sniper bot {self.bot_id}')
                heartbeat_id = f'sniper_bot_{self.altcoin}_{self.quotecoin}_{self.service_id}'
                bot.client.register_dead_man_switch(heartbeat_id, 1800)
                self.action = "START"
                r = self.redis
                self.write_status_to_db("Running")
                while not self.is_stopping:
                    gc.collect()
                    bot.client.send_heartbeat(heartbeat_id, 1800)
                    self.logger.info(f'in while sniper bot {self.bot_id}')
                    if int(time.time() - self.bot_action_config_refresh_ts) > config.BOT_ACTION_CONFIG_REFRESH_TS:
                        try:
                            current_time = time.time()
                            self.action = r.get(f'{config.BOT_ACTION_COMMAND_KEY}:{self.bot_id}') or "START"
                            self.logger.debug(f'action from redis = {self.action} for bot_id {self.bot_id}')
                            configuration = r.hgetall(f'{config.BOT_CONFIG_KEY}:{self.bot_id}')
                            if configuration != {}:
                                if float(configuration["last_updated"]) > self.bot_action_config_refresh_ts:
                                    self.logger.info(f'received updated configuration: {configuration}')
                                    bot.config = configuration
                            else:
                                self.logger.warn(f'aborting update as no configuration received')
                        except Exception as e:
                                self.logger.error(f"failed to update configuration - {e}")
                        finally:
                            self.bot_action_config_refresh_ts = current_time

                    if self.action == "PAUSE":
                        self.logger.info('sniper bot is currently paused')
                        time.sleep(1)
                    else:
                        bot.run()
                    self.redis.hset(f'{config.BOT_OUTPUT_REDIS_KEY}:{self.bot_id}:position', mapping=json_serialise(bot.info))
                    self.logger.debug(f'bot.info = {bot.info}')
                    time.sleep(config.LEGACY_BOT_SLEEP_INTERVAL)

                bot.client.unregister_dead_man_switch(heartbeat_id)
        except Exception as e:
            self.logger.error('uncaught exception during sniper_bot run cycle - {e}')
            self.logger.error(traceback.format_exc())
        finally:
            self.write_status_to_db("Exited")
            self.logger.info(f'sniper_bot terminated')


    def write_status_to_db(self, status):
        try:
            session = orm.scoped_session(self.session)
            try:
                service = session.query(Service).filter(Service.id == self.service_id).first()
                self.logger.debug(f"found record on query {session}")
                if service is not None:
                    self.logger.debug(f"found non-empty service {service.id} {service.name} {service.service_type_id} {service.status}")
                    service.status = status
                    if status.lower() == "exited":
                        service.end_time = datetime.now(tz=timezone.utc)
                    session.commit()
                    self.logger.info(f"successfully updated bot status to : {status}")
            except Exception as e:
                self.logger.error(f"failed to query db | {e}")
            finally:
                session.remove()
        except Exception as e:
            self.logger.error(f"failed to load session in scope | {e}")


    """
    PairTrading Bot
    """
    def _push_pair_trading_data_to_redis(self, update_config=False):
        """ push pair trading bot data to redis """
        r, bot, service_id = self.redis, self.bot, self.service_id

        self.logger.debug(f'updating redis for service {service_id}')

        _position = bot.position
        if _position: r.hset(f'{config.BOT_OUTPUT_REDIS_KEY}:{self.bot_id}:position', mapping=json_serialise(_position))

        
        self.logger.debug(f'updated redis for service {service_id}')

    
    def pair_trading_bot(self, parent_input):
        try:
            self.method = 'pair_trading_bot'
            self.logger.info('starting pair trading bot')
            self._load_data(parent_input)

            with PairTradingBot(self.account_ids, self.altcoin, self.quotecoin, self.bot_id, self.configuration, service_id=self.service_id, logger=self.logger) as bot:
                self.bot = bot
                self.logger.info('started pair trading bot')
                self._push_pair_trading_data_to_redis()

                heartbeat_id = f'pair_trading_bot_{self.altcoin}_{self.quotecoin}_{self.service_id}'
                bot.client.register_dead_man_switch(heartbeat_id, 1800)
                self.action = "START"
                r = self.redis
                self.write_status_to_db("Running")
                while not self.is_stopping:
                    gc.collect()
                    bot.client.send_heartbeat(heartbeat_id, 1800)
                    if int(time.time() - self.bot_action_config_refresh_ts) > config.BOT_ACTION_CONFIG_REFRESH_TS:
                        try:
                            current_time = time.time()
                            self.action = r.get(f'{config.BOT_ACTION_COMMAND_KEY}:{self.bot_id}') or "START"
                            self.logger.debug(f'action from redis = {self.action} for bot_id {self.bot_id}')
                            configuration = r.hgetall(f'{config.BOT_CONFIG_KEY}:{self.bot_id}')
                            if configuration != {}:
                                if float(configuration["last_updated"]) > self.bot_action_config_refresh_ts:
                                    self.logger.info(f'received updated configuration: {configuration}')
                                    bot.config = configuration
                            else:
                                self.logger.warn(f'aborting update as no configuration received')
                        except Exception as e:
                                self.logger.error(f"failed to update configuration - {e}")
                        finally:
                            self.bot_action_config_refresh_ts = current_time
                    if self.action == "PAUSE":
                        self.logger.info('pair_trading_bot is currently paused')
                        time.sleep(1)
                    else:
                        bot.run()
                        self._push_pair_trading_data_to_redis()
                    time.sleep(config.LEGACY_BOT_SLEEP_INTERVAL)

                bot.client.unregister_dead_man_switch(heartbeat_id)
        except:
            self.logger.error('uncaught exception during pair_trading_bot run cycle')
            self.logger.error(traceback.format_exc())
        finally:
            self.write_status_to_db("Exited")
            self.logger.info(f'pair_trading_bot terminated')

    '''
    TWAP Bot
    '''
    def twap_bot(self, parent_input):
        try:
            self.method = 'twap_bot'
            self.logger.info('starting twap bot')
            self._load_data(parent_input)

            with TWAPBot(self.account_ids[0], self.altcoin, self.quotecoin, self.bot_id, self.configuration, service_id=self.service_id, logger=self.logger) as bot:
                self.bot = bot
                self.logger.info('starting twap bot from Legacy Bot')
                heartbeat_id = f'twap_bot_{self.altcoin}_{self.quotecoin}_{self.service_id}'
                bot.client.register_dead_man_switch(heartbeat_id, 1800)
                self.action = "START"
                r = self.redis
                self.write_status_to_db("Running")
                while not self.is_stopping:
                    gc.collect()
                    bot.client.send_heartbeat(heartbeat_id, 1800)
                    if int(time.time() - self.bot_action_config_refresh_ts) > config.BOT_ACTION_CONFIG_REFRESH_TS:
                        try:
                            current_time = time.time()
                            self.action = r.get(f'{config.BOT_ACTION_COMMAND_KEY}:{self.bot_id}') or "START"
                            self.logger.debug(f'action from redis = {self.action} for bot_id {self.bot_id}')
                            configuration = r.hgetall(f'{config.BOT_CONFIG_KEY}:{self.bot_id}')
                            if configuration != {}:
                                if float(configuration["last_updated"]) > self.bot_action_config_refresh_ts:
                                    self.logger.info(f'received updated configuration: {configuration}')
                                    bot.config = configuration
                            else:
                                self.logger.warn(f'aborting update as no configuration received')
                        except Exception as e:
                                self.logger.error(f"failed to update configuration - {e}")
                        finally:
                            self.bot_action_config_refresh_ts = current_time
                    if self.action != bot.action:
                        self.logger.info('twap_bot action is updated - {action}')
                        bot.update_action(self.action)

                    if self.action == "PAUSE":
                        self.logger.info('twap_bot is currently paused')
                        time.sleep(1)
                    else:
                        bot.run()
                    bot.push_bot_data_to_redis()
                    time.sleep(0.5)

                bot.client.unregister_dead_man_switch(heartbeat_id)
        except:
            self.logger.error('uncaught exception during twap_bot run cycle')
            self.logger.error(traceback.format_exc())
        finally:
            self.write_status_to_db("Exited")
            self.logger.info(f'twap_bot terminated')

    '''
    Participation Bot
    '''
    def participation_bot(self, parent_input):
        try:
            self.method = 'participation_bot'
            self.logger.info('starting participation bot')
            self._load_data(parent_input)

            with ParticipationBot(self.account_ids[0], self.altcoin, self.quotecoin, self.bot_id, self.configuration, service_id=self.service_id, logger=self.logger) as bot:
                self.bot = bot
                self.logger.info('starting participation bot from Legacy Bot')
                heartbeat_id = f'participation_bot_{self.altcoin}_{self.quotecoin}_{self.service_id}'
                bot.client.register_dead_man_switch(heartbeat_id, 1800)
                self.action = "START"
                r = self.redis
                self.write_status_to_db("Running")
                while not self.is_stopping:
                    gc.collect()
                    bot.client.send_heartbeat(heartbeat_id, 1800)
                    if int(time.time() - self.bot_action_config_refresh_ts) > config.BOT_ACTION_CONFIG_REFRESH_TS:
                        try:
                            current_time = time.time()
                            self.action = r.get(f'{config.BOT_ACTION_COMMAND_KEY}:{self.bot_id}') or "START"
                            self.logger.debug(f'action from redis = {self.action} for bot_id {self.bot_id}')
                            configuration = r.hgetall(f'{config.BOT_CONFIG_KEY}:{self.bot_id}')
                            if configuration != {}:
                                if float(configuration["last_updated"]) > self.bot_action_config_refresh_ts:
                                    self.logger.info(f'received updated configuration: {configuration}')
                                    bot.config = configuration
                            else:
                                self.logger.warn(f'aborting update as no configuration received')
                        except Exception as e:
                                self.logger.error(f"failed to update configuration - {e}")
                        finally:
                            self.bot_action_config_refresh_ts = current_time
                    if self.action != bot.action:
                        self.logger.info('participation_bot action is updated - {action}')
                        bot.update_action(self.action)

                    if self.action == "PAUSE":
                        self.logger.info('participation_bot is currently paused')
                        time.sleep(1)
                    else:
                        bot.run()
                    bot.push_bot_data_to_redis()
                    time.sleep(0.5)

                bot.client.unregister_dead_man_switch(heartbeat_id)
        except:
            self.logger.error('uncaught exception during participation_bot run cycle')
            self.logger.error(traceback.format_exc())
        finally:
            self.write_status_to_db("Exited")
            self.logger.info(f'participation_bot terminated')