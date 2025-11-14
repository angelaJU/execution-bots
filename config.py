###############################################################################
# Date: 2019.10
# Author: Wee Howe Ang
# Email: weehowe.ang@altonomy.com
# Github: https://bitbucket.org/altonomy/altonomy-legacy-bots
# Description: Legacy Bots Config
###############################################################################

from configparser import ConfigParser
from os.path import expanduser

config = ConfigParser()
home = expanduser("~")

config.read(f"{home}/.altonomy/config.ini")

try:
    BROADCAST_CHANNEL = config['Trading'].get('BROADCAST_CHANNEL', 'order-executor')
except BaseException:
    BROADCAST_CHANNEL = 'order-executor'
try:
    VOLUMEBOT_EMAIL_ALERT_TIME_PERIOD_MIN = float(config['Trading'].get('VOLUMEBOT_EMAIL_ALERT_TIME_PERIOD_MIN', 30))
except BaseException:
    VOLUMEBOT_EMAIL_ALERT_TIME_PERIOD_MIN = 30
try:
    RECORD_TO_CSV = False if config['Trading'].get('RECORD_TO_CSV', 'True') == 'False' else True
except BaseException as e:
    RECORD_TO_CSV = True
try:
    RECORD_TO_DB = False if config['Trading'].get('RECORD_TO_DB', 'True') == 'False' else True
except BaseException as e:
    RECORD_TO_DB = True
try:
    ACCOUNT_TS = float(config['Trading'].get('ACCOUNT_TS', 30))
except BaseException as e:
    ACCOUNT_TS = 30
try:
    RECORDING_TS = float(config['Trading'].get('RECORDING_TS', 60))
except BaseException as e:
    RECORDING_TS = 60
try:
    BOT_ACTION_CONFIG_REFRESH_TS = float(config['Trading'].get('BOT_ACTION_CONFIG_REFRESH_TS', 1))
except BaseException as e:
    BOT_ACTION_CONFIG_REFRESH_TS = 1
try:
    LEGACY_BOT_SLEEP_INTERVAL = float(config['Trading'].get('LEGACY_BOT_SLEEP_INTERVAL', 0.001))
except BaseException as e:
    LEGACY_BOT_SLEEP_INTERVAL = 0.001
try:
    NEW_MM_BOT_CONFIG_RERIS_KEY = config['Trading'].get('NEW_MM_BOT_CONFIG_RERIS_KEY', 'config:Altonobots:New:72')
except BaseException as e:
    NEW_MM_BOT_CONFIG_RERIS_KEY = 'config:Altonobots:New:72'
try:
    BROKER_ORDERBOOK_REFRESH_MAX_TIME = float(config['Trading'].get('BROKER_ORDERBOOK_REFRESH_MAX_TIME', 1))
except BaseException as e:
    BROKER_ORDERBOOK_REFRESH_MAX_TIME = 1
try:
    BOT_ACTION_COMMAND_KEY = config['Trading'].get('BOT_ACTION_COMMAND_KEY', 'action:AltonoAPLBots')
except BaseException as e:
    BOT_ACTION_COMMAND_KEY = 'action:AltonoAPLBots'
try:
    BOT_CONFIG_KEY = config['Trading'].get('BOT_CONFIG_KEY', 'config:AltonoAPLBots')
except BaseException as e:
    BOT_CONFIG_KEY = 'config:AltonoAPLBots'
try:
    BOT_OUTPUT_REDIS_KEY = config['Trading'].get('BOT_OUTPUT_REDIS_KEY', 'output:AltonoAPLBots')
except BaseException as e:
    BOT_OUTPUT_REDIS_KEY = 'output:AltonoAPLBots'
try:
    RELOAD_INSTRUMENT_DATA_FREQUENCY = float(config['Trading'].get('RELOAD_INSTRUMENT_DATA_FREQUENCY', 14400))
except BaseException as e:
    RELOAD_INSTRUMENT_DATA_FREQUENCY = 14400
try:
    UPDATE_REDIS_FREQUENCY = float(config['Trading'].get('UPDATE_REDIS_FREQUENCY', 5))
except BaseException as e:
    UPDATE_REDIS_FREQUENCY = 5
try:
    REFERENCE_ORDERBOOK_REFRESH_MAX_TIME = float(config['Trading'].get('REFERENCE_ORDERBOOK_REFRESH_MAX_TIME', 1))
except BaseException:
    REFERENCE_ORDERBOOK_REFRESH_MAX_TIME = 1
# Database
try:
    DB_USERNAME = config['Database'].get('DB_USERNAME','')
except BaseException as e:
    DB_USERNAME = ''
try:
    DB_PASSWORD = config['Database'].get('DB_PASSWORD','')
except BaseException as e:
    DB_PASSWORD = ''
try:
    DB_HOSTNAME = config['Database'].get('DB_HOSTNAME','')
except BaseException as e:
    DB_HOSTNAME = ''
try:
    DB_PORTNAME = config['Database'].get('DB_PORTNAME','')
except BaseException as e:
    DB_PORTNAME = ''
try:
    DB_INSTNAME = config['Database'].get('DB_INSTNAME','')
except BaseException as e:
    DB_INSTNAME = ''
try:
    SQLALCHEMY_POOL_RECYCLE = float(config['Database'].get('SQLALCHEMY_POOL_RECYCLE','3600'))
except BaseException as e:
    SQLALCHEMY_POOL_RECYCLE = 3600
try:
    HELM_REPO = config['Deployment'].get('HELM_REPO','')
except BaseException as e:
    HELM_REPO = ''
try:
    HELM_BOOK_LIS_RESOURCES = dict(x.split('=') for x in config['Deployment'].get('HELM_BOOK_LIS_RESOURCES','').split(','))
except BaseException as e:
    HELM_BOOK_LIS_RESOURCES = {
        "resources.requests.memory": "260Mi", 
        "resources.requests.cpu": "180m",
        "resources.limits.memory": "500Mi", 
        "resources.limits.cpu": "300m"
    }

