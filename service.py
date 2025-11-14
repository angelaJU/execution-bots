import traceback
from altonomy.services.service import listener, service_factory
from altonomy.core import client as _client
from altonomy.core import logger as _logger
from altonomy.core import config as _config
from . import config
import argparse
import atexit
import zerorpc
from importlib import import_module
from multiprocessing import Event, Process
import sys
import time
import json
from nested_lookup import nested_lookup


class legacy_bot_factory(service_factory):
    def __init__(self, logger=None, service_id=0):
        super().__init__(logger=logger, service_id=service_id)
        self.bot = {}

    def run_legacy_bot(self, module_name, process_name, input_data={}, need_broker=False):
        """
        setup and runs a separate subprocess - for bots

        :param module_name: name of the python module
        :param process_name: name of the method to run
        :param input_data: dict of params
        :param need_broker: whether a broker is needed
        :returns None
        :raises None
        """

        try:
            robot = None
            shared_mode = True if 'legacy_bot' in module_name else False
            if need_broker:
                module_key, market_key = self.parameters.get('module_name', ''), self.parameters.get('market_name', '')
                mode = self.parameters.get('mode')
                if 'account_ids' in self.parameters:
                    broker = []
                    account_ids = self.parameters.get('account_ids')  # this only exists if there are multiple account
                    for i in range(len(account_ids)):
                        broker += [self.initialization(module_key[i], market_key[i], access='', private='', mode=mode[i], shared_mode=shared_mode, account_id=account_ids[i])]
                else:
                    broker = self.initialization(module_key, market_key, access='', private='', mode=mode, shared_mode=shared_mode, account_id=self.parameters.get('account_id'))
                self.logger.debug(f'initializing bot with broker')
                robot = getattr(import_module(f".{module_name}", 'altonomy.apl_bots'), module_name)(broker, logger=self.logger)
                if isinstance(broker, list):
                    for _broker in broker:
                        _broker.shutdown()
                else:
                    broker.shutdown()  # stop websocket before passing
            else:
                self.logger.debug(f'initializing bot without broker')
                robot = getattr(import_module(f".{module_name}", 'altonomy.apl_bots'), module_name)(logger=self.logger)

            robot.account_ids = self.parameters.get('account_ids') or [self.parameters.get('account_id')]

            self.logger.info(f'starting process {process_name} from {robot}')
            getattr(robot, process_name)(input_data)
            self.logger.debug(f'After starting process {process_name} from {robot}')

        except:
            self.logger.error(traceback.format_exc())
        return self.bot

def main():
    """main method

    Args:
        None
    Returns:
        None
    """

    parser = argparse.ArgumentParser(prog="altaplbot", description="starts a legacy bot")
    parser.add_argument("-b", "--bot_id", type=int, help="Unique Bot Id", default=None)
    parser.add_argument("-u", "--url", help="service url", default="tcp://0.0.0.0")
    parser.add_argument("-e", "--eurl", help="external service url", default=f"tcp://{_config.SERVER_IP}")
    parser.add_argument("-y", "--size", type=int, help="pool size", default=1)
    parser.add_argument("-z", "--status", help="service status", default=None)
    parser.add_argument("-m", "--method", help="method to run", default=None)
    parser.add_argument("-a", "--altcoin", help="altcoin", default=None)
    parser.add_argument("-q", "--quotecoin", help="quotecoin", default=None)
    parser.add_argument("account", help="the account name")
    args = parser.parse_args()

    if args.method is None or args.altcoin is None or args.quotecoin is None or args.bot_id is None:
        parser.error('--method, --altcoin, --quotecoin and --bot_id are all required')
        sys.exit(1)

    service_id = _client().get_service_id()
    logger = _logger(__name__, port=service_id)

    r = _client().redis
    configuration = r.hgetall(f'{config.BOT_CONFIG_KEY}:{args.bot_id}')
    logger.info(f'Configuration from redis = {configuration}')

    sf = legacy_bot_factory(logger=logger, service_id=service_id)
    # atexit.register(sf.exit_handler)

    url, eurl = f"{args.url}", f"{args.eurl if args.eurl else args.url}"
    eurl = 'tcp://aplbot.rancher:4000'
    account = args.account.replace(',', '+')
    sf.set_parameters(account, url, eurl, use_dummy_account=True)

    sf.set_service_name(args.account, 'legacy bot', f"{args.method} {args.altcoin}/{args.quotecoin} {args.bot_id}", args.status, args.bot_id)
    
    def object_hook(obj):
        for key, value in obj.items():
            if isinstance(value, str):
                try:
                    obj[key] = json.loads(value.replace("\'", "\""), object_hook=object_hook)
                except ValueError:
                    pass
        return obj
    sf.set_service_accounts(nested_lookup('account_id', json.loads(json.dumps(configuration), object_hook=object_hook)))

    sf.run_legacy_bot('legacy_bot', args.method, {
        'altcoin': args.altcoin,
        'quotecoin': args.quotecoin,
        'exchange_name': sf.parameters.get('market_name', ''),
        'config': configuration,
        'bot_id': args.bot_id,
        'service_id': sf.parameters.get('service_id', 0)}, True)

    sf.exit_handler()
    
    logger.info("Legacy bot is terminated.")


if __name__ == "__main__":
    main()
