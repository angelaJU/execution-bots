import re
from  typing import Dict
from subprocess import PIPE, CalledProcessError, check_output
from altonomy.core import client
import logging
from . import config

class HelmClient(object):
    """
    Implements an interface with the helm CLI.
    """
    def __init__(self, endpoint: str, logger=None):
        if not endpoint:
            raise RuntimeError("Helm Endpoint needs to be provided!")
        self.endpoint = endpoint
        self.logger = logger or logging.getLogger()
        self.client = client()

    @staticmethod
    def _exec(command):
        try:
            outp = check_output(command, shell=True, stderr=PIPE)
            return outp.decode("utf-8")
        except CalledProcessError as e:
            raise e
        
    @staticmethod
    def _format_cmd_value(value):
        if isinstance(value, list):
            return f"{{{ ','.join(str(v) for v in value) }}}"
        else:
            return value


    def format_str(self, s: str) -> str:
        return re.sub(r'[^0-9a-zA-Z]', "-", s.lower())

    def upgrade(
        self,
        name: str,
        package: str,
        values: dict,
        namespace: str = "default",
        install_if_not_exist: bool = True,
    ):
        install_option = "--install" if install_if_not_exist else ""
        if len(values) != 0:
            values_cmd = ",".join(f"{k}={self._format_cmd_value(v)}" for k, v in values.items())
            values_cmd = f"--set '{values_cmd}'"
        else:
            values_cmd = ""
        command = f"""helm upgrade {name} {package} {values_cmd} {install_option} --namespace {namespace} --repo {self.endpoint}"""
        self.logger.info(f"HelmClient.upgrade | command | {command}")
        return self._exec(command)

    def start_orderbook_listener(self, account, exchange, base, quote):
        """
        Starts an orderbook listener
        """

        exchange_name = self.client.exchange_name(exchange)
        values = {
            "mode": "publish_ws_book_to_redis_v2",
            'account' : account, 
            'base': base, 
            'quote': quote
        }
        self.logger.info(f'Helm book listener resources - {config.HELM_BOOK_LIS_RESOURCES}')
        values.update(config.HELM_BOOK_LIS_RESOURCES)
        helm_name = self.format_str(f"listener-book-{exchange_name}-{base}{quote}")
        package = "data-listener"
        namespace = "listener-book"
        self.upgrade(helm_name, package, values, namespace=namespace)
        return True