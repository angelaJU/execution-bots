#!/bin/bash

set -e

bot_id=$1
account=$2
altcoin=$3
quotecoin=$4
method=$5

if [ -z "$bot_id" ]; then
    echo "bot_id missing"
    exit -1
fi

if [ -z "$account" ]; then
    echo "account missing"
    exit -1
fi

if [ -z "$altcoin" ]; then
    echo "altcoin missing"
    exit -1
fi

if [ -z "$quotecoin" ]; then
    echo "quotecoin missing"
    exit -1
fi

if [ -z "$method" ]; then
    echo "method missing"
    exit -1
fi

echo "Install for aplbot: $bot_id $altcoin $account $quotecoin $method"
pod_name=aplbot-$bot_id
echo "Installing: $pod_name"
helm upgrade $pod_name aplbot --repo https://k8.altono.xyz/chartmuseum \
    --install --cleanup-on-fail --set botId=$bot_id --set altCoin=$altcoin \
    --set quoteCoin=$quotecoin \
    --set method=$method \
    --set account=$account 

