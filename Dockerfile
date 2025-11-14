ARG ALT_REPO_PYPI=pypi.altono.app
ARG VERSION="0.2.252"

FROM debian:10 AS deps
# install curl
RUN \
    apt-get update \
    && apt-get install curl -y

# install helm to /usr/local/bin/helm
RUN \
    curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3 \
    && chmod 700 get_helm.sh \
    && ./get_helm.sh \
    && rm get_helm.sh

# download kubectl
RUN curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"


FROM python:3.7.9
# copy helm
COPY --from=deps /usr/local/bin/helm /usr/local/bin/helm
# copy kubectl installer and install
COPY --from=deps kubectl kubectl
RUN install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl && rm kubectl

ARG ALT_REPO_PYPI

RUN apt-get update
RUN apt-get install -y build-essential automake pkg-config libtool libffi-dev \
    libgmp-dev tcpdump net-tools vim postgresql mariadb-client redis-tools \
    tcpdump

WORKDIR /app

ENV PIP_ARGS="--use-deprecated=legacy-resolver --extra-index-url=https://${ALT_REPO_PYPI} --trusted-host=${ALT_REPO_PYPI}"

COPY requirements.txt .
RUN pip install ${PIP_ARGS} -r requirements.txt
RUN pip install pudb

ARG VERSION

COPY . .
RUN pip install dist/altonomy_apl_bots-${VERSION}.tar.gz --use-deprecated=legacy-resolver
COPY ./docker/test.config.ini /root/.altonomy/config.ini
