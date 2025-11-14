# Altonomy-Legacy-Bots

> Legacy Bots

This package contains all the legacy bots from 2018

---

## Table of Contents

- [Installation](#installation)
- [Tech](#tech)
- [Features](#features)
- [FAQ](#faq)
- [Support](#support)
- [License](#license)

---

## Installation

> clone

- Clone this repo to your local machine using `git clone git@bitbucket.org:altonomy/altonomy-apl-bots.git`

> activate virtual environment

```shell
$ source env/bin/activate
```

> now install pypi packages

```shell
$ pip install -r requirements.txt
```
#### Testing

> run linting

```shell
$ flake8 ./altonomy/apl_bots/*
```

> run pytests

```shell
$ pytest -v --cov=altonomy/legacy_bots/*
```
#### Packaging

> generate documentation

```shell
$ cd docs
$ make html
```

> create package

```shell
$ python3 setup.py sdist bdist_wheel
```
#### Running

> run the server

```shell
$ altaplbot
```
---

## Tech

The secrets repository requires the following to work properly:

* [altonomy-client]
* [altonomy-services]
* [zerorpc]
* [pandas]
* [numpy]
* [sqlalchemy]
* [requests]
* [mysql]

---

## Features

- Internal network communications via Zerorpc
- Provides rpc endpoints for controlling the legacy bots

---

## FAQ

- TBC

---

## Support

Reach out to us at one of the following places!

- Website at [altonomy.com](https://www.altonomy.com)
- Email at [support@altonomy.com](mailto:support@altonomy.com)

---

## License

[![License](http://img.shields.io/:license-mit-blue.svg?style=flat-square)](http://badges.mit-license.org)

- **[MIT license](http://opensource.org/licenses/mit-license.php)**
- Copyright 2019 [Altonomy Pte Ltd](https://www.altonomy.com).

**To The Moon!**

   [altonomy-client]: <https://docs.altono.me/altonomy-client/>
   [altonomy-services]: <https://wiki.altono.me/>
   [zerorpc]: <https://www.zerorpc.io/>
   [pandas]: <https://pandas.pydata.org/>
   [numpy]: <https://numpy.org/>
   [sqlalchemy]: <https://www.sqlalchemy.org/>
   [requests]: <https://requests.kennethreitz.org>
   [mysql-connector]: <https://dev.mysql.com/doc/connector-python>