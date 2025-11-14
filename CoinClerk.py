#########################################################################################
# Date: 2018.05
# Auther: happyshiller 
# Email: happyshiller@gmail.com
# Github: https://github.com/happyshiller/Altonomy
# Function: to keep records and accounts and to undertake routine duties
#########################################################################################

from datetime import datetime, timezone
from sqlalchemy import exc, desc
from sqlalchemy.engine.url import URL
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import Column, Index, Table, ForeignKey, MetaData
from sqlalchemy.types import Integer, Float, String, DateTime
from sqlalchemy.sql import func
from sqlalchemy.orm.session import sessionmaker 
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base
import mysql.connector
from mysql.connector import connection
from mysql.connector import errorcode
import base64
import warnings
from altonomy.core import logger as _logger
from . import config
from altonomy.core import config as _config
# from altonomy.models import LegacyExecutionLog as Execution_log
from altonomy.models import LegacyBotLog as Bot_log

# crytocode = base64.b64encode("password")
# dbpassword = base64.b64decode(dbpassword)

DB_TABLES = {}
# Execution_log will combine with Bot_log by BotID
# in 'Manual' mode, BotID set to 0
# CONSTRAINT PK_ID PRIMARY KEY (OrderID, Exchange)
# There is only ONE PRIMARY KEY. The VALUE of the primary key is made up of two COLUMNS (OrderID + Exchange)
# ID INT NOT NULL AUTO_INCREMENT, 
# if use auto_increment surrogate id, there will be a risk that same order duplicated records
# NetFilledAmount = FilledAmount - BuyCommission
# NetFilledCashAmount = FilledCashAmount - SellCommission
DB_TABLES['Execution_log'] = (
    """
    CREATE TABLE IF NOT EXISTS Execution_log (
        OrderID VARCHAR(255) NOT NULL,
        TimeStamp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
        TradingPair VARCHAR(255) NOT NULL,
        Exchange VARCHAR(255) NOT NULL,
        AccountKey VARCHAR(255) NOT NULL,
        LongShort VARCHAR(255) NOT NULL,
        Amount DOUBLE NOT NULL,
        FilledAmount DOUBLE NOT NULL,
        Price DOUBLE NOT NULL,
        FilledCashAmount DOUBLE NOT NULL,
        BuyCommission DOUBLE NOT NULL,
        SellCommission DOUBLE NOT NULL,
        NetFilledAmount DOUBLE NOT NULL,
        NetFilledCashAmount DOUBLE NOT NULL,
        Status VARCHAR(255) NOT NULL,
        Source VARCHAR(255) DEFAULT 'Automatic' NOT NULL,
        BotID INT NOT NULL,
        PRIMARY KEY (OrderID, Exchange),
        INDEX(TimeStamp)
    )ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;
    """
)
# Some Bot may have more than one broker or pair
DB_TABLES['Bot_log'] = (
    """
    CREATE TABLE IF NOT EXISTS Bot_log (
        ID INT NOT NULL AUTO_INCREMENT,
        IpAddr VARCHAR(21) NOT NULL,
        BotName VARCHAR(255) NOT NULL,
        Parameters VARCHAR(4000),
        StartTime TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6),
        EndTime TIMESTAMP(6),
        Status VARCHAR(255),
        PRIMARY KEY (ID)
    )ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;
    """
)


SQL_QUERY = {}
# windows doesn't support time zone, TODO add time zone file to server
SQL_QUERY['Update_Timezone'] = {
    """

    """
}
# cursor.execute("INSERT INTO table VALUES (%s, %s, %s)", (var1, var2, var3))
SQL_QUERY['Insert_Execution_Order'] = (
    """
    INSERT IGNORE INTO Execution_log 
    (OrderID, TimeStamp, TradingPair, Exchange, AccountKey, LongShort, Amount, FilledAmount, Price, 
    FilledCashAmount, BuyCommission, SellCommission, NetFilledAmount, NetFilledCashAmount, Status, Source, BotID)
    VALUES (%s, FROM_UNIXTIME(%s), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
)
# set safe mode to false
# SET SQL_SAFE_UPDATES = 0
SQL_QUERY['Update_Execution_Order'] = (
    """
    UPDATE Execution_log
    SET TimeStamp=FROM_UNIXTIME(%s), FilledAmount=%s, FilledCashAmount=%s, BuyCommission=%s, SellCommission=%s, 
    NetFilledAmount=%s, NetFilledCashAmount=%s, Status=%s
    WHERE OrderID=%s AND Exchange=%s
    """
)
# SET SQL_SAFE_UPDATES = 0
# delete order with 0 filled amount after trying in max time 
SQL_QUERY['Delete_Execution_Order'] = (
    """
    DELETE FROM Execution_log WHERE OrderID=%s AND Exchange=%s
    """
)
# delete all orders with 0 filled amount if there is any, used when algo exit
SQL_QUERY['Delete_Execution_Order_CLR'] = (
    """
    DELETE FROM Execution_log WHERE FilledAmount=0.0
    """
)
# return lastest records with given limit number
# SET time_zone=%s; 
SQL_QUERY['Select_Execution_Orders_Desc'] = (
    """
    SELECT OrderID, DATE_FORMAT(TimeStamp, '%Y-%m-%d %T.%f') AS TimeStamp, TradingPair, Exchange, AccountKey, LongShort, 
    Amount, FilledAmount, Price, FilledCashAmount, BuyCommission, SellCommission, NetFilledAmount, NetFilledCashAmount, 
    Status, Source, BotID
    FROM Execution_log WHERE TradingPair=%s AND Exchange=%s AND AccountKey=%s AND Source=%s
    ORDER BY TimeStamp DESC LIMIT %s
    """
)
# return records between a given time period (starting, now())
# SET time_zone = 'Europe/Helsinki';
# TODO count in timezone
SQL_QUERY['Select_Execution_Orders_Histo'] = (
    """
    SELECT OrderID, DATE_FORMAT(TimeStamp, '%Y-%m-%d %T.%f') AS TimeStamp, TradingPair, Exchange, AccountKey, LongShort, 
    Amount, FilledAmount, Price, FilledCashAmount, BuyCommission, SellCommission, NetFilledAmount, NetFilledCashAmount,
    Status, Source, BotID
    FROM Execution_log WHERE TradingPair=%s AND Exchange=%s AND AccountKey=%s AND Source=%s 
    AND TimeStamp BETWEEN %s AND %s
    ORDER BY TimeStamp DESC
    """
)
# generate a position report sum(FilledAmount) average(Price) between a given time period group 
# by TradingPair, Exchange, AccountKey, LongShort, Source
# weighted average price = sum(FilledAmount*Price)/sum(FilledAmount)
# Source=%s
SQL_QUERY['Create_Execution_Report'] = (
    """
    SELECT TradingPair, Exchange, AccountKey, LongShort, Source, COUNT(*) AS TotalOrders, 
    SUM(FilledAmount) AS TotalFilledAmount, SUM(FilledCashAmount) AS TotalFilledCashAmount, 
    SUM(BuyCommission) AS TotalBuyCommission, SUM(SellCommission) AS TotalSellCommission, 
    SUM(NetFilledAmount) AS TotalNetFilledAmount, SUM(NetFilledCashAmount) AS TotalNetFilledCashAmount,
    (SUM(NetFilledCashAmount)/SUM(NetFilledAmount)) AS WeightedAvgPrice
    FROM Execution_log WHERE TradingPair=%s AND Exchange=%s AND AccountKey=%s AND Source=%s
    AND TimeStamp BETWEEN %s AND %s
    GROUP BY TradingPair, Exchange, AccountKey, LongShort, Source
    """
)


# SQLAlchemy ORM table definition Classes
BaseModel = declarative_base()
# Bot log records Bot entry and exit
# class Bot_log(BaseModel):
#     """ """
#     __tablename__ = 'Bot_log'
#     __table_args__ = {
#         'mysql_engine': "InnoDB",
#         'mysql_charset': "utf8",
#     }
#     # ProcessID = str(time.time())
#     ID = Column('ID', Integer(), primary_key=True, autoincrement=True)
#     ProcessID = Column('ProcessID', String(255), nullable=False)
#     PidName = Column('PidName', String(255), index=True, nullable=False)
#     IpAddr = Column('IpAddr', String(255))
#     Parameters = Column('Parameters', String(4000))
#     StartTime = Column('StartTime', DateTime(timezone=True), server_default=func.now(), nullable=False)
#     EndTime = Column('EndTime', DateTime(timezone=True))
#     Status = Column('Status', String(255), nullable=False)
#     Orders = relationship('Execution_log')

# Execution log records order details
class Execution_log(BaseModel):
    """ """
    __tablename__ = 'Execution_log'
    __table_args__ = {
        'mysql_engine': "InnoDB",
        'mysql_charset': "utf8",
    }
    ID = Column('ID', Integer(), primary_key=True, autoincrement=True)
    OrderID = Column('OrderID', String(255), index=True, nullable=False)
    # NO default=datetime.datetime.utcnow, use database server time, not the application time
    TimeStamp = Column('TimeStamp', DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), index=True, nullable=False)
    TradingPair = Column('TradingPair', String(255, collation="nocase"), nullable=False)
    Exchange = Column('Exchange', String(255, collation="nocase"), nullable=False)
    AccountKey = Column('AccountKey', String(255, collation="nocase"), nullable=False)
    LongShort = Column('LongShort', String(255, collation="nocase"), nullable=False)
    Amount = Column('Amount', Float(53), nullable=False)
    FilledAmount = Column('FilledAmount', Float(53), nullable=False)
    Price = Column('Price', Float(53), nullable=False)
    FilledCashAmount = Column('FilledCashAmount', Float(53), nullable=False)
    BuyCommission = Column('BuyCommission', Float(53), nullable=False)
    SellCommission = Column('SellCommission', Float(53), nullable=False)
    NetFilledAmount = Column('NetFilledAmount', Float(53), nullable=False)
    NetFilledCashAmount = Column('NetFilledCashAmount', Float(53), nullable=False)
    Status = Column('Status', String(255, collation="nocase"), nullable=False)
    Source = Column('Source', String(255, collation="nocase"), default='Automatic', nullable=False)
    BotID = Column('BotID', Integer(), default=0, nullable=False)

# Accounts table 
class Account(BaseModel):
    """ """
    __tablename__ = 'Account'
    __table_args__ = {
        'mysql_engine': "InnoDB",
        'mysql_charset': "utf8",
    }
    ID = Column('ID', Integer(), primary_key=True, autoincrement=True)
    Name = Column('Name', String(255), unique=True, nullable=False)
    AccountKey = Column('AccountKey', String(255), unique=True, nullable=False)
    Access = Column('Access', String(255), unique=True, nullable=False)
    Private = Column('Private', String(255), unique=True, nullable=False)
    # ExchangeID = Column('ExchangeID', Integer(), ForeignKey('Exchange.ID'), nullable=True)

    # def __repr__(self):
    #     return '<Account %r>' % self.name

# Risk handbook
class Risk_Metrics(BaseModel):
    """ """
    __tablename__ = 'Risk_Metrics'
    __table_args__ = {
        'mysql_engine': "InnoDB",
        'mysql_charset': "utf8",
    }
    ID = Column('ID', Integer(), primary_key=True, autoincrement=True)
    RiskMetric = Column('RiskMetric', String(255), unique=True, nullable=False)
    Definition = Column('Definition', String(255), nullable=False)
    AuditTrail = Column('AuditTrail', String(255), nullable=False)

# Risk limits
class Risk_Limits(BaseModel):
    """ """
    __tablename__ = 'Risk_Limits'
    __table_args__ = {
        'mysql_engine': "InnoDB",
        'mysql_charset': "utf8",
    }
    ID = Column('ID', Integer(), primary_key=True, autoincrement=True)
    RiskMetric = Column('RiskMetric', String(255), nullable=False)
    Trader = Column('Trader', String(255), nullable=True)
    Exchange = Column('Exchange', String(255), nullable=True)
    Account = Column('Account', String(255), nullable=True)
    TradingPair = Column('TradingPair', String(255), nullable=True)
    MinLimit = Column('MinLimit', Float(53), nullable=False)
    MaxLimit = Column('MaxLimit', Float(53), nullable=False)
    Unit = Column('Unit', String(255), nullable=False)
    AuditTrail = Column('AuditTrail', String(255), nullable=False)

# Risk consumption
class Risk_Consumption(BaseModel):
    """ """
    __tablename__ = 'Risk_Consumption'
    __table_args__ = {
        'mysql_engine': "InnoDB",
        'mysql_charset': "utf8",
    }
    ID = Column('ID', Integer(), primary_key=True, autoincrement=True)
    RiskMetric = Column('RiskMetric', String(255), nullable=False)
    Trader = Column('Trader', String(255), nullable=True)
    Exchange = Column('Exchange', String(255), nullable=True)
    Account = Column('Account', String(255), nullable=True)
    TradingPair = Column('TradingPair', String(255), nullable=True)
    Value = Column('Value', Float(53), nullable=False)
    Unit = Column('Unit', String(255), unique=True, nullable=False)
    MinLimit = Column('MinLimit', Float(53), nullable=False)
    MaxLimit = Column('MaxLimit', Float(53), nullable=False)
    Percentage = Column('%', String(255), nullable=False)
    TimeStamp = Column('TimeStamp', DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    

default_dbconfig = {
    'user': 'root',
    'password': 'root',
    'host': 'localhost',
    'port': '3306',
    'charset': 'utf8mb4',
    'autocommit': False,
    'database': 'Altonomy',
    'raise_on_warnings': True,
}


class CoinClerk():
    """
    CoinClerk is a generic class for operation functionality. Clerks keep records and accounts.
    There are two sets of database implementation: conventional SQL and SQLAlchemy library.
    """
        
    def __init__(self, dbconfig=None, logger=None):
        """initialize the clerk instance with Database config"""
        # MySQL connector version
        # dbconfig = {
        # 'user': 'root',
        # 'password': 'root',
        # 'host': 'localhost', #(or '127.0.0.1')
        # 'port': '3306',
        # 'charset': 'utf8mb4',
        # 'autocommit': False,
        # 'database': 'Altonomy',
        # 'raise_on_warnings': True,
        # 'options': {set time_zone='+00:00'},
        # }
        # SQLAlchemy version
        dbconfig = {
		'drivername': 'mysql+mysqlconnector',
		'username': _config.DB_USERNAME,
		'password': _config.DB_PASSWORD,
		'host': _config.DB_HOSTNAME,
		'port': _config.DB_PORTNAME,
		'database': _config.DB_INSTNAME
		}
        if not dbconfig:
            self.dbconfig = default_dbconfig
        else:
            self.dbconfig = dbconfig
        # an Engine is a connection pool - SQLAlchemy
        # Engine is the lowest level object used by SQLAlchemy. 
        # It maintains a pool of connections available for use 
        # whenever the application needs to talk to the database. 
        self.dbliteengine = None
        self.dbengine = None
        # database connection / SQLAlchemy
        # use connections executing raw SQL code and control
        self.dbliteconnection = None
        self.dbconnection = None
        # database session
        self.dblitecursor = None
        self.dbcursor = None
        # database session - SQLAlchemy
        # use session using the ORM functionality
        self.dblitesession = None
        self.dbsession = None
        # good to go signal
        self.passanitycheck = True
        # logger
        self.logger = logger if logger else _logger(__name__)
        # connect to database server through SQLAlchemy
        self.connect_to_DBserver(self.dbconfig)
        self.connect_to_DBLiteserver(self.dbconfig)
        # create db tables with SQLAlchemy if do not exists
        # if config.CREATE_TABLES:
        #     self.create_db_tables_sqla()
        # if config.CREATE_TABLES:
        self.create_db_tables_sqlitea()
        # connect to booking system (MySQL server DB - Altonomy)
        # self.connect_to_MySQLserver(self.dbconfig)
        # create db tables if do not exists
        # self.create_db_tables()
        # remove unfilled orders at starting
        # self.clear_unfilled_execution_orders()
        self.logger.debug('CoinClerk finished init')
    

    def create_database(self, dbconfig):
        """
        :dbconfig: **dbconfig {user:, password:, host:, port:, database:,}
        :create a new database if not exist
        """
        # dbconfig = {'a': 1, 'b': 2, 'c': 3}
        # f(**dbconfig) -> f(a=1, b=2, c=3)
        if isinstance(dbconfig, dict) and len(dbconfig) > 0:
            # make sure that database name is included in database config
            if 'database' in dbconfig:
                dbname = dbconfig['database']
                del dbconfig['database']
            else:
                self.logger.error("The database name is missing, Failed creating database!")
                return False
            # create db with dbname from config if not exist
            try:
                self.dbconnection = mysql.connector.connect(**dbconfig)
                self.dbcursor = self.dbconnection.cursor()
                self.dbcursor.execute("CREATE DATABASE IF NOT EXISTS %s DEFAULT CHARACTER SET 'utf8mb4'" % dbname )
            except mysql.connector.Error as err:
                self.logger.error("Failed creating database: %s!" % err)
                return False
            else:
                self.logger.info("Database %s is successfully created!" % dbname )
                self.dbconnection.database = dbname
                # dbcursor.close()
                # dbconnection.close()
        else:
            self.logger.error("The database config is not meaningful dictionary, Failed creating database!")
            return False
    

    def connect_to_MySQLserver(self, dbconfig):
        """
        :dbconfig: **dbconfig {user:, password:, host:, port:, database:,}
        :connect to MySQL server, create database 'Altonomy' if it doesn't exist
        """
        if isinstance(dbconfig, dict) and len(dbconfig) > 0:
            # dbconfig = {'a': 1, 'b': 2, 'c': 3}
            # f(**dbconfig) -> f(a=1, b=2, c=3)
            try:
                self.dbconnection = mysql.connector.connect(**dbconfig)
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                    self.logger.error("Incorrect user name or password!")
                    return False
                elif err.errno == errorcode.ER_DBACCESS_DENIED_ERROR:
                    self.logger.error("Current user doesn't have access right!")
                    return False
                elif err.errno == errorcode.ER_BAD_DB_ERROR:
                    self.logger.error("Database does not exist! Creating...")
                    self.create_database(dbconfig)
                else:
                    self.logger.error("Something went wrong: %s!" % err)
                    return False
            else:
                self.dbcursor = self.dbconnection.cursor()
                self.logger.info("Connecting to Database %s succeed." % dbconfig['database'])
                # dbcursor.close()
                # dbconnection.close()
        else:
            self.logger.error("The database config is not meaningful dictionary, connecting to MySQL server failed!")
            return False


    def create_db_tables(self, tabledict=DB_TABLES):
        """
        :tablesdict: a dictionary saved definition of db tables with table name as key
        :create series of db tables if not exist
        """
        if isinstance(tabledict, dict) and len(tabledict) > 0:
            # iterate table name, create db table based on definition if doesn't exist
            for tablename, tableddl in tabledict.items():
                try:
                    self.dbcursor.execute(tableddl)
                except mysql.connector.Error as err:
                    if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                        self.logger.info("DB table %s already exists." % tablename)
                    else:
                        self.logger.error("Failed creating DB table %s due to: %s!" % (tablename, err))
                        return False
                else:
                    self.logger.info("DB table %s is successfully created!" % tablename)
        else:
            self.logger.error("The DB table definition dictionary is empty! Failed creating any table!")
            return False



    def drop_table(self, tablenamelist):
        """
        :tablesdict: a dictionary saved definition of db tables with table name as key
        :drop series of db tables if exist
        """
        # iterate table name, create db table based on definition if doesn't exist
        if isinstance(tablenamelist, list) and len(tablenamelist) > 0:
            for tablename in tablenamelist:
                try:
                    self.dbcursor.execute("DROP TABLE IF EXISTS %s" % tablename)
                except mysql.connector.Error as err:
                    if err.errno == errorcode.ER_BAD_TABLE_ERROR:
                        self.logger.error("DB Table %s does not exist." % tablename)
                        return False
                    else:
                        self.logger.error("Failed dropping DB table %s due to: %s!" % (tablename, err))
                        return False
                else:
                    self.logger.info("DB table %s is successfully dropped!" % tablename)
        else:
            self.logger.error("DB table list is empty! Failed dropping any table!")
            return False
    
    
    def drop_database(self, dbname):
        """
        :dbname: name of the database asking to drop        
        :drop the given database
        """
        if dbname: 
            try:
                self.dbcursor.execute("DROP DATABASE IF EXISTS %s" % dbname )
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_DB_DROP_EXISTS:
                    self.logger.error("Database %s does not exist." % dbname)
                    return False
                else:
                    self.logger.error("Failed dropping database %s due to: %s!" % (dbname, err))
                    return False
            else:
                self.logger.info("Database %s is successfully dropped!" % dbname)
                return True
        else:
            self.logger.error("The database name is an empty string. Failed dropping any database!")
            return False
    

    def easy_query(self, sqlquery, params):
        """quickly process a SQL query"""
        try:
            self.dbcursor.execute(sqlquery, params)
        except mysql.connector.Error as err:
            self.logger.error("Failed processing SQL query due to: %s!" % err)
            return False
        else:
            columname = [col[0] for col in self.dbcursor.description]
            return columname, self.dbcursor.fetchall()


    def update_database_config(self, dbconfig):
        """
        :update the database connection arguments
        :dbconfig: {'username': 'root', 'password': '123456', ...} 
        """
        # dbconfig is a dictionary
        if isinstance(dbconfig, dict) and len(dbconfig) > 0:
            self.dbconfig = dbconfig
        else:
            self.passanitycheck = False
            self.logger.error("The database config is not meaningful dictionary, updating failed!")
            return False
    
    

    def insert_execution_order(self, orderdetails):
        """
        :orderdetails: OrderID, TimeStamp, TradingPair, Exchange, AccountKey, LongShort, Amount, FilledAmount, Price, Status, Source
        :insert new order record to db table Execution_log of db Altonomy
        """
        if isinstance(orderdetails, dict) and len(orderdetails) > 0:
            params = (orderdetails['OrderID'], float(orderdetails['TimeStamp']/1000), orderdetails['TradingPair'], 
            orderdetails['Exchange'], orderdetails['AccountKey'], orderdetails['LongShort'], float(orderdetails['Amount']), 
            float(orderdetails['FilledAmount']), float(orderdetails['Price']), float(orderdetails['FilledCashAmount']), 
            float(orderdetails['BuyCommission']), float(orderdetails['SellCommission']), float(orderdetails['NetFilledAmount']), 
            float(orderdetails['NetFilledCashAmount']), orderdetails['Status'], orderdetails['Source'], orderdetails['BotID'])

            try:
                # self.dbcursor.execute(SQL_QUERY['Insert_Execution_Order'], tuple(orderdetails.values()))
                self.dbcursor.execute(SQL_QUERY['Insert_Execution_Order'], params)
            except mysql.connector.Error as err:
                self.dbconnection.rollback()
                self.logger.error("Failed inserting order record due to: %s!" % err)
                return False
            else:
                self.dbconnection.commit()
                return self.dbcursor.rowcount
        else:
            self.logger.error("The orderdetails dict is empty. Failed inserting order!")
            return False


    def update_execution_order(self, orderdetails):
        """
        :orderdetails: OrderID, TimeStamp, TradingPair, Exchange, AccountKey, LongShort, Amount, FilledAmount, Price, Status, Source
        :update a given order record in db table Execution_log of db Altonomy
        """
        if isinstance(orderdetails, dict) and len(orderdetails) > 0:
            params = (float(orderdetails['TimeStamp']/1000), float(orderdetails['FilledAmount']), float(orderdetails['FilledCashAmount']), 
            float(orderdetails['BuyCommission']), float(orderdetails['SellCommission']), float(orderdetails['NetFilledAmount']), 
            float(orderdetails['NetFilledCashAmount']), orderdetails['Status'], orderdetails['OrderID'], orderdetails['Exchange'])

            try:
                self.dbcursor.execute(SQL_QUERY['Update_Execution_Order'], params)
            except mysql.connector.Error as err:
                self.dbconnection.rollback()
                self.logger.error("Failed updating order record due to: %s!" % err)
                return False
            else:
                self.dbconnection.commit()
                return self.dbcursor.rowcount
        else:
            self.logger.error("The orderdetails dict is empty. Failed updating order!")
            return False
        

    def delete_execution_order(self, orderdetails):
        """
        :orderdetails: OrderID, TimeStamp, TradingPair, Exchange, AccountKey, LongShort, Amount, FilledAmount, Price, Status, Source
        :delete a given order record in db table Execution_log of db Altonomy
        """
        if isinstance(orderdetails, dict) and len(orderdetails) > 0:
            params = (orderdetails['OrderID'], orderdetails['Exchange'])

            try:
                self.dbcursor.execute(SQL_QUERY['Delete_Execution_Order'], params)
            except mysql.connector.Error as err:
                self.dbconnection.rollback()
                self.logger.error("Failed deleting order record due to: %s!" % err)
                return False
            else:
                self.dbconnection.commit()
                self.logger.info("Order %s from Exchange %s was deleted from table Execution_log!" % (orderdetails['OrderID'], orderdetails['Exchange']))
                return self.dbcursor.rowcount
        else:
            self.logger.error("The orderdetails dict is empty. Failed deleting order!")
            return False


    def clear_unfilled_execution_orders(self):
        """delete unfilled orders in db table Execution_log of db Altonomy, used at starting/exiting"""
        try:
            self.dbcursor.execute(SQL_QUERY['Delete_Execution_Order_CLR'])
            deletedrows = self.dbcursor.rowcount
        except mysql.connector.Error as err:
            self.dbconnection.rollback()
            self.logger.error("Failed deleting unfilled orders due to: %s!" % err)
            return False
        else:
            self.dbconnection.commit()
            self.logger.info("There are totally %s unfilled orders being deleted from table Execution_log!" % deletedrows)
            return deletedrows



    def sanity_check(self):
        """perform sanity check to make sure Bot are configured correctly"""
        return self.passanitycheck
    
    

    def get_latest_execution_orders(self, tradingpair, exchange, account, source, num=20, timezone='Singapore'):
        """
        :tradingpair: such as WPRETH
        :exchange: such as Binance
        :account: accountkey, in case there are multiple accounts in same exchange
        :source: str 'Automatic' or 'Manual'
        :num: the most recent 20 records, regardless of manual or auto
        :timezone: doesn't work under windows
        :retrieve n order records from db table Execution_log
        """
        inum = int(num)
        # empty string evaluate to False in Python
        if (source == 'Automatic' or source == 'Manual') and timezone and tradingpair and exchange and account:
            # inputpara = ', '.join(list(map(lambda x: '%s', source)))
            params = (tradingpair, exchange, account, source, inum)
            # params = (timezone, source, inum)
        else:
            self.logger.error("Incorrect input. Failed retriveing order records!")
            return False
        
        try:
            # self.dbcursor.execute(SQL_QUERY['Select_Execution_Orders_Desc'] % inputpara, params)
            self.dbcursor.execute(SQL_QUERY['Select_Execution_Orders_Desc'], params)
        except mysql.connector.Error as err:
            self.logger.error("Failed retrieving order records due to: %s!" % err)
            return False
        else:
            columname = [col[0] for col in self.dbcursor.description]
            return columname, self.dbcursor.fetchall()
    

    def get_historical_execution_orders(self, tradingpair, exchange, account, source, begintime, endtime, timezone='Singapore'):
        """
        :tradingpair: such as WPRETH
        :exchange: such as Binance
        :account: accountkey, in case there are multiple accounts in same exchange
        :source: str 'Automatic' or 'Manual'
        :begintime: such as '2018-05-25 22:00:01'
        :endtime: such as '2018-05-25 22:30:00'
        :timezone: doesn't work under windows
        :retrieve order records from db table Execution_log from certain time period
        """
        # empty string evaluate to False in Python
        if (source == 'Automatic' or source == 'Manual') and timezone and tradingpair and exchange and account \
        and begintime and endtime:
        # if isinstance(source, list) and len(source)>0 and timezone and tradingpair and exchange and account and \
        # begintime and endtime:
            # strsource = "', '".join(source)
            # formatsource = ', '.join(list(map(lambda x: '%s', source)))
            # formatsource = ', '.join(map(str, source))
            params = (tradingpair, exchange, account, source, begintime, endtime)
        else:
            self.logger.error("Incorrect input. Failed retriveing order records!")
            return False

        try:
            self.dbcursor.execute(SQL_QUERY['Select_Execution_Orders_Histo'], params)
            # self.dbcursor.execute(SQL_QUERY['Select_Execution_Orders_Histo'] % ('%s', '%s', '%s', tuple(formatsource), '%s', '%s'), params)
        except mysql.connector.Error as err:
            self.logger.error("Failed retrieving order records due to: %s!" % err)
            return False
        else:
            columname = [col[0] for col in self.dbcursor.description]
            return columname, self.dbcursor.fetchall()
    

    def get_execution_report(self, tradingpair, exchange, account, source, begintime, endtime, timezone='Singapore'):
        """
        :tradingpair: such as WPRETH
        :exchange: such as Binance
        :account: accountkey, in case there are multiple accounts in same exchange
        :source: str 'Automatic' or 'Manual'
        :begintime: such as '2018-05-25 22:00:01'
        :endtime: such as '2018-05-25 22:30:00'
        :timezone: doesn't work under windows
        :report: tradingpair, exchange, long, short, net, WAPX_long, WAPX_short, totalcost, totalrevenue  
        """
        # empty string evaluate to False in Python
        if (source == 'Automatic' or source == 'Manual') and timezone and tradingpair and exchange and account \
        and begintime and endtime:
            params = (tradingpair, exchange, account, source, begintime, endtime)
        else:
            self.logger.error("Incorrect input. Failed generating execution report!")
            return False

        try:
            self.dbcursor.execute(SQL_QUERY['Create_Execution_Report'], params)
            # self.dbcursor.execute(SQL_QUERY['Select_Execution_Orders_Histo'] % ('%s', '%s', '%s', tuple(formatsource), '%s', '%s'), params)
        except mysql.connector.Error as err:
            self.logger.error("Failed generating execution report due to: %s!" % err)
            return False
        else:
            columname = [col[0] for col in self.dbcursor.description]
            return columname, self.dbcursor.fetchall()



    ##########################################
    #           SQLAlchemy Version           #
    ##########################################
    def create_database_sqla(self, dbconfig):
        """
        :dbconfig:
        :create a new database if not exist
        """
        # dbconfig = {'a': 1, 'b': 2, 'c': 3}
        # f(**dbconfig) -> f(a=1, b=2, c=3)
        if isinstance(dbconfig, dict) and len(dbconfig) > 0:
            # make sure that database name is included in database config
            if 'database' in dbconfig:
                dbname = dbconfig['database']
                del dbconfig['database']
                connecturl = URL(**dbconfig)
            else:
                self.logger.error("The database name is missing, Failed creating database!")
                return False
            # create db with dbname from config if not exist
            try:
                self.dbengine = create_engine(connecturl, pool_recycle=_config.SQLALCHEMY_POOL_RECYCLE, pool_pre_ping=True, pool_size=_config.SQLALCHEMY_POOL_SIZE, max_overflow=_config.SQLALCHEMY_POOL_MAX_OVERFLOW, isolation_level=_config.ISOLATION_LEVEL)
                self.dbengine.execute("CREATE DATABASE IF NOT EXISTS {0} DEFAULT CHARACTER SET 'utf8mb4'".format(dbname))
            except exc.SQLAlchemyError as e:
                self.logger.error("Failed creating database due to: {0}".format(e))
                return False
            else:
                self.logger.info("Database {0} is successfully created!".format(dbname))
                return True
        else:
            self.logger.error("The connection config is not meaningful, failed creating database!")
            return False


    def create_database_sqlitea(self, dbconfig):
        """
        :dbconfig:
        :create a new database if not exist
        """
        # dbconfig = {'a': 1, 'b': 2, 'c': 3}
        # f(**dbconfig) -> f(a=1, b=2, c=3)
        if isinstance(dbconfig, dict) and len(dbconfig) > 0:
            # make sure that database name is included in database config
            if 'database' in dbconfig:
                dbname = dbconfig['database']
                del dbconfig['database']
                connecturl = URL(**dbconfig)
            else:
                self.logger.error("The database name is missing, Failed creating database!")
                return False
            # create db with dbname from config if not exist
            try:
                self.dbliteengine = create_engine("sqlite://")
                self.dbliteengine.execute("CREATE DATABASE IF NOT EXISTS {0} DEFAULT CHARACTER SET 'utf8mb4'".format(dbname))
            except exc.SQLAlchemyError as e:
                self.logger.error("Failed creating database due to: {0}".format(e))
                return False
            else:
                self.logger.info("Database {0} is successfully created!".format(dbname))
                return True
        else:
            self.logger.error("The connection config is not meaningful, failed creating database!")
            return False


    def connect_to_DBserver(self, dbconfig):
        """
        :dbconfig: 
        :drivername – the name of the database backend. This name will correspond to a module in sqlalchemy/databases or a third party plug-in.
        :username – The user name.
        :password – database password.
        :host – The name of the host.
        :port – The port number.
        :database – The database name.
        :query – A dictionary of options to be passed to the dialect and/or the DBAPI upon connect.
        """
        # '数据库类型+数据库驱动名称://用户名:口令@机器地址:端口号/数据库名'
        # MySQL-Connector version:
        # mysql+mysqlconnector://<user>:<password>@<host>[:<port>]/<dbname>
        if isinstance(dbconfig, dict) and len(dbconfig) > 0:
            
            try:
                connecturl = URL(**dbconfig)
                self.dbengine = create_engine(connecturl, pool_recycle=_config.SQLALCHEMY_POOL_RECYCLE, pool_pre_ping=True, pool_size=_config.SQLALCHEMY_POOL_SIZE, max_overflow=_config.SQLALCHEMY_POOL_MAX_OVERFLOW, isolation_level=_config.ISOLATION_LEVEL)
                # connect() method raises an OperationalError if the database does not exist
                self.dbconnection = self.dbengine.connect()
            # except exc.OperationalError as e
            except TypeError as e:
                self.logger.error("Unable to create the connection URL due to: {0}".format(e))
                return False
            except exc.SQLAlchemyError as e:
                # self.logger.error("Encountered general SQLAlchemyError!")
                self.logger.error("Unable to connect to database due to: {0}".format(e))
                if self.create_database_sqla(dbconfig):
                    self.dbengine = create_engine(connecturl, pool_recycle=_config.SQLALCHEMY_POOL_RECYCLE, pool_pre_ping=True, pool_size=_config.SQLALCHEMY_POOL_SIZE, max_overflow=_config.SQLALCHEMY_POOL_MAX_OVERFLOW, isolation_level=_config.ISOLATION_LEVEL)
                    Session = sessionmaker(bind=self.dbengine)
                    self.dbsession = Session()
            else:
                Session = sessionmaker(bind=self.dbengine)
                self.dbsession = Session()
                self.logger.info("Connecting to Database {0} succeed!".format(dbconfig['database']))
            finally:
                pass
        else:
            self.logger.error("The connection config is not meaningful, failed connecting to Database server!")
            return False


    def connect_to_DBLiteserver(self, dbconfig):
        """
        :dbconfig: 
        :drivername – the name of the database backend. This name will correspond to a module in sqlalchemy/databases or a third party plug-in.
        :username – The user name.
        :password – database password.
        :host – The name of the host.
        :port – The port number.
        :database – The database name.
        :query – A dictionary of options to be passed to the dialect and/or the DBAPI upon connect.
        """
        # '数据库类型+数据库驱动名称://用户名:口令@机器地址:端口号/数据库名'
        # MySQL-Connector version:
        # mysql+mysqlconnector://<user>:<password>@<host>[:<port>]/<dbname>
        if isinstance(dbconfig, dict) and len(dbconfig) > 0:
            
            try:
                self.dbliteengine = create_engine("sqlite://")
                # connect() method raises an OperationalError if the database does not exist
                self.dbliteconnection = self.dbliteengine.connect()
            # except exc.OperationalError as e
            except TypeError as e:
                self.logger.error("Unable to create the connection URL due to: {0}".format(e))
                return False
            except exc.SQLAlchemyError as e:
                # self.logger.error("Encountered general SQLAlchemyError!")
                self.logger.error("Unable to connect to database due to: {0}".format(e))
                if self.create_database_sqlitea(dbconfig):
                    self.dbliteengine = create_engine("sqlite://")
                    Session = sessionmaker(bind=self.dbliteengine)
                    self.dblitesession = Session()
            else:
                Session = sessionmaker(bind=self.dbliteengine)
                self.dblitesession = Session()
                self.logger.info("Connecting to Database {0} succeed!".format(dbconfig['database']))
            finally:
                pass
        else:
            self.logger.error("The connection config is not meaningful, failed connecting to Database server!")
            return False


    def create_db_tables_sqla(self):
        """create series of DB tables if not exist"""
        # drop all tables at once
        # BaseModel.metadata.drop_all(self.dbengine) 
        try:
            BaseModel.metadata.create_all(bind=self.dbengine)    
        except exc.SQLAlchemyError as e:
            self.logger.error("Failed creating DB tables due to: {0}".format(e))
            return False
        else:
            for _table in BaseModel.metadata.tables:
                self.logger.info("Table {name} is successfully created!".format(name=_table))
        finally:
            pass

    def create_db_tables_sqlitea(self):
        """create series of DB tables if not exist"""
        # drop all tables at once
        # BaseModel.metadata.drop_all(self.dbengine) 
        try:
            BaseModel.metadata.create_all(bind=self.dbliteengine)    
        except exc.SQLAlchemyError as e:
            self.logger.error("Failed creating DB tables due to: {0}".format(e))
            return False
        else:
            for _table in BaseModel.metadata.tables:
                self.logger.info("Table {name} is successfully created!".format(name=_table))
        finally:
            pass


    # TODO: this is a stopgap measure, need to do this via a batch for efficiency
    def insert_or_update_orders_sqla(self, data, extra_data = {}):
        if isinstance(data, dict):
            for key, datam in data.items():
                datam.update(extra_data)
                try:
                    # _order = self.dbsession.query(Execution_log).filter(
                    _order = self.dblitesession.query(Execution_log).filter(
                        Execution_log.TradingPair == datam.get('TradingPair'),
                        Execution_log.Exchange == datam.get('Exchange'),
                        Execution_log.AccountKey == datam.get('AccountKey'),
                        Execution_log.OrderID == datam.get('OrderID')
                    ).first() # there should only be 1 record
                    if _order is None:
                        self.insert_execution_order_sqla(datam)
                    else:
                        self.update_execution_order_sqla(datam, _order.ID)
                except Exception as e:
                    self.logger.error(f"Error encounted during insert/update: {e}")


    def insert_execution_order_sqla(self, orderdetails):
        """
        :orderdetails: OrderID, TimeStamp, TradingPair, Exchange, AccountKey, LongShort, 
            Amount, FilledAmount, Price, FilledCashAmount, BuyCommission, SellCommission, 
            NetFilledAmount, NetFilledCashAmount, Status, Source, BotID
        :insert a new order record to DB table Execution_log
        """
        if isinstance(orderdetails, dict) and len(orderdetails) > 0:
            orderdetails['TimeStamp'] = datetime.utcfromtimestamp(int(orderdetails.get('TimeStamp', 0)/1000))
            try:
                _order = Execution_log(**orderdetails)
                # self.dbsession.add(_order)
                # self.dbsession.commit()
                self.dblitesession.add(_order)
                self.dblitesession.commit()
            except exc.SQLAlchemyError as e:
                # self.dbsession.rollback()
                self.dblitesession.rollback()
                self.logger.error("Failed inserting Order {orderid} record due to: {err}".format(orderid=orderdetails.get('OrderID', 0), err=e))
                return False
            else:
                # logging only if necessary
                self.logger.debug(f"==========> insert_execution_order_sqla {_order.ID}")
                return _order.ID
        else:
            self.logger.error("The orderdetails dict is empty. Failed inserting Order!")
            return False


    def update_execution_order_sqla(self, orderdetails, recordid):
        """
        :orderdetails:
        :recordid: automatically generated ID for each Order record 
        :update a given order record in DB table Execution_log
        """
        if isinstance(orderdetails, dict) and len(orderdetails) > 0:
            update_timestamp = False
            try:
                orderdetails['TimeStamp'] = datetime.utcfromtimestamp(int(orderdetails.get('TimeStamp', 0)/1000))
                update_timestamp = True
            except:
                self.logger.warning(f"failed to convert timestamp {orderdetails.get('TimeStamp', 0)}. timestamp will not be updated")
            try:
                # self.dbsession.query(Execution_log).filter(Execution_log.ID == recordid).update({
                #     Execution_log.FilledAmount: orderdetails.get('FilledAmount', 0),
                #     Execution_log.FilledCashAmount: orderdetails.get('FilledCashAmount', 0),
                #     Execution_log.BuyCommission: orderdetails.get('BuyCommission', -1),
                #     Execution_log.SellCommission: orderdetails.get('SellCommission', -1),
                #     Execution_log.NetFilledAmount: orderdetails.get('NetFilledAmount', -1),
                #     Execution_log.NetFilledCashAmount: orderdetails.get('NetFilledCashAmount', -1),
                #     Execution_log.TimeStamp: orderdetails.get('TimeStamp'),
                #     Execution_log.Status: orderdetails.get('Status')
                # })
                # record = self.dbsession.query(Execution_log).filter(Execution_log.ID == recordid).first()
                record = self.dblitesession.query(Execution_log).filter(Execution_log.ID == recordid).first()
                self.logger.debug(f"==========> update_execution_order_sqla {record}")
                if record is not None:
                    record.FilledAmount = orderdetails.get('FilledAmount', 0)
                    record.FilledCashAmount = orderdetails.get('FilledCashAmount', 0)
                    record.BuyCommission = orderdetails.get('BuyCommission', -1)
                    record.SellCommission = orderdetails.get('SellCommission', -1)
                    record.NetFilledAmount = orderdetails.get('NetFilledAmount', -1)
                    record.NetFilledCashAmount = orderdetails.get('NetFilledCashAmount', -1)
                    if update_timestamp:
                        record.TimeStamp = orderdetails.get('TimeStamp')
                    record.Status = orderdetails.get('Status')
                    price = orderdetails.get('Price',-1)
                    amount = orderdetails.get('Amount',-1)
                    if(price != -1 and amount != -1):
                        record.Price = price
                        record.Amount = amount

                # self.dbsession.commit()
                self.dblitesession.commit()
            except exc.SQLAlchemyError as e:
                # self.dbsession.rollback()
                self.dblitesession.rollback()
                self.logger.error("Failed updating Order {orderid} record due to: {err}".format(orderid=orderdetails.get('OrderID', 0), err=e))
                return False
            else:
                pass
        else:
            self.logger.error("The orderdetails dict is empty. Failed updating Order!")
            return False


    def delete_execution_order_sqla(self, orderdetails, recordid):
        """
        :orderdetails:
        :recordid: automatically generated ID for each Order record
        :delete a given order record in DB table Execution_log
        """
        if isinstance(orderdetails, dict) and len(orderdetails) > 0:
            try:
                # self.dbsession.query(Execution_log).filter(Execution_log.ID == recordid).delete()
                # self.dbsession.commit()
                self.dblitesession.query(Execution_log).filter(Execution_log.ID == recordid).delete()
                self.dblitesession.commit()
            except exc.SQLAlchemyError as e:
                # self.dbsession.rollback()
                self.dblitesession.rollback()
                self.logger.error("Failed deleting Order {orderid} record due to: {err}".format(orderid=orderdetails.get('OrderID', 0), err=e))
                return False
            else:
                pass
        else:
            self.logger.error("The orderdetails dict is empty. Failed deleting Order!")
            return False


    def clear_unfilled_execution_orders_sqla(self):
        """delete remaining unfilled orders in DB table Execution_log, used at Bot starting & exiting"""
        pass


    def insert_bot_info_sqla(self, botdetails):
        """
        :botdetails: ProcessID, PidName, IpAddr, Parameters, StartTime, EndTime, Status
        :insert a Bot record into DB table Bot_log
        """
        if isinstance(botdetails, dict) and len(botdetails) > 0:
            try:
                if self.logger.port > 0:
                    botdetails['ID'] = self.logger.port
                botdetails['IpAddr'] = _config.SERVER_IP
                _bot = Bot_log(**botdetails)
                self.dbsession.add(_bot)
                self.dbsession.commit()
            except exc.SQLAlchemyError as e:
                self.dbsession.rollback()
                self.logger.error("Failed inserting Bot record due to: {0}".format(e))
                return False
            else:
                # logging only if necessary
                return _bot.ID
        else:
            self.logger.error("The Bot info dict is empty. Failed inserting Bot record!")
            return False
    

    def update_bot_info_sqla(self, botdetails, recordid):
        """
        :botdetails:
        :update a Bot record in DB table Bot_log
        """
        if isinstance(botdetails, dict) and len(botdetails) > 0:
            # if botdetails.get('EndTime', None):
                # botdetails['EndTime'] = datetime.utcfromtimestamp(int(botdetails.get('EndTime', 0)/1000))
            try:
                self.dbsession.query(Bot_log).filter(Bot_log.ID == recordid).update({
                    Bot_log.Parameters: botdetails.get('Parameters', None),
                    Bot_log.Status: botdetails.get('Status', None),
                    Bot_log.EndTime: botdetails.get('EndTime', None)
                })
                self.dbsession.commit()
            except exc.SQLAlchemyError as e:
                self.dbsession.rollback()
                self.logger.error("Failed updating Bot record due to: {0}".format(e))
                return False
            else:
                pass
        else:
            self.logger.error("The Bot info dict is empty. Failed updating Bot record!")
            return False
    

    def delete_bot_info_sqla(self, botdetails, recordid):
        """
        :botdetails:
        :delete a Bot record in DB table Bot_log
        """
        # TODO warning 
        if isinstance(botdetails, dict) and len(botdetails) > 0:
            try:
                self.dbsession.query(Bot_log).filter(Bot_log.ID == recordid).delete()
                self.dbsession.commit()
            except exc.SQLAlchemyError as e:
                self.dbsession.rollback()
                self.logger.error("Failed deleting Bot record due to: {0}!".format(e))
                return False
            else:
                pass
        else:
            self.logger.error("The Bot info dict is empty. Failed deleting Bot record!")
            return False
    

    def get_bot_info_sqla(self, botid):
        """
        :
        :retrieve a bot record with given id
        """
        try:
            _bot = self.dbsession.query(Bot_log).filter(
                Bot_log.ID == botid
            ).first()
        except ValueError:
            # TODO logging
            return False
        except exc.SQLAlchemyError as e:
            self.dbsession.rollback()
            self.logger.error("Failed retrieving Bot#{id} record due to: {err}!".format(id=botid, err=e))
            return False
        else:
            return _bot


    def get_record_from_db(self, tradingpair, exchange, account, num=100, status='Not Completed', source='Manual', botid = None):
        # retrieve records
        if not isinstance(tradingpair, list):
            tradingpair = [tradingpair]
        if not isinstance(exchange, list):
            exchange = [exchange]
        if not isinstance(account, list):
            account = [account]
        if not isinstance(status, list):
            status = [status]
        if not isinstance(source, list):
            source = [source]
        if botid is not None and not isinstance(botid, list):
            botid = [botid]
        try:
            self.logger.debug(f"<get_record_from_db> starting db query")
            # _order = self.dbsession.query(Execution_log).filter(
            _order = self.dblitesession.query(Execution_log).filter(
                Execution_log.TradingPair.in_(tradingpair),
                Execution_log.Exchange.in_(exchange),
                Execution_log.AccountKey.in_(account),
                Execution_log.Status.in_(status),
                Execution_log.Source.in_(source)
                )
            if botid is not None:
                _order = _order.filter(
                    Execution_log.BotID.in_(botid)
                )
            _order = _order.order_by(Execution_log.ID.desc()).limit(num).all()
            self.logger.debug(f"<get_record_from_db> db query complete")
        except ValueError as e:
            # TODO logging
            self.logger.debug(f"<get_record_from_db> db query error {e}")
            return []
        except exc.SQLAlchemyError as e:
            self.logger.debug(f"<get_record_from_db> db query error {e}")
            # self.dbsession.rollback()
            self.dblitesession.rollback()
            self.logger.error("Failed retrieving records: {0}!".format(e))
            return []
        else:
            return _order


    def get_ghost_orders(self, tradingpair, exchange, account, status='Ghost', source='Automatic'):
        """
        :retrieve all Ghost Orders from DB table Execution_log
        """
        # TODO probably add another return variable, like list of order id
        try:
            # _ghorders = self.dbsession.query(Execution_log).filter(
            _ghorders = self.dblitesession.query(Execution_log).filter(
                Execution_log.TradingPair == tradingpair,
                Execution_log.Exchange == exchange,
                Execution_log.AccountKey == account,
                Execution_log.Status == status,
                Execution_log.Source == source
                ).order_by(Execution_log.ID.desc())
        except ValueError:
            # TODO logging
            return False
        except exc.SQLAlchemyError as e:
            # self.dbsession.rollback()
            self.dblitesession.rollback()
            self.logger.error("Failed retrieving Ghost Orders due to: {0}!".format(e))
            return False
        else:
            return _ghorders


    # TODO: this and get_execution_report_botid_sqla are duplicates
    def get_aggregate_sqla(self, botid):
        """
        :
        :calculate aggregate
        """
        try:
            # _agg = self.dbsession.query(func.sum(Execution_log.FilledAmount),func.sum(Execution_log.FilledCashAmount)).filter(
            _agg = self.dblitesession.query(func.sum(Execution_log.FilledAmount),func.sum(Execution_log.FilledCashAmount)).filter(
                Execution_log.BotID == botid
                ).all()
        except ValueError:
            # TODO logging
            return False
        except exc.SQLAlchemyError as e:
            # self.dbsession.rollback()
            self.dblitesession.rollback()
            self.logger.error("Failed retrieving records due to: {0}!".format(e))
            return False
        else:
            return _agg


    def get_latest_execution_orders_sqla(self, tradingpair, exchange, account, num=20, source='Automatic'):
        """
        :
        :retrieve n Order records from DB table Execution_log
        """
        try:
            # _orders = self.dbsession.query(Execution_log).filter(
            _orders = self.dblitesession.query(Execution_log).filter(
                Execution_log.TradingPair == tradingpair,
                Execution_log.Exchange == exchange,
                Execution_log.AccountKey == account,
                Execution_log.Source == source
                ).order_by(Execution_log.ID.desc()).limit(num)
        except ValueError:
            # TODO logging
            return False
        except exc.SQLAlchemyError as e:
            # self.dbsession.rollback()
            self.dblitesession.rollback()
            self.logger.error("Failed retrieving Order records due to: {0}!".format(e))
            return False
        else:
            return _orders
    
    def all_orders(self):
        return self.dblitesession.query(Execution_log).all()
    
    def get_historical_execution_aggregates_sqla(self, tradingpair, exchange, account, begintime, endtime, source='Automatic', bot_id=None):
        """
        :UTC Time
        :retrieve aggregate figures for records between a given time period from DB table Execution_log
        """
        try:
            # _aggregate = self.dbsession.query(
            _aggregate = self.dblitesession.query(
                Execution_log.LongShort,
                func.sum(Execution_log.Amount).label('Amount'),
                func.sum(Execution_log.Amount * Execution_log.Price).label('CashAmount'),
                func.sum(Execution_log.FilledAmount).label('FilledAmount'),
                func.sum(Execution_log.FilledCashAmount).label('FilledCashAmount')
            ).filter(
                Execution_log.TradingPair == tradingpair,
                Execution_log.Exchange == exchange,
                Execution_log.TimeStamp > begintime,
                Execution_log.TimeStamp < endtime,
                Execution_log.Source == source
                )
            if isinstance(account,str):
                _aggregate = _aggregate.filter(    
                    Execution_log.AccountKey == account
                )
            elif isinstance(account,list):
                _aggregate = _aggregate.filter(    
                    Execution_log.AccountKey.in_(account)
                )
            if isinstance(bot_id, str):
                _aggregate = _aggregate.filter(    
                    Execution_log.BotID == bot_id
                )
            _aggregate = _aggregate.group_by(
                    Execution_log.LongShort
                ).all()
            self.logger.debug(f"==========> get_historical_execution_aggregates_sqla {_aggregate}")
        except ValueError:
            # TODO logging
            self.logger.error(e)
            return False
        except exc.SQLAlchemyError as e:
            # self.dbsession.rollback()
            self.dblitesession.rollback()
            self.logger.error("Failed retrieving Order records due to: {0}!".format(e))
            return False
        except Exception as e:
            self.logger.error(e)
            return False
        else:
            return _aggregate


    def get_historical_execution_orders_sqla(self, tradingpair, exchange, account, begintime, endtime, source='Automatic'):
        """
        :UTC Time
        :retrieve Order records between a given time period from DB table Execution_log
        """
        try:
            # _orders = self.dbsession.query(Execution_log).filter(
            _orders = self.dblitesession.query(Execution_log).filter(
                Execution_log.TradingPair == tradingpair,
                Execution_log.Exchange == exchange,
                Execution_log.AccountKey == account,
                Execution_log.TimeStamp > begintime,
                Execution_log.TimeStamp < endtime,
                Execution_log.Source == source
            ).order_by(Execution_log.ID.desc()).all()
        except ValueError:
            # TODO logging
            self.logger.error(e)
            return False
        except exc.SQLAlchemyError as e:
            # self.dbsession.rollback()
            self.dblitesession.rollback()
            self.logger.error("Failed retrieving Order records due to: {0}!".format(e))
            return False
        else:
            return _orders
    

    def get_execution_report_sqla(self, tradingpair, exchange, account, begintime, endtime, source='Automatic'):
        """
        :UTC Time
        :generate an execution report
        """
        try:
            # _report = self.dbsession.query(
            _report = self.dblitesession.query(
                Execution_log.TradingPair,
                Execution_log.Exchange,
                Execution_log.AccountKey,
                Execution_log.LongShort,
                func.count(Execution_log.OrderID).label('Count'),
                func.sum(Execution_log.FilledAmount).label('TotalFilledAmount'),
                func.sum(Execution_log.FilledCashAmount).label('TotalFilledCashAmount'),
                func.sum(Execution_log.BuyCommission).label('TotalBuyCommission'),
                func.sum(Execution_log.SellCommission).label('TotalSellCommission'),
                func.sum(Execution_log.NetFilledAmount).label('TotalNetFilledAmount'),
                func.sum(Execution_log.NetFilledCashAmount).label('TotalNetFilledCashAmount'),
                (func.sum(Execution_log.NetFilledCashAmount)/func.sum(Execution_log.NetFilledAmount)).label('WeightedAvgPrice')
                ).filter(
                    Execution_log.TradingPair == tradingpair,
                    Execution_log.Exchange == exchange,
                    Execution_log.AccountKey == account,
                    Execution_log.TimeStamp > begintime,
                    Execution_log.TimeStamp < endtime,
                    Execution_log.Status.in_(['Completed', 'Not Completed']),
                    Execution_log.Source == source
                    ).group_by(
                        Execution_log.TradingPair,
                        Execution_log.Exchange,
                        Execution_log.AccountKey,
                        Execution_log.LongShort
                        ).all()
        except ValueError:
            # TODO logging
            self.logger.error(e)
            return False
        except exc.SQLAlchemyError as e:
            # self.dbsession.rollback()
            self.dblitesession.rollback()
            self.logger.error("Failed generating execution report due to: {0}!".format(e))
            return False
        else:
            return _report
    

    # TODO: this and get_aggregate_sqla are duplicates
    def get_execution_report_botid_sqla(self, tradingpair, exchange, account, botid, source='Automatic'):
        """
        :return executed results by BotID
        """
        try:
            # _report = self.dbsession.query(
            _report = self.dblitesession.query(
                Execution_log.TradingPair,
                Execution_log.Exchange,
                Execution_log.AccountKey,
                Execution_log.LongShort,
                func.count(Execution_log.OrderID).label('Count'),
                func.sum(Execution_log.FilledAmount).label('TotalFilledAmount'),
                func.sum(Execution_log.FilledCashAmount).label('TotalFilledCashAmount'),
                func.sum(Execution_log.BuyCommission).label('TotalBuyCommission'),
                func.sum(Execution_log.SellCommission).label('TotalSellCommission'),
                func.sum(Execution_log.NetFilledAmount).label('TotalNetFilledAmount'),
                func.sum(Execution_log.NetFilledCashAmount).label('TotalNetFilledCashAmount'),
                (func.sum(Execution_log.NetFilledCashAmount)/func.sum(Execution_log.NetFilledAmount)).label('WeightedAvgPrice')
                ).filter(
                    Execution_log.TradingPair == tradingpair,
                    Execution_log.Exchange == exchange,
                    Execution_log.AccountKey == account,
                    Execution_log.BotID == botid,
                    Execution_log.Status.in_(['Completed', 'Not Completed']),
                    Execution_log.Source == source
                    ).group_by(
                        Execution_log.TradingPair,
                        Execution_log.Exchange,
                        Execution_log.AccountKey,
                        Execution_log.LongShort
                        ).all()
        except ValueError:
            # TODO logging
            self.logger.error(e)
            return False
        except exc.SQLAlchemyError as e:
            # self.dbsession.rollback()
            self.dblitesession.rollback()
            self.logger.error("Failed generating execution report due to: {0}!".format(e))
            return False
        else:
            return _report
  


    #============================#
    #   Risk Management System   #
    #============================#
    def insert_riskmetric_sqla(self, riskmetric):
        """
        :riskmetric: RiskMetric, Definition, AuditTrail
        :insert a new risk metric into DB table Risk_Metrics
        """
        warnings.warn("insert or update risk metric shall limit to authorized user only!")
        
        if isinstance(riskmetric, dict) and len(riskmetric) > 0:
            try:
                _metric = Risk_Metrics(**riskmetric)
                self.dbsession.add(_metric)
                self.dbsession.commit()
            except exc.SQLAlchemyError as e:
                self.dbsession.rollback()
                self.logger.error("Failed inserting a new risk metric due to: {0}".format(e))
                return False
            else:
                return _metric.ID
        else:
            self.logger.error("The risk metric dict is empty. Failed inserting a new risk metric!")
            return False


    def update_riskmetric_sqla(self, riskmetric):
        """
        :riskmetric: ID, RiskMetric, Definition, AuditTrail
        :update an existing risk metric in DB table Risk_Metrics
        """
        warnings.warn("insert or update risk metric shall limit to authorized user only!")
        
        if isinstance(riskmetric, dict) and len(riskmetric) > 0:
            try:
                self.dbsession.query(Risk_Metrics).filter(Risk_Metrics.RiskMetric == riskmetric.RiskMetric).update({
                    Risk_Metrics.Definition: riskmetric.get('Definition', None),
                    Risk_Metrics.AuditTrail: riskmetric.get('AuditTrail', None)
                })
                self.dbsession.commit()
            except exc.SQLAlchemyError as e:
                self.dbsession.rollback()
                self.logger.error("Failed updating an existing risk metric due to: {0}".format(e))
                return False
            else:
                pass
        else:
            self.logger.error("The risk metric dict is empty. Failed updating an existing risk metric!")
            return False
    

    def insert_risklimit_sqla(self, risklimit):
        """
        :risklimit: RiskMetric, Trader, Exchange, Account, TradingPair, MinLimit, MaxLimit, Unit, AuditTrail
        :insert a new risk limit into DB table Risk_Limits
        """
        warnings.warn("insert or update risk limit shall limit to authorized user only!")

        if isinstance(risklimit, dict) and len(risklimit) > 0:
            try:
                _limit = Risk_Limits(**risklimit)
                self.dbsession.add(_limit)
                self.dbsession.commit()
            except exc.SQLAlchemyError as e:
                self.dbsession.rollback()
                self.logger.error("Failed inserting a new risk limit due to: {0}".format(e))
                return False
            else:
                return _limit.ID
        else:
            self.logger.error("The risk limit dict is empty. Failed inserting a new risk limit!")
            return False


    def update_risklimit_sqla(self, risklimit, recordid):
        """
        :risklimit: RiskMetric, Trader, Exchange, Account, TradingPair, MinLimit, MaxLimit, Unit, AuditTrail
        :update an existing risk limit in DB table Risk_Limits
        """
        warnings.warn("insert or update risk limit shall limit to authorized user only!")

        if isinstance(risklimit, dict) and len(risklimit) > 0:
            try:
                # TODO add more fields based on needs
                self.dbsession.query(Risk_Limits).filter(
                    Risk_Limits.ID == recordid
                    ).update({
                        Risk_Limits.MinLimit: risklimit.get('MinLimit', None),
                        Risk_Limits.MaxLimit: risklimit.get('MaxLimit', None),
                        Risk_Limits.AuditTrail: risklimit.get('AuditTrail', None)
                        })
                self.dbsession.commit()
            except exc.SQLAlchemyError as e:
                self.dbsession.rollback()
                self.logger.error("Failed updating a risk limit due to: {0}".format(e))
                return False
            else:
                pass
        else:
            self.logger.error("The risk limit dict is empty. Failed updating a risk limit!")
            return False
    

    def insert_riskconsumption_sqla(self, riskcon):
        """
        :risklimit: RiskMetric, Trader, Exchange, Account, TradingPair, MinLimit, MaxLimit, Unit, AuditTrail
        :insert a new risk limit into DB table Risk_Limits
        """
        if isinstance(riskcon, dict) and len(riskcon) > 0:
            try:
                _consumption = Risk_Consumption(**riskcon)
                self.dbsession.add(_consumption)
                self.dbsession.commit()
            except exc.SQLAlchemyError as e:
                self.dbsession.rollback()
                self.logger.error("Failed inserting a new risk consumption due to: {0}".format(e))
                return False
            else:
                return _consumption.ID
        else:
            self.logger.error("The risk consumption dict is empty. Failed inserting a new risk consumption!")
            return False


    def update_riskconsumption_sqla(self, riskcon, recordid):
        """
        :risklimit: RiskMetric, Trader, Exchange, Account, TradingPair, MinLimit, MaxLimit, Unit, AuditTrail
        :update an existing risk limit in DB table Risk_Limits
        """
        if isinstance(riskcon, dict) and len(riskcon) > 0:
            try:
                # TODO add more fields based on needs
                self.dbsession.query(Risk_Consumption).filter(
                    Risk_Consumption.ID == recordid
                    ).update({
                        Risk_Consumption.Value: riskcon.get('Value', None),
                        Risk_Consumption.Percentage: riskcon.get('%', None),
                        Risk_Consumption.TimeStamp: riskcon.get('TimeStamp', None)
                        })
                self.dbsession.commit()
            except exc.SQLAlchemyError as e:
                self.dbsession.rollback()
                self.logger.error("Failed updating a risk consumption due to: {0}".format(e))
                return False
            else:
                pass
        else:
            self.logger.error("The risk consumption dict is empty. Failed updating a risk consumption!")
            return False


    def get_risklimits_sqla(self):
        """retrieve whole risk limits table"""
        # TODO join with riskmetrics table to bring in the definition
        try:
            _risklimits = self.dbsession.query(Risk_Limits).all()
        except ValueError:
            # TODO logging
            self.logger.error(e)
            return False
        except exc.SQLAlchemyError as e:
            self.dbsession.rollback()
            self.logger.error("Failed retrieving risk limits due to: {0}!".format(e))
            return False
        else:
            return _risklimits
    

    def get_riskmetrics_sqla(self):
        """retrieve whole risk metrics table"""
        # TODO join with riskmetrics table to bring in the definition
        try:
            _riskmetrics = self.dbsession.query(Risk_Metrics).all()
        except ValueError:
            # TODO logging
            self.logger.error(e)
            return False
        except exc.SQLAlchemyError as e:
            self.dbsession.rollback()
            self.logger.error("Failed retrieving risk metrics due to: {0}!".format(e))
            return False
        else:
            return _riskmetrics
    

    def get_realtime_orderrate(self, exchange, begintime, endtime):
        """
        :UTC Time
        :report the frequency of order making in a given time period
        """
        try:
            # _count = self.dbsession.query(
            _count = self.dblitesession.query(
                Execution_log.Exchange,
                func.count(Execution_log.OrderID).label('Count')
                ).filter(
                    Execution_log.Exchange == exchange,
                    Execution_log.TimeStamp > begintime,
                    Execution_log.TimeStamp < endtime
                    ).group_by(
                        Execution_log.Exchange
                        ).all()
        except TypeError as e:
            # TODO logging
            self.logger.error(e)
            return False
        except ValueError as e:
            # TODO logging
            self.logger.error(e)
            return False
        except exc.SQLAlchemyError as e:
            # self.dbsession.rollback()
            self.dblitesession.rollback()
            self.logger.error("Failed reporting the frequncy of order making due to: {0}!".format(e))
            return False
        except Exception as e:
            self.logger.error(e)
            return False
        else:
            return _count
