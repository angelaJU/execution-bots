Conclusion & System Architecture Summary
TWAP (Time-Weighted Average Price) trading bot, designed to execute orders gradually over time while monitoring market conditions, orderbooks, account operations, and error states. The core logic is encapsulated in the TWAPBot class, supported by a variety of supporting constants, enums, helper functions, and state-management components.
Below is a structured breakdown of all its parts.
1. Global Constants & Error Codes
These constants define:
Bot Actions
START, PAUSE, RESUME
 Used to control the bot’s execution state.


Backoff Parameters
Force-balance-check exponential backoff settings
 Used when the bot repeatedly fails to refresh account balance.


Operational Delays
Sleep after failed orders


Waiting time after cancellations


Error Messages
Provide consistent error reporting for:
Invalid configs


Missing base/quote symbols


Min order quantity violations


Price/qty calculation errors


Duration errors
 and more.



2. Enums — Control the Bot’s Internal State
(a) Aggressiveness
Used to determine pricing mode:
TICK_BETTER


TAKING


(b) TradableBitMask
Bitmask to represent market readiness:
MarketStatus


MarketDataStale


PricerNotReady
 Used to determine if orders are allowed.


(c) BotStatus
Represents the overall bot state:
WAITING
ORDER_SUBMITTED
ERROR
STRATEGY_COMPLETED
STOP_CONDITION_MET
NOT_ENOUGH_BALANCE
 etc.


Used for redis persistence and UI updates.

3. Core Class: TWAPBot
✔ Loads config
 ✔ Loads instrument data
 ✔ Loads account operation state (from Redis)
 ✔ Starts orderbook listeners
 ✔ Computes TWAP parameters
 ✔ Sends orders periodically
 ✔ Tracks fills via OrderMonitor
 ✔ Manages errors
 4. Constructor — Major Components
Client Subsystems
self.client (main account)
self.ref_client (reference prices from another venue)


Internal Variables
base/quote, pair
total quantity
duration
slice size
post frequency
min order qty
order IDs
thresholds / conditions
leverage
bitmasks


Monitoring
OrderMonitor instance
Tracks open orders, completed orders
Allows cancelation / refreshing statuses


Initialization Steps
get_account_operation() — loads state from Redis
initialise_order_monitor() — loads open/completed orders
start_book_listener() — orderbook streaming


load_instruments_data() — min size, tick sizes, leverage
load_startegy_params() — load slice_size etc.
check_strategy_params() — recompute strategy dynamically
5. Strategy Reset Functions
reset_twap_strategy()
Resets:
quantities
slice parameters
progress trackers
bitmasks
timers


Used when restarting or reloading configs.
6. Strategy Parameter Management
load_startegy_params()
Loads from Redis:
post_frequency
slice_size
posts_completed
total_no_of_posts
bot_progress_duration
last_min_order_qty
last order id/qty


check_strategy_params()
If market min order size changed:
recompute TWAP parameters


calculate_strategy_params(qty, duration)
Computes:
slice size
frequency
number of remaining posts


Uses formula:
post_frequency = duration * slice_size / qty
Ensures:
frequency cannot go below default
slice_size respects min_order_qty and precision
7. Orderbook & Price Handling
validate_orderbook(ob)
Ensures:
bids/asks not empty
timestamp exists
data is fresh
orderbook latency acceptable
Updates bitmask flags.
update_market_data()
reads orderbook
sets TOB (tob/ask, mid)
clears bitmask flags when fresh
validate_ref_orderbook(), get_reference_price()
Used to fetch reference prices from other exchanges.
 8. Instrument & Exchange Handling
load_instruments_data()
Pulls data:
min_order_size
min_notional
tick sizes
leverage


Calls:
update_market_data()
update_min_qty(mid)
update_min_qty(price)
Computes real-time min order quantity based on:
coin-margined mode
min notional
min size
get_exchange_symbol()
Adapter to convert common symbol → exchange-specific symbol.

9. Account State / Redis Interaction
get_account_operation()
Loads previous bot run state from Redis position cache.
initialise_order_monitor()
Loads order state from Redis:
open orders
completed orders
update_remark(...)
Validate JSON remark.
update_trigger_condition(...)
Validates string: "exchange;pair;direction;value"
update_stop_condition(...)
Validates string: "asset;direction;value"

10. Properties & Helpers
remain_qty:total_quantity - dealt_so_far
side (property):Wraps value into enum Side.
action:Returns bot action.
update_action(value)
Handles timing logic for:
pause


resume



11. Interactions Between Components
TWAPBot interacts with:
Component
Purpose
client
order placement, orderbook, instruments
ref_client
reference prices
OrderMonitor
track fills, cancel orders, keep fill state
Redis (via account_operation)
store bot progress
HelmClient (book listener)
real-time orderbook subscription
instrument_data
get min order size, precision, leverage
config
broker/orderbook timing thresholds

Main Data Flow
Load config → extract parameters


Load instrument → determine min size, leverage


Load state from Redis → restore progress


Update market data → get TOB / mid


Compute TWAP parameters:


slice_size


post_frequency


Loop:


post an order


wait post_frequency seconds


use OrderMonitor to confirm fills


update progress


Stop when:


remain_qty == 0


stop condition hit
error

Final Summary
market data ingestion


dynamic computation of TWAP parameters


min-notional constraints


state persistence


order tracking and reconciliation


error handling and retry logic


exchange-symbol translation


account operation watchdogs


trigger/stop conditions


It is highly modular, separating:
strategy parameters


market readiness


order monitoring


instrument metadata


pricing logic


Redis restoration


This allows the bot to recover from restarts, adapt to changing market conditions, and remain safe through multiple layers of validation.
run()
 ├─ validate_config_parameters()
 ├─ cancel_pending_order()
 │    └─ order_monitor.*
 ├─ update_market_data()
 │    └─ client.*
 ├─ check_stop_condition()
 │    └─ get_coin_tradable_balance()
 ├─ check_trigger_condition()
 │    └─ get_reference_price()
 ├─ get_last_unfilled_qty()
 │    └─ order_monitor.*
 ├─ get_price()
 ├─ balance_can_meet_order()
 │    └─ get_future_balance()
 │         └─ dual_side_position_of()
 ├─ get_placed_orders_volume()
 │    └─ order_monitor.*
 ├─ send_order()
 │    └─ client.buy()/sell()
 ├─ push_bot_data_to_redis()
 │    └─ client.post()
 └─ bot_position
      └─ balance()

Parameter / Attribute
Type
Purpose / Usage
instrument_type
str
"SPOT" or "FUTURES"
side
str
BUY / SELL
total_quantity
float
Total quantity to trade
total_duration
float
Total execution duration (s)
threshold_price
float
Max/min price limit for order execution
default_post_frequency
int
Time between order slices (s)
remark
str
Order remark/comment
max_slice_size_multiplier
float
Max allowed order size multiplier
account_id
str
Trading account identifier
pair
str
Trading pair symbol
instrument_data
object
Metadata for instrument (contract size, settlement asset, precision)
slice_size
float
Single order slice size
post_frequency
float
Current order post frequency
order_type
str
LIMIT, MARKET, LIMIT_CLOSE
order_id
str
Last sent order ID
order_quantity
float
Quantity of the last order
remain_qty
float
Remaining quantity to trade
bot_status
BotStatus
Enum: WAITING, ORDER_SUBMITTED, STRATEGY_COMPLETED, ERROR, etc.
last_post_ts
float
Timestamp of last order posting
posts_completed
int
Number of executed order posts
last_min_order_qty
float
Quantity of last minimum order posted
position
dict
Current bot position (Redis snapshot)
om_orders
dict
Order monitor status
last_error
str
Last error description
tradable_bit_mask
int
Bitmask representing market status & pricing readiness
balance_check_backoff
float
Cooldown timer for forced balance check
last_force_rpc_balance_ts
float
Timestamp of last forced balance fetch
consecutive_cancel_count
int
Track repeated cancellations
consecutive_cancel_count_ts
float
Timestamp of last cancel attempt
continues_failed_order_count
int
Track failed orders for cooldown logic
continues_failed_order_count_ts
float
Timestamp of last failed order

3. Function Overview & Interactions
Here is all key functions grouped by category and their interactions:

A. Config & Setup
Function
Purpose
config (getter/setter)
Sets or returns bot configuration dict
validate_config_parameters()
Checks essential parameters (qty, duration, base/quote, account, instrument_data)
update_remark(), update_trigger_condition(), update_stop_condition()
Internal updates for dynamic conditions
__enter__ / __exit__
Context manager for starting/stopping order monitor and deregistering resources

Flow:
 config.setter → validates and updates attributes → validate_config_parameters() checks correctness

B. Market & Instrument Data
Function
Purpose
exchange_name_of(), exchange_id_of(), name_of()
Cached exchange/account lookups
is_futures_of(), dual_side_position_of(), is_coin_margin()
Instrument type checks
order_book_delay_threshold()
Fetches market refresh rate for throttling
is_market_data_good()
Check if tradable_bit_mask allows trading
update_market_data()
Updates TOB/TOA (top-of-book) and other market info
instrument_load_ts, load_instruments_data()
Reload instrument metadata periodically

Flow:
 Used in run() to verify market state before order placement.

C. Order & Execution
Function
Purpose
get_price(aggressiveness)
Determines target order price
get_order_type()
Returns full order type string for API
send_order(price, size)
Submits order via client (buy/sell)
cancel_pending_order()
Cancels unfilled previous orders
get_last_unfilled_qty()
Determines leftover qty from last order
get_placed_orders_volume()
Tracks total executed order volume to prevent overshoot
max_order_size_breached(size)
Risk check for slice sizing

Flow:
 run() → get_last_unfilled_qty() → get_price() → max_order_size_breached() → send_order() → order_monitor.add()

D. Balance & Risk Checks
Function
Purpose
balance property
Fetch available balances (spot/futures)
get_coin_tradable_balance(currency)
Coin-specific balance
balance_can_meet_order(order_price, order_amount)
Ensures account can fund the next order
get_future_balance()
Compute margin for futures with leverage

Flow:
 run() → balance_can_meet_order() → prevent execution if insufficient funds

E. Trigger & Stop Conditions
Function
Purpose
check_trigger_condition()
Conditional market trigger (e.g., reference exchange/pair)
check_stop_condition()
Stop trading if balance or custom metric reached
threshold_price_breached(price)
Limit order execution by threshold price

Flow:
 run() → evaluated before placing order → may halt posting if conditions met

F. Progress Tracking & Redis
Function
Purpose
bot_position
Snapshot of bot with balance, progress, and last order
position
Encapsulates bot status and position
om_orders
Serialized order monitor state
push_bot_data_to_redis()
Periodically pushes bot snapshot to Redis
get_bot_progress_duration(), get_theorotical_progress(), get_actual_progress()
Progress tracking metrics


G. TWAP Execution Cycle
Function
Purpose
run()
Main execution loop:


Validate config


Load market/instrument data


Check bot status


Evaluate stop/trigger conditions


Determine order size and price


Place order via send_order()


Update order monitor and progress |
 | adjusted_post_time() | Ensure correct timing between slices |


Core loop flow:
validate_config_parameters() → check_trigger_condition() → check_stop_condition()
→ get_last_unfilled_qty() → get_price() → max_order_size_breached()
→ balance_can_meet_order() → send_order()
→ order_monitor.add() → push_bot_data_to_redis()


4. TWAP Bot Infrastructure Flowchart
flowchart TD
    A[Config Set] --> B[Validate Parameters]
    B --> C[Load Market Data & Instrument Data]
    C --> D[Check Market Status & Tradable Bitmask]
    D --> E[Check Trigger Condition]
    E --> F[Check Stop Condition]
    F --> G[Determine Last Unfilled Qty]
    G --> H[Determine Target Price & Order Type]
    H --> I[Check Max Order Size & Balance]
    I --> J[Send Order to Exchange]
    J --> K[Add Order to Order Monitor]
    K --> L[Update Bot Progress & Push to Redis]
    L --> M[Wait Until Next Post Interval]
    M --> C

Notes:
Loops continuously until remain_qty <= min_order_qty or bot_status = STRATEGY_COMPLETED.


Error handling for failed orders triggers temporary cooldown (ORDER_FAILED status).


Cancellation logic handles consecutive unfilled orders with cooldown.


 5. Summary of Function Interactions
Initialization


config → validate_config_parameters


__enter__ → starts order monitor


Market Verification


is_market_data_good()


order_book_delay_threshold()


instrument_load_ts → reload instrument data


Execution


get_last_unfilled_qty()


get_price()


max_order_size_breached()


balance_can_meet_order()


send_order() → order_monitor.add()


Progress Tracking


bot_position


position


push_bot_data_to_redis()


Error Handling


check_stop_condition()


check_trigger_condition()


Failed order handling → cooldown


Completion


STRATEGY_COMPLETED → cancel all orders → __exit__ deregisters resources

6: flow chart
flowchart TD
    %% Initialization
    A[Start TWAP Bot] --> B[Set Config (config.setter)]
    B --> C[Validate Config (validate_config_parameters)]
    C -->|Valid| D[Load Market Data & Instrument Data]
    C -->|Invalid| E[Set BotStatus = ERROR]
    D --> F[Check Market Data (is_market_data_good)]
    F -->|Not Good| G[BotStatus = WAITING / Exit Cycle]
    F -->|Good| H[Check Tradable Bitmask]

    %% Pre-Order Checks
    H --> I[Check Trigger Condition (check_trigger_condition)]
    I -->|Not Met| J[BotStatus = TRIGGER_CONDITION_BREACH]
    I -->|Met| K[Check Stop Condition (check_stop_condition)]
    K -->|Met| L[BotStatus = STOP_CONDITION_MET]
    K -->|Not Met| M[Check Threshold Price (threshold_price_breached)]
    M -->|Breached| N[BotStatus = THRESHOLD_PRICE_BREACH]
    M -->|OK| O[Check Last Unfilled Qty (get_last_unfilled_qty)]
    
    %% Determine Order
    O --> P[Determine Target Price (get_price)]
    P --> Q[Check Max Order Size (max_order_size_breached)]
    Q -->|Breached| R[BotStatus = MAX_ORDER_SIZE_BREACH]
    Q -->|OK| S[Check Balance (balance_can_meet_order)]
    S -->|Insufficient| T[BotStatus = NOT_ENOUGH_BALANCE]
    S -->|Sufficient| U[Adjust Order Size to Avoid Overshoot]

    %% Order Execution
    U --> V[Send Order (send_order)]
    V --> W[Add Order to Order Monitor (order_monitor.add)]
    W --> X[Update Last Post Timestamp (adjusted_post_time)]
    X --> Y[Update Bot Progress & Push to Redis (push_bot_data_to_redis)]
    
    %% Loop / Completion
    Y --> Z[Wait Until Next Post Interval]
    Z --> D

    %% Completion
    Z --> AA{Remain Qty < Min Order Qty?}
    AA -->|Yes| AB[Cancel Open Orders (order_monitor.cancel_all_open_orders)]
    AB --> AC[BotStatus = STRATEGY_COMPLETED]
    AA -->|No| D

    %% Error Handling / Failed Orders
    V --> AD{Order Failed?}
    AD -->|Yes| AE[Increment failed order counter, set BotStatus = ORDER_FAILED]
    AD -->|No| AF[Reset failed order counter]

    %% Legend / Data Sources
    subgraph CONFIG
        B
    end
    subgraph MARKET_DATA
        D
        F
        H
        P
    end
    subgraph ORDER_MONITOR
        W
        AB
    end
    subgraph REDIS
        Y
    end
    subgraph BALANCE_CHECK
        S
    end


8: sequenceDiagram
    participant Bot as TWAPBot
    participant OM as OrderMonitor
    participant API as ClientAPI
    participant R as Redis

    Bot->>Bot: run()
    Bot->>Bot: validate_config_parameters()
    Bot->>Bot: update_market_data()
    Bot->>Bot: check_trigger_condition()
    Bot->>Bot: check_stop_condition()
    Bot->>Bot: threshold_price_breached()
    Bot->>Bot: get_last_unfilled_qty()
    Bot->>Bot: get_price()
    Bot->>Bot: max_order_size_breached()
    Bot->>Bot: balance_can_meet_order()

    alt If all checks pass
        Bot->>API: send_order(price, size)
        API-->>Bot: order_id
        Bot->>OM: add(order_id)
        Bot->>R: post(bot_position)
    else If check fails
        Bot->>Bot: set BotStatus (ERROR, STOP_CONDITION, etc.)
    end

    Bot->>Bot: adjusted_post_time()
    Bot->>Bot: update bot progress
    Bot->>Bot: wait until next post_frequency

Summary:

Flow Summary
Bot Execution (run)


Config validation → market data check → trigger/stop/threshold check


Determine order size & price


Balance check → max order size check → overshoot safeguard


Order Placement


Use ClientAPI to place order (buy / sell)


Add order to OrderMonitor


Push bot snapshot to Redis (push_bot_data_to_redis)


Monitoring & Loop


Track last unfilled qty → adjust aggressiveness


Cancel pending orders if needed (cancel_pending_order)


Repeat every post_frequency seconds


Complete when remaining quantity < min_order_qty


Error Handling


Track failed orders and apply cooldown


Log any config / market / balance errors



