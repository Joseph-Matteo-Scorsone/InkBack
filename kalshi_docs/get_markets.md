GetMarkets
==========

get https://api.elections.kalshi.com/trade-api/v2/markets

Endpoint for listing and discovering markets on Kalshi.

Query Params

limit 

int64

1 to 1000

Parameter to specify the number of results per page. Defaults to 100.

cursor 

string

The Cursor represents a pointer to the next page of records in the pagination.

 So this optional parameter, when filled, should be filled with the cursor string returned in a previous request to this end-point.

 Filling this would basically tell the api to get the next page containing the number of records passed on the limit parameter.

 On the other side not filling it tells the api you want to get the first page for another query.

 The cursor does not store any filters, so if any filter parameters like tickers, max_ts or min_ts were passed in the original query they must be passed again.

event_ticker 

string

Event ticker to retrieve markets for.

series_ticker 

string

Series ticker to retrieve contracts for.

max_close_ts 

int64

Restricts the markets to those that are closing in or before this timestamp.

min_close_ts 

int64

Restricts the markets to those that are closing in or after this timestamp.

status 

string

Restricts the markets to those with certain statuses, as a comma separated list.

 The following values are accepted: unopened, open, closed, settled.

tickers 

string

Restricts the markets to those with certain tickers, as a comma separated list.

Response

200
===

Response body

object

cursor 

string

The Cursor represents a pointer to the next page of records in the pagination.

 Use the value returned here in the cursor query parameter for this end-point to get the next page containing limit records.

 An empty value of this field indicates there is no next page.

markets

array of objects

required

markets*

object

can_close_early 

boolean

required

If true then this market can close earlier then the time provided on close_time.

cap_strike 

number

category 

string

required

Deprecated: Category for this market.

close_time 

date-time

required

Date and time in the ISO 8601 spec. Example: 2022-11-30T15:00:00Z

custom_strike

object

Expiration value for each target that leads to a YES settlement.

Filled only if "strike_type" is "custom" or "structured".

Has additional fields

event_ticker 

string

required

Unique identifier for events.

expected_expiration_time 

date-time

Date and time in the ISO 8601 spec. Example: 2022-11-30T15:00:00Z

expiration_time 

date-time

required

Date and time in the ISO 8601 spec. Example: 2022-11-30T15:00:00Z

expiration_value 

string

required

The value that was considered for the settlement.

fee_waiver_expiration_time 

date-time

Date and time in the ISO 8601 spec. Example: 2022-11-30T15:00:00Z

floor_strike 

number

functional_strike 

string

Mapping from expiration values to settlement values of the YES/LONG side, in centi-cents.

Filled only if "market_type" is "scalar" and "strike_type" is "functional".

Ex. f(x) = max(0, min(10000, 500 * x))

A scalar market with this functional strike and an expiration value of 10 would have a settlement value on the YES/LONG side of 5000 centi cents.

last_price 

int64

required

Price for the last traded yes contract on this market.

latest_expiration_time 

date-time

required

Date and time in the ISO 8601 spec. Example: 2022-11-30T15:00:00Z

liquidity 

int64

required

Value for current offers in this market in cents.

market_type 

string

required

Identifies the type of market, which affects its payout and structure.

binary: Every binary market has two sides, YES and NO. If the market's "payout criterion" is satisfied, it pays out the notional value to holders of YES. Otherwise, it pays out the notional value to holders of NO.

scalar: Every scalar market has two sides, LONG and SHORT (although these might be referred to as YES/NO in some API endpoints). At settlement, each contract's notional value is split between LONG and SHORT as described in the market rules.

no_ask 

int64

required

Price for the lowest NO sell offer on this market.

no_bid 

int64

required

Price for the highest NO buy offer on this market.

no_sub_title 

string

required

Shortened title for the no side of this market.

notional_value 

int64

required

The total value of a single contract at settlement.

open_interest 

int64

required

Number of contracts bought on this market disconsidering netting.

open_time 

date-time

required

Date and time in the ISO 8601 spec. Example: 2022-11-30T15:00:00Z

previous_price 

int64

required

Price for the last traded yes contract on this market a day ago.

previous_yes_ask 

int64

required

Price for the lowest YES sell offer on this market a day ago.

previous_yes_bid 

int64

required

Price for the highest YES buy offer on this market a day ago.

response_price_units 

string

required

The units used to express all price related fields in this response, including: prices, bids/asks, liquidity, notional and settlement values.

 usd_cent MONEY_UNIT_USD_CENT

 usd_centi_cent MONEY_UNIT_USD_CENTI_CENT

`usd_cent``usd_centi_cent`

result 

string

required

Settlement result for this market. Filled only after determination. Omitted for scalar markets.

 MARKET_RESULT_NO_RESULT

 yes MARKET_RESULT_YES

 no MARKET_RESULT_NO

 void MARKET_RESULT_VOID

 scalar MARKET_RESULT_SCALAR

 all_no RANGED_MARKET_RESULT_ALL_NO

 all_yes RANGED_MARKET_RESULT_ALL_YES

`yes``no``void``scalar``all_no``all_yes`

risk_limit_cents 

int64

required

Deprecated: Risk limit for this market in cents.

rules_primary 

string

required

A plain language description of the most important market terms.

rules_secondary 

string

required

A plain language description of secondary market terms.

settlement_timer_seconds 

int32

required

The amount of time after determination that the market settles (pays out).

settlement_value 

int64

The settlement value of the YES/LONG side of the contract. Only filled after determination.

status 

string

required

Represents the current status of a market.

strike_type 

string

Strike type defines how the market strike (expiration value) is defined and evaluated.

greater: It will be a single number. For YES outcome the expiration value should be greater than "floor_strike".

greater_or_equal: It will be a single number. For YES outcome the expiration value should be greater OR EQUAL than "floor_strike".

less: It will be a single number. For YES outcome the expiration value should be less than "cap_strike".

less_or_equal: It will be a single number. For YES outcome the expiration value should be less OR EQUAL than "cap_strike".

between: It will be two numbers. For YES outcome the expiration value should be between inclusive "floor_strike" and "cap_strike", that means expiration value needs to be greater or equal "floor_strike" and less or equal "cap_strike".

functional: For scalar markets only. A mapping from expiration values to settlement values of the YES/LONG side will be in "functional_strike".

custom: It will be one or more non-numerical values. For YES outcome the expiration values should be equal to the values in "custom_strike".

structured: A key value map from relationship -> structured target IDs. Metadata for these structured targets can be fetched via the /structured_targets endpoints.

 unknown MarketStrikeTypeUnknown

 greater MarketStrikeTypeGreater

 less MarketStrikeTypeLess

 greater_or_equal MarketStrikeTypeGreaterOrEqual

 less_or_equal MarketStrikeTypeLessOrEqual

 between MarketStrikeTypeBetween

 functional MarketStrikeTypeFunctional

 custom MarketStrikeTypeCustom

 structured MarketStrikeTypeStructured

`unknown``greater``less``greater_or_equal``less_or_equal``between``functional``custom``structured`

subtitle 

string

required

Deprecated: Shortened title for this market. Use "yes_sub_title" or "no_sub_title" instead.

tick_size 

int64

required

The minimum price movement in the market. All limit order prices must be in denominations of the tick size.

ticker 

string

required

Unique identifier for markets.

title 

string

Full title describing this market.

volume 

int64

required

Number of contracts bought on this market.

volume_24h 

int64

required

Number of contracts bought on this market in the past day.

yes_ask 

int64

required

Price for the lowest YES sell offer on this market.

yes_bid 

int64

required

Price for the highest YES buy offer on this market.

yes_sub_title 

string

required

Shortened title for the yes side of this market.

Updated 5 months ago