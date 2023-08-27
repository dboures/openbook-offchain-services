# openbook-offchain-services

OpenBook Offchain Services is an open source trade scraper and candle batcher combined with web API for OpenBook frontends. The web API is largely based off of the code [here](https://github.com/Bonfida/agnostic-candles) and a previous version [here](https://github.com/dboures/openbook-candles).

[Scraper](#scraper)

[Worker](#worker)

[Server](#server)

<a  name="configuration"></a>

<h2  align="center">Configuration</h2>

<br  />

> ⚠️ This repo requires that Postgres be used as the database to store trades and candles.

<br  />

<a  name="scraper"></a>

<h2  align="center">Scraper</h2>

<br  />

The scraper directory contains the program that scrapes OpenBook trades and new markets. By default, no trades will be saved to the database unless the scraper_active flag is set to true. Since certain RPC calls have high latency, scraping is done in 2 parts:

1. Using [getConfirmedSignaturesForAddress2](https://docs.solana.com/api/http#getconfirmedsignaturesforaddress2) to obtain transaction signatures.
2. Multiple workers call getTransaction and parse the fills and market creation events.

To run the scraper locally:

```

cargo run --bin scraper

```

<br  />

<br  />

<a  name="worker"></a>

<h2  align="center">Worker</h2>

<br  />

The worker directory contains the program that scrapes OpenBook trades and stores them. The worker is also responsible for batching the trades into OHLCV candles.

To run the worker locally:

```

cargo run --bin worker

```

<br  />

Each market will automatically batch 1,3,5,15,30 minute, 1,2,4 hour, and 1 day candles from the scraped trades.

<br  />

<a  name="server"></a>

<h2  align="center">Server</h2>

<br  />

The server uses [actix web](https://actix.rs/) and is served by default on port `8080` .

To run the server locally:

```

cargo run --bin server

```

The server supports the following endpoints:

### Markets

**Request:**

`GET api/markets`

Show all markets available via the API


### Candles

**Request:**

`GET /api/candles?market_name={market_name}&from={from}&to={to}&resolution={resolution}`

Returns historical candles

**Response:**

```json

{

"s": "ok",

"time": [1651189320, 1651189380],

"close": [1.2090027797967196, 1.2083083698526025],

"open": [1.2090027797967196, 1.208549999864772],

"high": [1.2090027797967196, 1.208549999864772],

"low": [1.2090027797967196, 1.208055029856041],

"volume": [0, 0]

}

```

Note that if `market_name` contains a forward slash, it will need to be delimited.

For example: `GET /api/candles?market_name=SOL%2FUSDC&from=1678425243&to=1678725243&resolution=1M`

### Traders (By Base Token Volume)

**Request:**

`GET /api/traders/base-volume?market_name={market_name}&from={from}&to={to}`

Returns the top traders sorted by base token volume (limited to 10,000)


### Traders (By Quote Token Volume)

**Request:**

`GET /api/traders/quote-volume?market_name={market_name}&from={from}&to={to}`

Returns the top traders sorted by quote token volume (limited to 10,000)


# CoinGecko APIs

### Pairs

**Request:**

`GET /api/coingecko/pairs`

Returns a summary on the trading pairs available on OpenBook.

### Tickers

**Request:**

`GET /api/coingecko/tickers`

Returns 24-hour pricing and volume information on each market available.

### OrderBook

**Request:**

`GET /api/coingecko/orderbook?ticker_id={ticker_id}&depth={depth}`

Returns order book information with a specified depth for a given market.

