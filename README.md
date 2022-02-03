# binance-trading-bot
A simple trading robot for a binance exchange.
This is a work in progress, for now my bot can:
- collect historical data from the exchange. This is done through historical data module. Collected data is stored in a MySQL database via mysql connector python. Data is downloaded asynchronously using async.io.
- run event-driven backtests. It has en event loop, based on this article: https://www.fmz.com/bbs-topic/3600.
