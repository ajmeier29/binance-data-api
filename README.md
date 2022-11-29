# binance-data-api
Simple REST microservice to get pnl data from my account. To be hosted in Azure. 

## Azure Configuration Environment
- BINANCEKEY - The api key
- BINANCESECRET - The api secret
- SCM_DO_BUILD_DURING_DEPLOYMENT - Auto set by setting up github CI/CD, and is default to 1. 

## Dependancies
- flask
- pandas
- binance-futures-connector
- babel
- flask-jsonpify
