workflowmonit
=============

Predict running workflows' actions. Currently running at vocms0116.
> Project under CMS CompOps T&I

## how to run
After environments set up and necessary configuration pieces added,
```bash
./startMonit.sh # this starts monitoring in the background
cd web/
./quickStart.sh # this starts Flask basic server on port 8020 (subject to change)
```

## configurations etc.
A few more configuration files are needed to get it rolling.
1. `config/config.yml` for connections to UNIFIED DB, and alert email sending.
```yml
oracle:
  - ***
  - ***
  - ***

alert_recipients:
  - XXX@YYYY.ZZ
```

2. `config/credential.yml` for `stompAMQ` to produce docs and authentication.
```yml
producer: toolsandint-workflows-collector
topic: /topic/cms.toolsandint.workflowsinfo
cert: PATH_TO_CERT_FILE (***.pem)
key: PATH_TO_KEY_FILE (***.ras)
hostport:
  host: XXXX.cern.ch
  port: XXXXX
```

3. `models/xgb_optimized.model` for running workflow inference.

---
### data source
- UNIFIED DB
- wmstats server
- couchdb/acdc server

### data storage
- `stompAMQ` -> HDFS /CERN MONIT infrastructure

### model training
- Spark (data fetch)
- SWAN
- XGBoost

### web SPA
- Flask
- dataTable.js
- plotly.js
