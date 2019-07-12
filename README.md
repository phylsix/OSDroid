OSDroid
=============

Predict running workflows' actions. Currently running at vocms0116.
> Project under CMS CompOps T&I

## How to run
After environments set up and necessary configuration pieces added,
```bash
./startMonit.sh # this starts monitoring in the background
cd web/
./quickStart.sh # this starts Flask basic server on port 8020 (subject to change)
```

## Configurations etc.
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
    key: PATH_TO_KEY_FILE (***.rsa)
    hostport:
      host: XXXX.cern.ch
      port: XXXXX
    ```

3. `models/xgb_optimized.model` for running workflow inference.

---

### Data source
- UNIFIED DB
- wmstats server
- couchdb/acdc server

### Data storage
- [`stompAMQ`](https://github.com/jasonrbriggs/stomp.py) (wrapped in [`CMSMonitoring`](https://github.com/dmwm/CMSMonitoring)) -> HDFS /CERN MONIT infrastructure

### Model training
- [pyspark](https://github.com/apache/spark/tree/master/python) (data fetch)
- [SWAN](https://swan.cern.ch/)
- [XGBoost](https://github.com/dmlc/xgboost)

### Web SPA
- [Flask](https://github.com/pallets/flask)
- [dataTable.js](https://github.com/DataTables/DataTables)
- [plotly.js](https://github.com/plotly/plotly.js/)
