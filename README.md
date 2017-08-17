# Alternative scheduler
Scheduler that when triggered runs a project until it stabilizes, then stops. 

[![Build Status](https://travis-ci.org/sesam-community/scheduler.svg?branch=master)](https://travis-ci.org/sesam-community/scheduler)

## API

GET / returns scheduler state (se example-state.json)
```
{
  "state" : "init|running|failed|success"
}
```
GET /text returns an ASCII text that can be displayed to show progress:

```
INPUT PIPES                                            LAST  QUEUE   TAIL  INDEX    LOG  LOOPINESS
  cab-address                                            10             0     10     10       1.00
  cab-annualload_latestmeterreading                      11             0     11     11       1.00
  cab-businesssector                                     10             0     10     10       1.00
  cab-companycustomer                                    10             0     10     10       1.00
  cab-consumptioncode                                    10             0     10     10       1.00
OUTPUT PIPES                                           LAST  QUEUE   TAIL  INDEX    LOG  LOOPINESS
  anlegg-azure-csv                                                      0                     1.00
  anlegg-azure-sql                                        0     10      0                     1.00
  annualload-geonis-endpoint                              0     10      0                     1.00
  annualload-newinstallation-quant-endpoint               0      0      0                     1.00
  asset-solr                                                   178      0                     1.00
INTERNAL PIPES                                         LAST  QUEUE   TAIL  INDEX    LOG  LOOPINESS
  anlegg-azure                                            0     70      0     10     10       1.00
  anlegg-gridanalysis                                    10     70      0     10     10       1.00
  annualload-geonis                                       0     70      0     10     10       1.00
  annualload-newinstallation-quant                        0     44      0      0      0       1.00
  cab-companycustomer-sub                                 0      0      0     10     10       1.00
  cab-contract-hist                                       3     10      0      3      3       1.00
  cab-custaddress-sub                                     0      0      0     10     10       1.00
  cab-mehistory-sub                                       0      0      0     10     10       1.00
  cab-powsuppperiod-sub                                   0      0      0     10     10       1.00
  cab-setlpnt-deliveryprofile                            10     10      0     10     10       1.00
  cab-settlementpoint-with-ordersgroup                    0      0      0     10     10       1.00
  cab-view-cab-phonenumber-sub                            0      0      0     10     10       1.00
  contract-qlik                                           0             0      0      0       1.00
   - merges cab-contract-hist                                    2
  customer-azure                                          7     30      0      7      7       1.00
```


POST /start starts the scheduler when not in state ``running``.
