## Data remover

* [POST]source: HBase -> destination: Cassandra **/datacopy/hbase2cassandra**
* [POST]source: HBase -> destination: Hbase **/datacopy/hbase2hbase**
* [POST]source: Cassandra -> destination: Cassandra **/datacopy/cassandra2cassandra**
* [POST]source: Cassandra -> destination: HBase **/datacopy/cassandra2hbase**

* [GET]check removing status: **/datacopy/status**
* [GET]check database TTL setting(default: 2592000s): **/datacopy/database/ttl**
* [POST]change database TTL setting: **/datacopy/database/ttl**

> NOTE: If your data wasn't set TTL in original DB, make sure set **TTL=0** before execute data remove. 


> Trigger these functions with a HTTP **POST** request. Request body example as below. Check more info after request with **/datacopy/status**. 

## PostBody

```json
  {
    "source": {
      "hbaseZKQuorum": "192.168.0.125",
      "hbaseZKPropertyClientPort": "2181",
      "hbaseTable": "demo:demo",
      "hbaseColumnFamily": "demo"
    },
    "destination": {
      "cassandraHosts": "192.168.0.126",
      "cassandraClusterName": "hello",
      "cassandraKeyspace": "world",
      "cassandraColumnFamily": "hi"
    }
  }
```
