# Connector catalog ingest report

Spec SHA: `f3bd43a`
Capability SHA: `0bbc5d2c`

> The capability face comes from an earlier upstream revision than the spec face: modes, sink and write semantics were derived at `0bbc5d2c`, while the structure below was read at `f3bd43a`. A full refresh brings them back together.
Ingested connectors: 78

## Unclassified — no resolvable mode (need tapstate.modes)
- ai-chat
- bes-channels
- bigquery
- databend
- elasticsearch
- hazelcast
- lark-im
- lark-task
- risingwave
- tablestore
- vika

## Not built — this repository cannot build these, by name and with reason
- aws-clickhouse: upstream module compiles against clickhouse classes its dependencies do not carry
- dws: upstream module compiles against postgres-core, which nothing it depends on carries
- file-stream: upstream module does not compile against the current file connector base
- greenplum: upstream module compiles against postgres-core, which nothing it depends on carries
- highgo: driver published only to the upstream project's private repository
- huawei-gauss-db: driver published only to the upstream project's private repository
- mongodb3: upstream module does not compile against the current mongodb connector
- vastbase: upstream module compiles against postgres-core, which nothing it depends on carries
- yashandb: driver published only to the upstream project's private repository

## Not derived — no built jar or did not classload (excluded from refresh)
(none)

## Unverified modes — derived for a non-database connector nobody declared
- coding
- csv
- excel
- http-receiver
- json
- kafka_avro
- quickapi
- xml
- zoho-desk

## Overlay carrying it alone — upstream declares nothing, the mode is ours only
- aws-clickhouse: upstream declares nothing, ours [snapshot]
- dws: upstream declares nothing, ours [snapshot]
- file-stream: upstream declares nothing, ours [cdc, snapshot]
- greenplum: upstream declares nothing, ours [snapshot]
- highgo: upstream declares nothing, ours [cdc, snapshot]
- huawei-gauss-db: upstream declares nothing, ours [cdc, snapshot]
- mongodb3: upstream declares nothing, ours [cdc, snapshot]
- vastbase: upstream declares nothing, ours [cdc, snapshot]

## Overlay divergences — our declaration differs from the connector's own
(none)

## Overlay not derivable — we declare a mode the capabilities do not support
- aws-clickhouse: snapshot needs batch_read_function
- dws: snapshot needs batch_read_function
- file-stream: cdc needs stream_read_function
- file-stream: snapshot needs batch_read_function
- greenplum: snapshot needs batch_read_function
- highgo: cdc needs stream_read_function
- highgo: snapshot needs batch_read_function
- huawei-gauss-db: cdc needs stream_read_function
- huawei-gauss-db: snapshot needs batch_read_function
- mongodb3: cdc needs stream_read_function
- mongodb3: snapshot needs batch_read_function
- selectdb: snapshot needs batch_read_function
- vastbase: cdc needs stream_read_function
- vastbase: snapshot needs batch_read_function
- yashandb: snapshot needs batch_read_function

## Sink semantics defaulted — no DML signal
- activemq
- bigquery
- csv
- custom
- doris
- dummy
- hbase
- kafka
- kafka_avro
- kafka_enhanced
- rabbitmq
- redis
- rocketmq
- tablestore
- vika

## Unrecognized type tokens — fell to string input
(none)

## Unresolved label refs — fell back to raw key
- aliyun-adb-mysql:addtionalString
- aliyun-rds-mysql:addtionalString
- aws-rds-mysql:addtionalString
- mysql-pxc:addtionalString
- polar-db-mysql:addtionalString
- tencent-db-mariadb:addtionalString

## Exemptions — modules and specs set aside
- [EXCLUDED] coding-demo-connector: known non-connector module
- [EXCLUDED] demo-connector: known non-connector module
- [EXCLUDED] js-core: known non-connector module
- [EXCLUDED] mock-source-connector: known non-connector module
- [EXCLUDED] mock-target-connector: known non-connector module
- [EXCLUDED] tdd-connector: known non-connector module
- [MULTI_SPEC] bigquery-connector: spec.json
- [MULTI_SPEC] coding-connector: spec.json
- [MULTI_SPEC] lark-doc-connector: spec-oauth.json
- [NO_CANONICAL_SPEC] connector-perf-test: no @TapConnectorClass and no spec.json
