# Connector catalog ingest report

Spec SHA: `03bcdd8`
Capability SHA: `0bbc5d2c`

> The capability face comes from an earlier upstream revision than the spec face: modes, sink and write semantics were derived at `0bbc5d2c`, while the structure below was read at `03bcdd8`. A full refresh brings them back together.
Ingested connectors: 78

## Unclassified — no resolvable mode (need tapstate.modes)
- ai-chat
- bes-channels
- bigquery
- databend
- elasticsearch
- lark-im
- lark-task
- risingwave
- tablestore
- vika

## Not derived — no built jar or did not classload (excluded from refresh)
- hazelcast

## MQ suspects — derived cdc, undeclared (need tapstate.modes)
- kafka_avro

## Sink semantics defaulted — no DML signal
- activemq
- bigquery
- csv
- custom
- doris
- dummy
- file-stream
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
