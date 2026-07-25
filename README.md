# Liquibase Clickhouse

[![Minimum required Java version](https://img.shields.io/badge/Java-11%2B-blue?logo=openjdk)](https://openjdk.org/projects/jdk/11/)
[![Maven Central](https://img.shields.io/maven-central/v/io.goodforgod/liquibase-clickhouse.svg)](https://central.sonatype.com/artifact/io.goodforgod/liquibase-clickhouse)
[![GitHub Action](https://github.com/goodforgod/liquibase-clickhouse/workflows/CI%20Master/badge.svg)](https://github.com/GoodforGod/liquibase-clickhouse/actions?query=workflow%3A"CI+Master"++)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=GoodforGod_liquibase-clickhouse&metric=coverage)](https://sonarcloud.io/dashboard?id=GoodforGod_liquibase-clickhouse)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=GoodforGod_liquibase-clickhouse&metric=sqale_rating)](https://sonarcloud.io/dashboard?id=GoodforGod_liquibase-clickhouse)
[![Lines of Code](https://sonarcloud.io/api/project_badges/measure?project=GoodforGod_liquibase-clickhouse&metric=ncloc)](https://sonarcloud.io/dashboard?id=GoodforGod_liquibase-clickhouse)

Liquibase ClickHouse module for migration.

Supported operations: 
- `update`
- `rollback` (with provided SQL script)
- `tag`

## Dependency :rocket:

[**Gradle**](https://mvnrepository.com/artifact/io.goodforgod/liquibase-clickhouse)
```groovy
implementation "io.goodforgod:liquibase-clickhouse:0.9.1"
```

[**Maven**](https://mvnrepository.com/artifact/io.goodforgod/liquibase-clickhouse)
```xml
<dependency>
    <groupId>io.goodforgod</groupId>
    <artifactId>liquibase-clickhouse</artifactId>
    <version>0.9.1</version>
</dependency>
```

### Compatibility

- Version 0.9.1+ - Liquibase [4.33.0+](https://mvnrepository.com/artifact/org.liquibase/liquibase-core) and [clickhouse driver 0.9.2+](https://github.com/ClickHouse/clickhouse-java/releases/tag/v0.9.2)
- Version 0.8.0+ - Liquibase [4.29.0+](https://mvnrepository.com/artifact/org.liquibase/liquibase-core) and [clickhouse driver 0.7.0-0.7.2](https://github.com/ClickHouse/clickhouse-java/releases/tag/v0.7.2)

## Cluster

The cluster mode can be activated by adding the `liquibaseClickhouse.properties` file to the classpath (liquibase/lib/).

Property file path can be specified also via:
- System property - `liquibaseClickhousePropertiesFile`
- Environment variable - `LIQUIBASE_CLICKHOUSE_PROPERTIES_FILE`

Configuration values are resolved in the following order:
- Environment variable
- Property from `liquibaseClickhouse.properties`
- Default value

Properties file format:
```properties
# these are our cluster config values
clickhouse.cluster.clusterName=Cluster1
clickhouse.cluster.tableZooKeeperPathPrefix=Path1
clickhouse.cluster.tableReplicaName=Replica1
clickhouse.mutationsSyncAcquire=2
clickhouse.mutationsSyncRelease=1
clickhouse.mutationsSyncInit=1
```

You can also specify values in file via environment variables with default values:
```properties
# these are our cluster config values
clickhouse.cluster.clusterName=${CLICKHOUSE_CLUSTER}
clickhouse.cluster.tableZooKeeperPathPrefix=Path1
clickhouse.cluster.tableReplicaName=${CLICKHOUSE_REPLICA_TABLE|defaultReplaceTableName}
clickhouse.mutationsSyncAcquire=${CLICKHOUSE_MUTATIONS_SYNC_ACQUIRE|2}
clickhouse.mutationsSyncRelease=${CLICKHOUSE_MUTATIONS_SYNC_RELEASE|1}
clickhouse.mutationsSyncInit=${CLICKHOUSE_MUTATIONS_SYNC_INIT|1}
```

All configuration values can also be passed directly via environment variables:
- `LIQUIBASE_CLICKHOUSE_CLUSTER_NAME`
- `LIQUIBASE_CLICKHOUSE_CLUSTER_TABLE_ZOOKEEPER_PATH_PREFIX`
- `LIQUIBASE_CLICKHOUSE_CLUSTER_TABLE_REPLICA_NAME`
- `LIQUIBASE_CLICKHOUSE_MUTATIONS_SYNC_ACQUIRE`
- `LIQUIBASE_CLICKHOUSE_MUTATIONS_SYNC_RELEASE`
- `LIQUIBASE_CLICKHOUSE_MUTATIONS_SYNC_INIT`

In this mode, liquibase will create its own tables as replicated.
All changes in these files will be replicated on the entire cluster.
Your updates should also affect the entire cluster either by using ON CLUSTER clause, or by using replicated tables.

## Lock mutations sync

The `mutations_sync` setting applied to lock table mutations can be overridden via:
- Environment variables - `LIQUIBASE_CLICKHOUSE_MUTATIONS_SYNC_ACQUIRE`,
  `LIQUIBASE_CLICKHOUSE_MUTATIONS_SYNC_RELEASE`, `LIQUIBASE_CLICKHOUSE_MUTATIONS_SYNC_INIT`
- Properties - `clickhouse.mutationsSyncAcquire`, `clickhouse.mutationsSyncRelease`,
  `clickhouse.mutationsSyncInit`

Defaults:
- `mutationsSyncAcquire=2` - lock acquire waits for mutations on all replicas.
- `mutationsSyncRelease=1` - lock release waits for mutations on the current server only.
- `mutationsSyncInit=1` - lock table init waits for mutations on the current server only.

Accepted values (see the ClickHouse `mutations_sync` setting):
- `0` - mutation executes asynchronously, Liquibase does not wait for it to complete.
- `1` - Liquibase waits for the mutation to complete on the current server only.
- `2` - Liquibase waits for the mutation to complete on all replicas (default).

WARNING: `0` makes the Liquibase lock mutation asynchronous. Liquibase can continue before the
`DATABASECHANGELOGLOCK` row is visibly locked, so concurrent Liquibase executions must be prevented externally.

Any configured invalid value fails startup.

For example, set it in `liquibaseClickhouse.properties`:
```properties
clickhouse.mutationsSyncAcquire=1
clickhouse.mutationsSyncRelease=1
clickhouse.mutationsSyncInit=1
```

## License

Based on [MEDIARITHMICS/liquibase-clickhouse](https://github.com/mediarithmics/liquibase-clickhouse) 

This project licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
