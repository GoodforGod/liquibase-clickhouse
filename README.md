# Liquibase Clickhouse

[![Minimum required Java version](https://img.shields.io/badge/Java-11%2B-blue?logo=openjdk)](https://openjdk.org/projects/jdk/11/)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.goodforgod/liquibase-clickhouse/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.goodforgod/liquibase-clickhouse)
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
implementation "io.goodforgod:liquibase-clickhouse:0.8.0"
```

[**Maven**](https://mvnrepository.com/artifact/io.goodforgod/liquibase-clickhouse)
```xml
<dependency>
    <groupId>io.goodforgod</groupId>
    <artifactId>liquibase-clickhouse</artifactId>
    <version>0.8.0</version>
</dependency>
```

## Liquibase

Supported Liquibase version [4.29.0+](https://mvnrepository.com/artifact/org.liquibase/liquibase-core)

## Driver

Supported driver version [0.7.0+](https://mvnrepository.com/artifact/com.clickhouse/clickhouse-jdbc)

```groovy
implementation "com.clickhouse:clickhouse-jdbc:0.7.0"
implementation("com.clickhouse:clickhouse-http-client") { version { strictly "0.7.0" } }
```

## Cluster

The cluster mode can be activated by adding the `liquibaseClickhouse.conf` file to the classpath (liquibase/lib/).

```hocon
cluster {
    clusterName="{cluster}"
    tableZooKeeperPathPrefix="/clickhouse/tables/{shard}/{database}/"
    tableReplicaName="{replica}"
}
```

In this mode, liquibase will create its own tables as replicated.
All changes in these files will be replicated on the entire cluster.
Your updates should also affect the entire cluster either by using ON CLUSTER clause, or by using replicated tables.

## License

Based on [MEDIARITHMICS/liquibase-clickhouse](https://github.com/mediarithmics/liquibase-clickhouse) 

This project licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
