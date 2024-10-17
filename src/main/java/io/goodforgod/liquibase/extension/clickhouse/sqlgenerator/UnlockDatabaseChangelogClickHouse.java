package io.goodforgod.liquibase.extension.clickhouse.sqlgenerator;

import io.goodforgod.liquibase.extension.clickhouse.database.ClickHouseDatabase;
import io.goodforgod.liquibase.extension.clickhouse.params.ClusterConfig;
import io.goodforgod.liquibase.extension.clickhouse.params.ParamsLoader;
import liquibase.database.Database;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.UnlockDatabaseChangeLogGenerator;
import liquibase.statement.core.UnlockDatabaseChangeLogStatement;

public class UnlockDatabaseChangelogClickHouse extends UnlockDatabaseChangeLogGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(UnlockDatabaseChangeLogStatement statement, Database database) {
        return database instanceof ClickHouseDatabase;
    }

    @Override
    public Sql[] generateSql(
                             UnlockDatabaseChangeLogStatement statement,
                             Database database,
                             SqlGeneratorChain sqlGeneratorChain) {
        ClusterConfig properties = ParamsLoader.getLiquibaseClickhouseProperties();

        String unlockQuery = String.format(
                "ALTER TABLE `%s`.%s "
                        + SqlGeneratorUtil.generateSqlOnClusterClause(properties)
                        + "UPDATE LOCKED = 0,LOCKEDBY = null, LOCKGRANTED = null WHERE ID = 1 AND LOCKED = 1 SETTINGS mutations_sync = 1",
                database.getDefaultSchemaName(),
                database.getDatabaseChangeLogLockTableName());

        return SqlGeneratorUtil.generateSql(database, unlockQuery);
    }
}
