package io.goodforgod.liquibase.extension.clickhouse.sqlgenerator;

import io.goodforgod.liquibase.extension.clickhouse.database.ClickHouseDatabase;
import io.goodforgod.liquibase.extension.clickhouse.params.ClusterConfig;
import io.goodforgod.liquibase.extension.clickhouse.params.ParamsLoader;
import liquibase.database.Database;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.InitializeDatabaseChangeLogLockTableGenerator;
import liquibase.statement.core.InitializeDatabaseChangeLogLockTableStatement;

public class InitializeDatabaseChangeLogLockClickHouse
        extends
        InitializeDatabaseChangeLogLockTableGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(
                            InitializeDatabaseChangeLogLockTableStatement statement,
                            Database database) {
        return database instanceof ClickHouseDatabase;
    }

    @Override
    public Sql[] generateSql(
                             InitializeDatabaseChangeLogLockTableStatement statement,
                             Database database,
                             SqlGeneratorChain sqlGeneratorChain) {
        ClusterConfig properties = ParamsLoader.getLiquibaseClickhouseProperties();

        String clearDatabaseQuery = String.format(
                "ALTER TABLE `%s`.%s "
                        + SqlGeneratorUtil.generateSqlOnClusterClause(properties)
                        + "DELETE WHERE 1 SETTINGS mutations_sync = 1",
                database.getDefaultSchemaName(),
                database.getDatabaseChangeLogLockTableName());

        String initLockQuery = String.format(
                "INSERT INTO `%s`.%s (ID, LOCKED) VALUES (1, 0)",
                database.getDefaultSchemaName(), database.getDatabaseChangeLogLockTableName());

        return SqlGeneratorUtil.generateSql(database, clearDatabaseQuery, initLockQuery);
    }
}
