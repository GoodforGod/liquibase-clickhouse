package io.goodforgod.liquibase.extension.clickhouse.sqlgenerator;

import io.goodforgod.liquibase.extension.clickhouse.database.ClickHouseDatabase;
import io.goodforgod.liquibase.extension.clickhouse.params.ClusterConfig;
import io.goodforgod.liquibase.extension.clickhouse.params.ParamsLoader;
import java.util.*;
import liquibase.database.Database;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.CreateDatabaseChangeLogLockTableGenerator;
import liquibase.statement.core.CreateDatabaseChangeLogLockTableStatement;

public class CreateDatabaseChangeLogLockTableClickHouse
        extends
        CreateDatabaseChangeLogLockTableGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(CreateDatabaseChangeLogLockTableStatement statement, Database database) {
        return database instanceof ClickHouseDatabase;
    }

    @Override
    public Sql[] generateSql(
                             CreateDatabaseChangeLogLockTableStatement statement,
                             Database database,
                             SqlGeneratorChain sqlGeneratorChain) {
        ClusterConfig properties = ParamsLoader.getLiquibaseClickhouseProperties();
        String tableName = database.getDatabaseChangeLogLockTableName();

        String createTableQuery = String.format(
                "CREATE TABLE IF NOT EXISTS `%s`.%s "
                        + SqlGeneratorUtil.generateSqlOnClusterClause(properties)
                        + "("
                        + "ID Int64,"
                        + "LOCKED UInt8,"
                        + "LOCKGRANTED Nullable(DateTime64),"
                        + "LOCKEDBY Nullable(String)) "
                        + SqlGeneratorUtil.generateSqlEngineClause(
                                properties, tableName.toLowerCase(Locale.ROOT)),
                database.getDefaultSchemaName(),
                tableName);

        return SqlGeneratorUtil.generateSql(database, createTableQuery);
    }
}
