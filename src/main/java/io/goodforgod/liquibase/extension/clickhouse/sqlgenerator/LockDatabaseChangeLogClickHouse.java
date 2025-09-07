package io.goodforgod.liquibase.extension.clickhouse.sqlgenerator;

import io.goodforgod.liquibase.extension.clickhouse.database.ClickHouseDatabase;
import io.goodforgod.liquibase.extension.clickhouse.params.ClusterConfig;
import io.goodforgod.liquibase.extension.clickhouse.params.ParamsLoader;
import io.goodforgod.liquibase.extension.clickhouse.statement.ClickhouseLockDatabaseChangeLogStatement;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;

public class LockDatabaseChangeLogClickHouse extends AbstractSqlGenerator<ClickhouseLockDatabaseChangeLogStatement> {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(ClickhouseLockDatabaseChangeLogStatement statement, Database database) {
        return database instanceof ClickHouseDatabase;
    }

    @Override
    public ValidationErrors
            validate(ClickhouseLockDatabaseChangeLogStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        return new ValidationErrors();
    }

    @Override
    public Sql[] generateSql(ClickhouseLockDatabaseChangeLogStatement statement,
                             Database database,
                             SqlGeneratorChain sqlGeneratorChain) {
        ClusterConfig properties = ParamsLoader.getLiquibaseClickhouseProperties();

        String lockQuery = String.format("ALTER TABLE `%s`.%s %s\n" +
                "UPDATE LOCKED = 1, \n" +
                "       LOCKEDBY = '%s', \n" +
                "       LOCKGRANTED = %s \n" +
                "WHERE ID = 1 AND LOCKED = 0 \n" +
                "SETTINGS mutations_sync = 2",
                database.getDefaultSchemaName(),
                database.getDatabaseChangeLogLockTableName(),
                SqlGeneratorUtil.generateSqlOnClusterClause(properties),
                statement.getHost(),
                ClickHouseDatabase.CURRENT_DATE_TIME_FUNCTION);

        return SqlGeneratorUtil.generateSql(database, lockQuery);
    }
}
