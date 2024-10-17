package io.goodforgod.liquibase.extension.clickhouse.sqlgenerator;

import io.goodforgod.liquibase.extension.clickhouse.database.ClickHouseDatabase;
import io.goodforgod.liquibase.extension.clickhouse.params.ClusterConfig;
import io.goodforgod.liquibase.extension.clickhouse.params.ParamsLoader;
import liquibase.database.Database;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.LockDatabaseChangeLogGenerator;
import liquibase.statement.core.LockDatabaseChangeLogStatement;

public class LockDatabaseChangeLogClickHouse extends LockDatabaseChangeLogGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(LockDatabaseChangeLogStatement statement, Database database) {
        return database instanceof ClickHouseDatabase;
    }

    @Override
    public Sql[] generateSql(
                             LockDatabaseChangeLogStatement statement,
                             Database database,
                             SqlGeneratorChain sqlGeneratorChain) {
        ClusterConfig properties = ParamsLoader.getLiquibaseClickhouseProperties();

        String host = String.format("%s %s (%s)", hostname, hostDescription, hostaddress);
        String lockQuery = String.format(
                "ALTER TABLE `%s`.%s "
                        + SqlGeneratorUtil.generateSqlOnClusterClause(properties)
                        + "UPDATE LOCKED = 1,LOCKEDBY = '%s',LOCKGRANTED = %s WHERE ID = 1 AND LOCKED = 0 SETTINGS mutations_sync = 1",
                database.getDefaultSchemaName(),
                database.getDatabaseChangeLogLockTableName(),
                host,
                ClickHouseDatabase.CURRENT_DATE_TIME_FUNCTION);

        return SqlGeneratorUtil.generateSql(database, lockQuery);
    }
}
