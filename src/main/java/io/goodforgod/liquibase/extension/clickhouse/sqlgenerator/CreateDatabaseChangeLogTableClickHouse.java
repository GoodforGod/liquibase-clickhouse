package io.goodforgod.liquibase.extension.clickhouse.sqlgenerator;

import io.goodforgod.liquibase.extension.clickhouse.database.ClickHouseDatabase;
import io.goodforgod.liquibase.extension.clickhouse.params.ClusterConfig;
import io.goodforgod.liquibase.extension.clickhouse.params.ParamsLoader;
import java.util.Locale;
import liquibase.database.Database;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.CreateDatabaseChangeLogTableGenerator;
import liquibase.statement.core.CreateDatabaseChangeLogTableStatement;

public class CreateDatabaseChangeLogTableClickHouse extends CreateDatabaseChangeLogTableGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(CreateDatabaseChangeLogTableStatement statement, Database database) {
        return database instanceof ClickHouseDatabase;
    }

    @Override
    public Sql[] generateSql(
                             CreateDatabaseChangeLogTableStatement statement,
                             Database database,
                             SqlGeneratorChain sqlGeneratorChain) {
        ClusterConfig properties = ParamsLoader.getLiquibaseClickhouseProperties();
        String tableName = database.getDatabaseChangeLogTableName();

        String createTableQuery = String.format(
                "CREATE TABLE IF NOT EXISTS `%s`.%s "
                        + SqlGeneratorUtil.generateSqlOnClusterClause(properties)
                        + "("
                        + "ID String,"
                        + "AUTHOR String,"
                        + "FILENAME String,"
                        + "DATEEXECUTED DateTime64,"
                        + "ORDEREXECUTED UInt64,"
                        + "EXECTYPE String,"
                        + "MD5SUM Nullable(String),"
                        + "DESCRIPTION Nullable(String),"
                        + "COMMENTS Nullable(String),"
                        + "TAG Nullable(String),"
                        + "LIQUIBASE Nullable(String),"
                        + "CONTEXTS Nullable(String),"
                        + "LABELS Nullable(String),"
                        + "DEPLOYMENT_ID Nullable(String)) "
                        + SqlGeneratorUtil.generateSqlEngineClause(
                                properties, tableName.toLowerCase(Locale.ROOT)),
                database.getDefaultSchemaName(),
                tableName);

        return SqlGeneratorUtil.generateSql(database, createTableQuery);
    }
}
