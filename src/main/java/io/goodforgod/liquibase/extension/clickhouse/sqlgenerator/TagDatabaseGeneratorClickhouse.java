package io.goodforgod.liquibase.extension.clickhouse.sqlgenerator;

import io.goodforgod.liquibase.extension.clickhouse.database.ClickHouseDatabase;
import io.goodforgod.liquibase.extension.clickhouse.params.ClusterConfig;
import io.goodforgod.liquibase.extension.clickhouse.params.ParamsLoader;
import liquibase.database.Database;
import liquibase.datatype.DataTypeFactory;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.TagDatabaseGenerator;
import liquibase.statement.core.TagDatabaseStatement;
import liquibase.structure.core.Column;

public class TagDatabaseGeneratorClickhouse extends TagDatabaseGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(TagDatabaseStatement statement, Database database) {
        return database instanceof ClickHouseDatabase;
    }

    @Override
    public Sql[] generateSql(TagDatabaseStatement statement,
                             Database database,
                             SqlGeneratorChain sqlGeneratorChain) {
        String tableNameEscaped = database.escapeTableName(
                database.getLiquibaseCatalogName(),
                database.getLiquibaseSchemaName(),
                database.getDatabaseChangeLogTableName());
        ClusterConfig properties = ParamsLoader.getLiquibaseClickhouseProperties();
        String orderColumnNameEscaped = database.escapeObjectName("ORDEREXECUTED", Column.class);
        String dateColumnNameEscaped = database.escapeObjectName("DATEEXECUTED", Column.class);
        String tagEscaped = DataTypeFactory.getInstance()
                .fromObject(statement.getTag(), database)
                .objectToSql(statement.getTag(), database);

        return new Sql[] {
                new UnparsedSql(
                        "ALTER TABLE "
                                + tableNameEscaped
                                + SqlGeneratorUtil.generateSqlOnClusterClause(properties)
                                + " UPDATE TAG="
                                + tagEscaped
                                + " WHERE "
                                + dateColumnNameEscaped
                                + "=(SELECT "
                                + dateColumnNameEscaped
                                + " FROM "
                                + tableNameEscaped
                                + " ORDER BY "
                                + dateColumnNameEscaped
                                + " DESC, "
                                + orderColumnNameEscaped
                                + " DESC LIMIT 1)"
                                + " AND "
                                + orderColumnNameEscaped
                                + "=(SELECT "
                                + orderColumnNameEscaped
                                + " FROM "
                                + tableNameEscaped
                                + " ORDER BY "
                                + dateColumnNameEscaped
                                + " DESC, "
                                + orderColumnNameEscaped
                                + " DESC LIMIT 1) SETTINGS mutations_sync = 1")
        };
    }
}
