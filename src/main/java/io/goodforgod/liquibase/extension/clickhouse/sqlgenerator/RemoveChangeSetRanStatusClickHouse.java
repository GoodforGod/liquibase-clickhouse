package io.goodforgod.liquibase.extension.clickhouse.sqlgenerator;

import io.goodforgod.liquibase.extension.clickhouse.database.ClickHouseDatabase;
import io.goodforgod.liquibase.extension.clickhouse.params.ClusterConfig;
import io.goodforgod.liquibase.extension.clickhouse.params.ParamsLoader;
import liquibase.changelog.ChangeSet;
import liquibase.database.Database;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.RemoveChangeSetRanStatusGenerator;
import liquibase.statement.core.RemoveChangeSetRanStatusStatement;

public class RemoveChangeSetRanStatusClickHouse extends RemoveChangeSetRanStatusGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(RemoveChangeSetRanStatusStatement statement, Database database) {
        return database instanceof ClickHouseDatabase;
    }

    @Override
    public Sql[] generateSql(RemoveChangeSetRanStatusStatement statement,
                             Database database,
                             SqlGeneratorChain sqlGeneratorChain) {
        ClusterConfig properties = ParamsLoader.getLiquibaseClickhouseProperties();

        ChangeSet changeSet = statement.getChangeSet();
        String unlockQuery = String.format(
                "ALTER TABLE `%s`.%s "
                        + SqlGeneratorUtil.generateSqlOnClusterClause(properties)
                        + "DELETE WHERE ID = '%s' AND AUTHOR = '%s' AND FILENAME = '%s' SETTINGS mutations_sync = 1",
                database.getDefaultSchemaName(),
                database.getDatabaseChangeLogTableName(),
                changeSet.getId(),
                changeSet.getAuthor(),
                changeSet.getFilePath());
        return SqlGeneratorUtil.generateSql(database, unlockQuery);
    }
}
