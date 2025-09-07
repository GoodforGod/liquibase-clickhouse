package io.goodforgod.liquibase.extension.clickhouse.sqlgenerator;

import io.goodforgod.liquibase.extension.clickhouse.database.ClickHouseDatabase;
import io.goodforgod.liquibase.extension.clickhouse.params.ClusterConfig;
import io.goodforgod.liquibase.extension.clickhouse.params.ParamsLoader;
import liquibase.ChecksumVersion;
import liquibase.changelog.ChangeSet;
import liquibase.database.Database;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.UpdateChangeSetChecksumGenerator;
import liquibase.statement.core.UpdateChangeSetChecksumStatement;

public class UpdateChangeSetChecksumClickHouse extends UpdateChangeSetChecksumGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(UpdateChangeSetChecksumStatement statement, Database database) {
        return database instanceof ClickHouseDatabase;
    }

    @Override
    public Sql[] generateSql(UpdateChangeSetChecksumStatement statement,
                             Database database,
                             SqlGeneratorChain sqlGeneratorChain) {
        ClusterConfig properties = ParamsLoader.getLiquibaseClickhouseProperties();

        ChangeSet changeSet = statement.getChangeSet();
        String updateChecksumQuery = String.format(
                "ALTER TABLE `%s`.%s "
                        + SqlGeneratorUtil.generateSqlOnClusterClause(properties)
                        + "UPDATE MD5SUM = '%s' WHERE ID = '%s' AND AUTHOR = '%s' AND FILENAME = '%s' SETTINGS mutations_sync = 1",
                database.getDefaultSchemaName(),
                database.getDatabaseChangeLogTableName(),
                changeSet.generateCheckSum(ChecksumVersion.V9).toString(),
                changeSet.getId(),
                changeSet.getAuthor(),
                changeSet.getFilePath());

        return SqlGeneratorUtil.generateSql(database, updateChecksumQuery);
    }
}
