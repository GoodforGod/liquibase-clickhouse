package io.goodforgod.liquibase.extension.clickhouse.sqlgenerator;

import io.goodforgod.liquibase.extension.clickhouse.params.ClusterConfig;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import liquibase.database.Database;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.statement.core.RawParameterizedSqlStatement;

class SqlGeneratorUtil {

    public static Sql[] generateSql(Database database, String... statements) {
        SqlGeneratorFactory sqlGeneratorFactory = SqlGeneratorFactory.getInstance();
        List<Sql> allSqlStatements = new ArrayList<>();
        for (String statement : statements) {
            RawParameterizedSqlStatement rawSqlStatement = new RawParameterizedSqlStatement(statement);
            Sql[] perStatement = sqlGeneratorFactory.generateSql(rawSqlStatement, database);
            allSqlStatements.addAll(Arrays.asList(perStatement));
        }
        return allSqlStatements.toArray(new Sql[0]);
    }

    public static String generateSqlOnClusterClause(ClusterConfig properties) {
        if (properties != null)
            return String.format("ON CLUSTER '%s' ", properties.getClusterName());
        else
            return " ";
    }

    public static String generateSqlEngineClause(ClusterConfig properties, String tableName) {
        if (properties != null)
            return String.format(
                    "ENGINE ReplicatedMergeTree('%s','%s') ORDER BY ID",
                    properties.getTableZooKeeperPathPrefix() + tableName.toLowerCase(Locale.ROOT),
                    properties.getTableReplicaName());
        else
            return "ENGINE MergeTree() ORDER BY ID";
    }
}
