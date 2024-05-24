
package io.goodforgod.liquibase.extension.clickhouse.sqlgenerator;

import java.util.*;
import liquibase.database.Database;
import io.goodforgod.liquibase.extension.clickhouse.params.ClusterConfig;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.statement.core.RawSqlStatement;

class SqlGeneratorUtil {

    public static Sql[] generateSql(Database database, String... statements) {
        SqlGeneratorFactory sqlGeneratorFactory = SqlGeneratorFactory.getInstance();
        List<Sql> allSqlStatements = new ArrayList<>();
        for (String statement : statements) {
            RawSqlStatement rawSqlStatement = new RawSqlStatement(statement);
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
