package io.goodforgod.liquibase.extension.clickhouse.sqlgenerator;

import static liquibase.util.SqlUtil.replacePredicatePlaceholders;

import io.goodforgod.liquibase.extension.clickhouse.database.ClickHouseDatabase;
import io.goodforgod.liquibase.extension.clickhouse.params.ClusterConfig;
import io.goodforgod.liquibase.extension.clickhouse.params.ParamsLoader;
import java.util.Date;
import liquibase.database.Database;
import liquibase.datatype.DataTypeFactory;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.UpdateGenerator;
import liquibase.statement.DatabaseFunction;
import liquibase.statement.core.UpdateStatement;

public class UpdateGeneratorClickHouse extends UpdateGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(UpdateStatement statement, Database database) {
        return database instanceof ClickHouseDatabase;
    }

    @Override
    public Sql[] generateSql(UpdateStatement statement,
                             Database database,
                             SqlGeneratorChain sqlGeneratorChain) {
        ClusterConfig properties = ParamsLoader.getLiquibaseClickhouseProperties();

        StringBuilder sb = new StringBuilder(
                String.format(
                        "ALTER TABLE `%s`.`%s` " + SqlGeneratorUtil.generateSqlOnClusterClause(properties),
                        statement.getCatalogName(),
                        statement.getTableName()));

        sb.append(" UPDATE ");
        for (String column : statement.getNewColumnValues().keySet()) {
            sb.append(" ")
                    .append(column)
                    .append(" = ")
                    .append(convertToString(statement.getNewColumnValues().get(column), database))
                    .append(",");
        }
        int lastComma = sb.lastIndexOf(",");
        if (lastComma >= 0) {
            sb.deleteCharAt(lastComma);
        }
        if (statement.getWhereClause() != null) {
            sb.append(" WHERE ")
                    .append(
                            replacePredicatePlaceholders(
                                    database,
                                    statement.getWhereClause(),
                                    statement.getWhereColumnNames(),
                                    statement.getWhereParameters()));
        }

        String updateQuery = sb.toString();

        return SqlGeneratorUtil.generateSql(database, updateQuery);
    }

    private String convertToString(Object newValue, Database database) {
        String sqlString;
        if ((newValue == null) || "NULL".equalsIgnoreCase(newValue.toString())) {
            sqlString = "NULL";
        } else if ((newValue instanceof String)
                && !looksLikeFunctionCall(((String) newValue), database)) {
            sqlString = DataTypeFactory.getInstance()
                    .fromObject(newValue, database)
                    .objectToSql(newValue, database);
        } else if (newValue instanceof Date) {
            // converting java.util.Date to java.sql.Date
            Date date = (Date) newValue;
            if (date.getClass().equals(java.util.Date.class)) {
                date = new java.sql.Date(date.getTime());
            }

            sqlString = database.getDateLiteral(date);
        } else if (newValue instanceof Boolean) {
            if (((Boolean) newValue)) {
                sqlString = DataTypeFactory.getInstance().getTrueBooleanValue(database);
            } else {
                sqlString = DataTypeFactory.getInstance().getFalseBooleanValue(database);
            }
        } else if (newValue instanceof DatabaseFunction) {
            sqlString = database.generateDatabaseFunctionValue((DatabaseFunction) newValue);
        } else {
            sqlString = newValue.toString();
        }
        return sqlString;
    }
}
