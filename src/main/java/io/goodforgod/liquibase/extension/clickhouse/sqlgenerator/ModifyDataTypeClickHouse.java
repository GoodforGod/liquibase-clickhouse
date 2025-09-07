package io.goodforgod.liquibase.extension.clickhouse.sqlgenerator;

import io.goodforgod.liquibase.extension.clickhouse.database.ClickHouseDatabase;
import liquibase.database.Database;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.ModifyDataTypeGenerator;
import liquibase.statement.core.ModifyDataTypeStatement;

/**
 * Noop, overridden to ensure Liquibase does not try to modify its changelog table columns sizes (as
 * the String type is used to encode strings, not FixedString(N).
 */
public class ModifyDataTypeClickHouse extends ModifyDataTypeGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(ModifyDataTypeStatement statement, Database database) {
        return database instanceof ClickHouseDatabase;
    }

    @Override
    public Sql[] generateSql(ModifyDataTypeStatement statement,
                             Database database,
                             SqlGeneratorChain sqlGeneratorChain) {
        return new Sql[0];
    }
}
