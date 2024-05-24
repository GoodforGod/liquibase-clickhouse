
package io.goodforgod.liquibase.extension.clickhouse.lockservice;

import liquibase.Scope;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import io.goodforgod.liquibase.extension.clickhouse.database.ClickHouseDatabase;
import liquibase.lockservice.StandardLockService;
import liquibase.logging.Logger;
import liquibase.statement.core.RawSqlStatement;

public class ClickHouseLockService extends StandardLockService {

    private boolean isLockTableInitialized;

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(Database database) {
        return database instanceof ClickHouseDatabase;
    }

    @Override
    public boolean isDatabaseChangeLogLockTableInitialized(boolean tableJustCreated) {
        if (!isLockTableInitialized) {
            try {
                String query = String.format(
                        "SELECT COUNT(*) FROM `%s`.%s",
                        database.getDefaultSchemaName(), database.getDatabaseChangeLogLockTableName());
                int nbRows = getExecutor().queryForInt(new RawSqlStatement(query));
                isLockTableInitialized = nbRows > 0;
            } catch (LiquibaseException e) {
                if (getExecutor().updatesDatabase()) {
                    throw new UnexpectedLiquibaseException(e);
                } else {
                    isLockTableInitialized = !tableJustCreated;
                }
            }
        }
        return isLockTableInitialized;
    }

    @Override
    protected boolean isDatabaseChangeLogLockTableCreated() {
        boolean hasTable = false;
        try {
            String query = String.format(
                    "SELECT ID FROM `%s`.%s LIMIT 1",
                    database.getDefaultSchemaName(), database.getDatabaseChangeLogLockTableName());
            getExecutor().execute(new RawSqlStatement(query));
            hasTable = true;
        } catch (DatabaseException e) {
            getLogger()
                    .info(
                            String.format("No %s table available", database.getDatabaseChangeLogLockTableName()));
        }
        return hasTable;
    }

    private Executor getExecutor() {
        return Scope.getCurrentScope()
                .getSingleton(ExecutorService.class)
                .getExecutor("jdbc", database);
    }

    private Logger getLogger() {
        return Scope.getCurrentScope().getLog(ClickHouseLockService.class);
    }
}
