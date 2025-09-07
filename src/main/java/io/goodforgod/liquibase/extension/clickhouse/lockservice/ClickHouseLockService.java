package io.goodforgod.liquibase.extension.clickhouse.lockservice;

import io.goodforgod.liquibase.extension.clickhouse.database.ClickHouseDatabase;
import liquibase.Scope;
import liquibase.changelog.ChangeLogHistoryServiceFactory;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.exception.LockException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.lockservice.StandardLockService;
import liquibase.logging.Logger;
import liquibase.statement.core.LockDatabaseChangeLogStatement;
import liquibase.statement.core.RawSqlStatement;

public class ClickHouseLockService extends StandardLockService {

    private volatile boolean isLockTableInitialized;

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
    protected boolean isDatabaseChangeLogLockTableCreated(boolean forceRecheck) {
        if (forceRecheck || hasDatabaseChangeLogLockTable == null) {
            try {
                String query = String.format("SELECT ID FROM %s.%s LIMIT 1",
                        database.getDefaultSchemaName(), database.getDatabaseChangeLogLockTableName());
                getExecutor().execute(new RawSqlStatement(query));
                hasDatabaseChangeLogLockTable = true;
            } catch (DatabaseException e) {
                getLogger().info(String.format("No %s table available", database.getDatabaseChangeLogLockTableName()));
                hasDatabaseChangeLogLockTable = false;
            }
        }
        return hasDatabaseChangeLogLockTable;
    }

    @Override
    public boolean acquireLock() throws LockException {
        if (hasChangeLogLock) {
            return true;
        }

        quotingStrategy = database.getObjectQuotingStrategy();

        Executor executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", database);

        try {
            database.rollback();
            this.init();

            String query = String.format(
                    "SELECT MAX(LOCKED) FROM %s.%s",
                    database.getDefaultSchemaName(), database.getDatabaseChangeLogLockTableName());
            boolean locked = executor.queryForInt(new RawSqlStatement(query)) > 0;

            if (locked) {
                return false;
            } else {
                executor.comment("Lock Database");
                int rowsUpdated = executor.update(new LockDatabaseChangeLogStatement());

                if (rowsUpdated > 1) {
                    throw new LockException("Did not update change log lock correctly");
                }
                if (rowsUpdated == 0) {
                    // another node was faster
                    return false;
                }
                database.commit();
                Scope.getCurrentScope()
                        .getLog(getClass())
                        .info(coreBundle.getString("successfully.acquired.change.log.lock"));

                hasChangeLogLock = true;

                ChangeLogHistoryServiceFactory.getInstance().resetAll();
                database.setCanCacheLiquibaseTableInfo(true);
                return true;
            }
        } catch (Exception e) {
            throw new LockException(e);
        } finally {
            try {
                database.rollback();
            } catch (DatabaseException ignored) {}
        }
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
