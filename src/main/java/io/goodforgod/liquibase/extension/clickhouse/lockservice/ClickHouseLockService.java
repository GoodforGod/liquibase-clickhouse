package io.goodforgod.liquibase.extension.clickhouse.lockservice;

import io.goodforgod.liquibase.extension.clickhouse.database.ClickHouseDatabase;
import io.goodforgod.liquibase.extension.clickhouse.statement.ClickhouseLockDatabaseChangeLogStatement;
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
import liquibase.statement.core.RawParameterizedSqlStatement;

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
                int nbRows = getExecutor().queryForInt(new RawParameterizedSqlStatement(query));
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
                getExecutor().execute(new RawParameterizedSqlStatement(query));
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
            boolean locked = executor.queryForInt(new RawParameterizedSqlStatement(query)) > 0;

            if (locked) {
                return false;
            } else {
                executor.comment("Lock Database");
                final ClickhouseLockDatabaseChangeLogStatement statement = new ClickhouseLockDatabaseChangeLogStatement();
                int rowsUpdated = executor.update(statement);
                if (rowsUpdated > 1) {
                    throw new LockException("Did not update change log lock correctly");
                }

                if (rowsUpdated == 0) {
                    // recheck on ID cause clickhouse driver v2 doesn't properly return executeUpdate/executeLargeUpdate
                    // updated rows in metadata
                    String lockedBy = executor.queryForObject(
                            new RawParameterizedSqlStatement(String.format("SELECT LOCKEDBY FROM %s.%s",
                                    database.getDefaultSchemaName(), database.getDatabaseChangeLogLockTableName())),
                            String.class);

                    if (!lockedBy.equals(statement.getHost())) {
                        // another node was faster
                        return false;
                    }
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
