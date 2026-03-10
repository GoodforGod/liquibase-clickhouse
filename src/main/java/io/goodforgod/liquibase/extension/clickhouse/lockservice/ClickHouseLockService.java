package io.goodforgod.liquibase.extension.clickhouse.lockservice;

import io.goodforgod.liquibase.extension.clickhouse.database.ClickHouseDatabase;
import io.goodforgod.liquibase.extension.clickhouse.statement.ClickhouseLockDatabaseChangeLogStatement;
import java.util.logging.Level;
import liquibase.Scope;
import liquibase.changelog.ChangeLogHistoryServiceFactory;
import liquibase.database.Database;
import liquibase.database.ObjectQuotingStrategy;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.exception.LockException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.executor.jvm.ChangelogJdbcMdcListener;
import liquibase.lockservice.StandardLockService;
import liquibase.logging.Logger;
import liquibase.logging.mdc.MdcKey;
import liquibase.logging.mdc.MdcObject;
import liquibase.logging.mdc.MdcValue;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawParameterizedSqlStatement;
import liquibase.statement.core.UnlockDatabaseChangeLogStatement;

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
                        "SELECT COUNT(*) FROM `%s`.`%s`",
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
                String query = String.format("SELECT ID FROM `%s`.`%s` LIMIT 1",
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
                    "SELECT MAX(LOCKED) FROM `%s`.`%s`",
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
                            new RawParameterizedSqlStatement(String.format("SELECT LOCKEDBY FROM `%s`.`%s`",
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

    // copy cat with fix for clickhouse driver v2 doesn't properly return
    // executeUpdate/executeLargeUpdate updated rows in metadata
    @Override
    public void releaseLock() throws LockException {
        ObjectQuotingStrategy incomingQuotingStrategy = null;
        if (this.quotingStrategy != null) {
            incomingQuotingStrategy = database.getObjectQuotingStrategy();
            database.setObjectQuotingStrategy(this.quotingStrategy);
        }

        boolean success = false;
        Executor executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", database);
        try {
            if (this.isDatabaseChangeLogLockTableCreated()) {
                executor.comment("Release Database Lock");
                database.rollback();
                SqlStatement unlockStatement = new UnlockDatabaseChangeLogStatement();
                int updatedRows = ChangelogJdbcMdcListener.query(database, ex -> ex.update(unlockStatement));
                if (updatedRows != 1) {
                    // 0 unlocked, 1 locked
                    Integer lockedStatus = executor.queryForObject(
                            new RawParameterizedSqlStatement(String.format("SELECT LOCKED FROM `%s`.`%s`",
                                    database.getDefaultSchemaName(), database.getDatabaseChangeLogLockTableName())),
                            Integer.class);

                    if (!Integer.valueOf(0).equals(lockedStatus)) {
                        SqlStatement countStatement = new RawParameterizedSqlStatement(
                                String.format("SELECT COUNT(*) FROM `%s`.`%s`",
                                        database.getDefaultSchemaName(), database.getDatabaseChangeLogLockTableName()));

                        throw new LockException(
                                "Did not update change log lock correctly.\n\n" +
                                        updatedRows +
                                        " rows were updated instead of the expected 1 row using executor "
                                        + executor.getClass().getName() + "" +
                                        " there are "
                                        + ChangelogJdbcMdcListener.query(database, ex -> ex.queryForList(countStatement)) +
                                        " rows in the table");
                    }
                }
                database.commit();
                success = true;
            }
        } catch (Exception e) {
            throw new LockException(e);
        } finally {
            try {
                hasChangeLogLock = false;

                database.setCanCacheLiquibaseTableInfo(false);
                try (MdcObject releaseLocksOutcome = Scope.getCurrentScope().addMdcValue(MdcKey.RELEASE_LOCKS_OUTCOME, success
                        ? MdcValue.COMMAND_SUCCESSFUL
                        : MdcValue.COMMAND_FAILED)) {
                    Scope.getCurrentScope().getLog(getClass()).log(success
                            ? Level.INFO
                            : Level.WARNING,
                            (success
                                    ? "Successfully released"
                                    : "Failed to release") + " change log lock",
                            null);
                }

                database.rollback();
            } catch (DatabaseException e) {
                Scope.getCurrentScope().getLog(getClass()).warning("Failed to rollback", e);
            }
            if (incomingQuotingStrategy != null) {
                database.setObjectQuotingStrategy(incomingQuotingStrategy);
            }
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
