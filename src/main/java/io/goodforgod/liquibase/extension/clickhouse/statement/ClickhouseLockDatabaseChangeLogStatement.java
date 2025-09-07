package io.goodforgod.liquibase.extension.clickhouse.statement;

import java.util.UUID;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.statement.AbstractSqlStatement;
import liquibase.util.NetUtil;

/**
 * Anton Kurako (GoodforGod)
 *
 * @since 07.09.2025
 */
public class ClickhouseLockDatabaseChangeLogStatement extends AbstractSqlStatement {

    private static final String HOST_NAME;
    private static final String HOST_ADDRESS;
    private static final String HOST_DESCRIPTION = (System.getProperty("liquibase.hostDescription") == null)
            ? ""
            : ("#" + System.getProperty("liquibase.hostDescription"));

    static {
        try {
            HOST_NAME = NetUtil.getLocalHostName();
            HOST_ADDRESS = NetUtil.getLocalHostAddress();
        } catch (Exception e) {
            throw new UnexpectedLiquibaseException(e);
        }
    }

    private final String hostRequestId;

    public ClickhouseLockDatabaseChangeLogStatement() {
        this.hostRequestId = UUID.randomUUID().toString();
    }

    public String getHostRequestId() {
        return hostRequestId;
    }

    public String getHost() {
        return String.format("%s-%s %s (%s)", getHostName(), getHostRequestId(), getHostDescription(), getHostAddress());
    }

    public String getHostName() {
        return HOST_NAME;
    }

    public String getHostAddress() {
        return HOST_ADDRESS;
    }

    public String getHostDescription() {
        return HOST_DESCRIPTION;
    }

    @Override
    public String toString() {
        return "ClickhouseLockDatabaseChangeLogStatement{" +
                "id='" + hostRequestId + '\'' +
                '}';
    }
}
