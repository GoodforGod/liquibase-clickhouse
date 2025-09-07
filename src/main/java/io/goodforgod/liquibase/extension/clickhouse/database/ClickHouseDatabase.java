package io.goodforgod.liquibase.extension.clickhouse.database;

import com.clickhouse.jdbc.ClickHouseDriver;
import java.text.SimpleDateFormat;
import java.util.Date;
import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.DatabaseConnection;
import liquibase.exception.DatabaseException;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawParameterizedSqlStatement;

public class ClickHouseDatabase extends AbstractJdbcDatabase {

    private static final String DATABASE_NAME = "ClickHouse";
    private static final int DEFAULT_PORT = 8123;
    private static final String DRIVER_CLASS_NAME = ClickHouseDriver.class.getName();

    public static final String CURRENT_DATE_TIME_FUNCTION = "toDateTime64('"
            + new SimpleDateFormat("yyyy.MM.dd HH:mm:ss.SSS").format(new Date())
            + "',3)";

    public ClickHouseDatabase() {
        super();
        this.setCurrentDateTimeFunction(CURRENT_DATE_TIME_FUNCTION);
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    protected String getDefaultDatabaseProductName() {
        return DATABASE_NAME;
    }

    @Override
    public boolean isCorrectDatabaseImplementation(DatabaseConnection conn) throws DatabaseException {
        return DATABASE_NAME.equals(conn.getDatabaseProductName());
    }

    @Override
    public String getDefaultDriver(String url) {
        return url != null && url.startsWith("jdbc:clickhouse")
                ? DRIVER_CLASS_NAME
                : null;
    }

    @Override
    public String getShortName() {
        return "clickhouse";
    }

    @Override
    public Integer getDefaultPort() {
        return DEFAULT_PORT;
    }

    @Override
    public boolean supportsInitiallyDeferrableColumns() {
        return false;
    }

    @Override
    public boolean supportsTablespaces() {
        return false;
    }

    @Override
    public boolean supportsSequences() {
        return false;
    }

    @Override
    public boolean supportsDDLInTransaction() {
        return false;
    }

    @Override
    protected SqlStatement getConnectionSchemaNameCallStatement() {
        return new RawParameterizedSqlStatement("SELECT currentDatabase()");
    }
}
