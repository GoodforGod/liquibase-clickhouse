package io.goodforgod.liquibase.extension.clickhouse;

import static org.junit.jupiter.api.Assertions.*;

import io.goodforgod.liquibase.extension.clickhouse.params.ClusterConfig;
import io.goodforgod.liquibase.extension.clickhouse.params.ParamsLoader;
import liquibase.exception.UnexpectedLiquibaseException;
import org.junit.jupiter.api.Test;

public class ParamsLoaderTests {

    @Test
    void loadParams() {
        ClusterConfig params = ParamsLoader.getLiquibaseClickhouseProperties("testLiquibaseClickhouse.properties");
        assertNotNull(params);
        assertEquals("Cluster1", params.getClusterName());
        assertEquals("Path1", params.getTableZooKeeperPathPrefix());
        assertEquals("Replica1", params.getTableReplicaName());
    }

    @Test
    void loadBrokenParams() {
        assertThrows(UnexpectedLiquibaseException.class,
                () -> ParamsLoader.getLiquibaseClickhouseProperties("testLiquibaseClickhouseBroken.properties"));
    }
}
