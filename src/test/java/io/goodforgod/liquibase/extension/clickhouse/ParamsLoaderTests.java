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
        assertEquals(2, params.getMutationsSyncAcquire());
        assertEquals(1, params.getMutationsSyncRelease());
        assertEquals(1, params.getMutationsSyncInit());
    }

    @Test
    void loadBrokenParams() {
        assertThrows(UnexpectedLiquibaseException.class,
                () -> ParamsLoader.getLiquibaseClickhouseProperties("testLiquibaseClickhouseBroken.properties"));
    }

    @Test
    void loadDefaultsWithoutParamsFile() {
        ClusterConfig params = ParamsLoader.getLiquibaseClickhouseProperties("missingLiquibaseClickhouse.properties");
        assertNotNull(params);
        assertNull(params.getClusterName());
        assertNull(params.getTableZooKeeperPathPrefix());
        assertNull(params.getTableReplicaName());
        assertEquals(2, params.getMutationsSyncAcquire());
        assertEquals(1, params.getMutationsSyncRelease());
        assertEquals(1, params.getMutationsSyncInit());
    }

    @Test
    void mutationsSyncValuesReadPropertiesFile() {
        ClusterConfig params = ParamsLoader.getLiquibaseClickhouseProperties("testLiquibaseClickhouseMutationsSync.properties");
        assertEquals(0, params.getMutationsSyncAcquire());
        assertEquals(0, params.getMutationsSyncRelease());
        assertEquals(0, params.getMutationsSyncInit());
    }

    @Test
    void mutationsSyncFailsOnInvalidValue() {
        assertThrows(UnexpectedLiquibaseException.class,
                () -> ParamsLoader
                        .getLiquibaseClickhouseProperties("testLiquibaseClickhouseInvalidMutationsSync.properties"));
    }
}
