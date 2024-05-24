
package io.goodforgod.liquibase.extension.clickhouse;

import io.goodforgod.liquibase.extension.clickhouse.params.ClusterConfig;
import io.goodforgod.liquibase.extension.clickhouse.params.ParamsLoader;
import org.junit.jupiter.api.Test;

public class ParamsLoaderTests {

    @Test
    void loadParams() {
        ClusterConfig params = ParamsLoader.getLiquibaseClickhouseProperties("testLiquibaseClickhouse");
        assert (params != null);
        assert (params.getClusterName().equals("Cluster1"));
        assert (params.getTableZooKeeperPathPrefix().equals("Path1"));
        assert (params.getTableReplicaName().equals("Replica1"));
    }

    @Test
    void loadBrokenParams() {
        ClusterConfig params = ParamsLoader.getLiquibaseClickhouseProperties("testLiquibaseClickhouseBroken");
        assert (params == null);
    }
}
