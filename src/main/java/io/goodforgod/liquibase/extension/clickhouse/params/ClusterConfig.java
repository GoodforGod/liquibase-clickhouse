package io.goodforgod.liquibase.extension.clickhouse.params;

import java.util.Objects;

public class ClusterConfig {

    private String clusterName;
    private String tableZooKeeperPathPrefix;
    private String tableReplicaName;
    private int mutationsSyncAcquire = 2;
    private int mutationsSyncRelease = 1;
    private int mutationsSyncInit = 1;

    public ClusterConfig() {}

    public ClusterConfig(String clusterName, String tableZooKeeperPathPrefix, String tableReplicaName) {
        this.clusterName = clusterName;
        this.tableZooKeeperPathPrefix = tableZooKeeperPathPrefix;
        this.tableReplicaName = tableReplicaName;
    }

    public ClusterConfig(String clusterName,
                         String tableZooKeeperPathPrefix,
                         String tableReplicaName,
                         int mutationsSyncAcquire,
                         int mutationsSyncRelease,
                         int mutationsSyncInit) {
        this.clusterName = clusterName;
        this.tableZooKeeperPathPrefix = tableZooKeeperPathPrefix;
        this.tableReplicaName = tableReplicaName;
        this.mutationsSyncAcquire = mutationsSyncAcquire;
        this.mutationsSyncRelease = mutationsSyncRelease;
        this.mutationsSyncInit = mutationsSyncInit;
    }

    public boolean isClusterConfigured() {
        return clusterName != null && tableZooKeeperPathPrefix != null && tableReplicaName != null;
    }

    public String getClusterName() {
        return clusterName;
    }

    public ClusterConfig setClusterName(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    public String getTableZooKeeperPathPrefix() {
        return tableZooKeeperPathPrefix;
    }

    public ClusterConfig setTableZooKeeperPathPrefix(String tableZooKeeperPathPrefix) {
        this.tableZooKeeperPathPrefix = tableZooKeeperPathPrefix;
        return this;
    }

    public String getTableReplicaName() {
        return tableReplicaName;
    }

    public ClusterConfig setTableReplicaName(String tableReplicaName) {
        this.tableReplicaName = tableReplicaName;
        return this;
    }

    public int getMutationsSyncAcquire() {
        return mutationsSyncAcquire;
    }

    public ClusterConfig setMutationsSyncAcquire(int mutationsSyncAcquire) {
        this.mutationsSyncAcquire = mutationsSyncAcquire;
        return this;
    }

    public int getMutationsSyncRelease() {
        return mutationsSyncRelease;
    }

    public ClusterConfig setMutationsSyncRelease(int mutationsSyncRelease) {
        this.mutationsSyncRelease = mutationsSyncRelease;
        return this;
    }

    public int getMutationsSyncInit() {
        return mutationsSyncInit;
    }

    public ClusterConfig setMutationsSyncInit(int mutationsSyncInit) {
        this.mutationsSyncInit = mutationsSyncInit;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ClusterConfig that = (ClusterConfig) o;
        return Objects.equals(clusterName, that.clusterName)
                && Objects.equals(tableZooKeeperPathPrefix, that.tableZooKeeperPathPrefix)
                && Objects.equals(tableReplicaName, that.tableReplicaName)
                && mutationsSyncAcquire == that.mutationsSyncAcquire
                && mutationsSyncRelease == that.mutationsSyncRelease
                && mutationsSyncInit == that.mutationsSyncInit;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                clusterName, tableZooKeeperPathPrefix, tableReplicaName, mutationsSyncAcquire, mutationsSyncRelease,
                mutationsSyncInit);
    }

    @Override
    public String toString() {
        return "ClusterConfig{" +
                "clusterName='" + clusterName + '\'' +
                ", tableZooKeeperPathPrefix='" + tableZooKeeperPathPrefix + '\'' +
                ", tableReplicaName='" + tableReplicaName + '\'' +
                ", mutationsSyncAcquire=" + mutationsSyncAcquire +
                ", mutationsSyncRelease=" + mutationsSyncRelease +
                ", mutationsSyncInit=" + mutationsSyncInit +
                '}';
    }
}
