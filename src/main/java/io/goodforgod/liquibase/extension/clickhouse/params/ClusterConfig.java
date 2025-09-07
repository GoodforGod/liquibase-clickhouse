package io.goodforgod.liquibase.extension.clickhouse.params;

import java.util.Objects;

public class ClusterConfig {

    private String clusterName;
    private String tableZooKeeperPathPrefix;
    private String tableReplicaName;

    public ClusterConfig() {}

    public ClusterConfig(String clusterName, String tableZooKeeperPathPrefix, String tableReplicaName) {
        this.clusterName = clusterName;
        this.tableZooKeeperPathPrefix = tableZooKeeperPathPrefix;
        this.tableReplicaName = tableReplicaName;
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

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ClusterConfig that = (ClusterConfig) o;
        return Objects.equals(clusterName, that.clusterName)
                && Objects.equals(tableZooKeeperPathPrefix, that.tableZooKeeperPathPrefix)
                && Objects.equals(tableReplicaName, that.tableReplicaName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterName, tableZooKeeperPathPrefix, tableReplicaName);
    }

    @Override
    public String toString() {
        return "ClusterConfig{" +
                "clusterName='" + clusterName + '\'' +
                ", tableZooKeeperPathPrefix='" + tableZooKeeperPathPrefix + '\'' +
                ", tableReplicaName='" + tableReplicaName + '\'' +
                '}';
    }
}
