package io.goodforgod.liquibase.extension.clickhouse.params;

import io.goodforgod.liquibase.extension.clickhouse.util.ResourceUtils;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import liquibase.Scope;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.logging.Logger;

public class ParamsLoader {

    private ParamsLoader() {}

    private static final Logger logger = Scope.getCurrentScope().getLog(ParamsLoader.class);

    private static final Set<String> CLICKHOUSE_PROPERTIES = Set.of(
            "clickhouse.cluster.clusterName",
            "clickhouse.cluster.tableZooKeeperPathPrefix",
            "clickhouse.cluster.tableReplicaName");
    private static final String CLUSTER_NAME_PROPERTY = "clickhouse.cluster.clusterName";
    private static final String CLUSTER_ZOOKEEPER_PATH_PREFIX_PROPERTY = "clickhouse.cluster.tableZooKeeperPathPrefix";
    private static final String CLUSTER_REPLICA_NAME_PROPERTY = "clickhouse.cluster.tableReplicaName";
    private static final String MUTATIONS_SYNC_ACQUIRE_PROPERTY = "clickhouse.mutationsSyncAcquire";
    private static final String MUTATIONS_SYNC_RELEASE_PROPERTY = "clickhouse.mutationsSyncRelease";
    private static final String MUTATIONS_SYNC_INIT_PROPERTY = "clickhouse.mutationsSyncInit";
    private static final String CLUSTER_NAME_ENV_VARIABLE = "LIQUIBASE_CLICKHOUSE_CLUSTER_NAME";
    private static final String CLUSTER_ZOOKEEPER_PATH_PREFIX_ENV_VARIABLE = "LIQUIBASE_CLICKHOUSE_CLUSTER_TABLE_ZOOKEEPER_PATH_PREFIX";
    private static final String CLUSTER_REPLICA_NAME_ENV_VARIABLE = "LIQUIBASE_CLICKHOUSE_CLUSTER_TABLE_REPLICA_NAME";
    private static final String MUTATIONS_SYNC_ACQUIRE_ENV_VARIABLE = "LIQUIBASE_CLICKHOUSE_MUTATIONS_SYNC_ACQUIRE";
    private static final String MUTATIONS_SYNC_RELEASE_ENV_VARIABLE = "LIQUIBASE_CLICKHOUSE_MUTATIONS_SYNC_RELEASE";
    private static final String MUTATIONS_SYNC_INIT_ENV_VARIABLE = "LIQUIBASE_CLICKHOUSE_MUTATIONS_SYNC_INIT";
    private static final int DEFAULT_MUTATIONS_SYNC_ACQUIRE = 2;
    private static final int DEFAULT_MUTATIONS_SYNC_RELEASE = 1;
    private static final int DEFAULT_MUTATIONS_SYNC_INIT = 1;

    private static final Map<String, ClusterConfig> LIQUIBASE_CLICKHOUSE_PROPERTIES = new ConcurrentHashMap<>();

    private static StringBuilder appendWithComma(StringBuilder sb, String text) {
        if (sb.length() > 0) {
            sb.append(", ");
        }
        sb.append(text);
        return sb;
    }

    private static String getMissingProperties(Set<String> properties) {
        StringBuilder missingProperties = new StringBuilder();
        for (String validProperty : CLICKHOUSE_PROPERTIES)
            if (!properties.contains(validProperty)) {
                appendWithComma(missingProperties, validProperty);
            }

        return missingProperties.toString();
    }

    public static ClusterConfig getLiquibaseClickhouseProperties() {
        String propFile = Optional.ofNullable(System.getProperty("liquibaseClickhousePropertiesFile"))
                .or(() -> Optional.ofNullable(System.getenv("LIQUIBASE_CLICKHOUSE_PROPERTIES_FILE")))
                .orElse("liquibaseClickhouse.properties");

        return getLiquibaseClickhouseProperties(propFile);
    }

    public static ClusterConfig getLiquibaseClickhouseProperties(String configFile) {
        return LIQUIBASE_CLICKHOUSE_PROPERTIES.computeIfAbsent(configFile, k -> {
            try {
                final Properties properties = new Properties();
                Optional<String> propsAsString = ResourceUtils.getFileAsString(configFile);
                if (propsAsString.isPresent()) {
                    properties.load(new StringReader(propsAsString.get()));
                } else {
                    logger.info("Clickhouse settings file not found, using environment variables and defaults: " + configFile);
                }

                String clusterName = getConfigValue(
                        properties, CLUSTER_NAME_PROPERTY, CLUSTER_NAME_ENV_VARIABLE);
                String tableZooKeeperPathPrefix = getConfigValue(
                        properties, CLUSTER_ZOOKEEPER_PATH_PREFIX_PROPERTY, CLUSTER_ZOOKEEPER_PATH_PREFIX_ENV_VARIABLE);
                String tableReplicaName = getConfigValue(
                        properties, CLUSTER_REPLICA_NAME_PROPERTY, CLUSTER_REPLICA_NAME_ENV_VARIABLE);
                int mutationsSyncAcquire = resolveMutationsSync(
                        properties,
                        MUTATIONS_SYNC_ACQUIRE_PROPERTY,
                        MUTATIONS_SYNC_ACQUIRE_ENV_VARIABLE,
                        DEFAULT_MUTATIONS_SYNC_ACQUIRE,
                        "lock acquire");
                int mutationsSyncRelease = resolveMutationsSync(
                        properties,
                        MUTATIONS_SYNC_RELEASE_PROPERTY,
                        MUTATIONS_SYNC_RELEASE_ENV_VARIABLE,
                        DEFAULT_MUTATIONS_SYNC_RELEASE,
                        "lock release");
                int mutationsSyncInit = resolveMutationsSync(
                        properties,
                        MUTATIONS_SYNC_INIT_PROPERTY,
                        MUTATIONS_SYNC_INIT_ENV_VARIABLE,
                        DEFAULT_MUTATIONS_SYNC_INIT,
                        "lock init");

                validateClusterConfig(clusterName, tableZooKeeperPathPrefix, tableReplicaName);

                ClusterConfig clusterConfig = new ClusterConfig(
                        clusterName,
                        tableZooKeeperPathPrefix,
                        tableReplicaName,
                        mutationsSyncAcquire,
                        mutationsSyncRelease,
                        mutationsSyncInit);

                if (clusterConfig.isClusterConfigured()) {
                    logger.info("Clickhouse Cluster settings are found and processed correctly. "
                            + "Work in cluster replicated clickhouse mode.");
                } else {
                    logger.info("Clickhouse Cluster settings are not configured. Work in single-instance clickhouse mode.");
                }
                return clusterConfig;
            } catch (IllegalArgumentException e) {
                logger.severe("Clickhouse settings ("
                        + configFile
                        + ") are not defined properly.", e);
                throw new UnexpectedLiquibaseException(e);
            } catch (IOException e) {
                logger.severe("Clickhouse settings config file ("
                        + configFile
                        + ") parse exception.", e);
                throw new UnexpectedLiquibaseException(e);
            }
        });
    }

    private static void validateClusterConfig(
                                              String clusterName,
                                              String tableZooKeeperPathPrefix,
                                              String tableReplicaName) {
        Set<String> presentProperties = new HashSet<>();
        if (clusterName != null) {
            presentProperties.add(CLUSTER_NAME_PROPERTY);
        }
        if (tableZooKeeperPathPrefix != null) {
            presentProperties.add(CLUSTER_ZOOKEEPER_PATH_PREFIX_PROPERTY);
        }
        if (tableReplicaName != null) {
            presentProperties.add(CLUSTER_REPLICA_NAME_PROPERTY);
        }

        if (!presentProperties.isEmpty() && presentProperties.size() != CLICKHOUSE_PROPERTIES.size()) {
            throw new IllegalArgumentException(
                    "Clickhouse Cluster settings are partially configured. "
                            + "Either define all cluster settings or none. Missing settings: "
                            + getMissingProperties(presentProperties));
        }
    }

    private static int resolveMutationsSync(
                                            Properties properties,
                                            String propertyName,
                                            String envName,
                                            int defaultValue,
                                            String scope) {
        String propertyValue = getEnvValueOrSelf(properties.getProperty(propertyName));
        String value = System.getenv(envName);
        String source = "environment variable '" + envName + "'";
        if (value == null) {
            value = propertyValue;
            source = "property '" + propertyName + "'";
        }

        if (value == null) {
            return defaultValue;
        }
        if (value.isBlank()) {
            throw new IllegalArgumentException("Blank Clickhouse 'mutations_sync' value for "
                    + scope
                    + " from "
                    + source
                    + ". Expected one of 0, 1, 2.");
        }

        try {
            int parsed = Integer.parseInt(value.trim());
            if (parsed < 0 || parsed > 2) {
                throw new NumberFormatException("value out of range [0, 2]");
            }
            if (parsed == 0) {
                logger.warning("Clickhouse 'mutations_sync=0' for "
                        + scope
                        + " configured from "
                        + source
                        + ". This makes Liquibase lock table mutation asynchronous: ClickHouse accepts ALTER TABLE "
                        + "mutation and returns before DATABASECHANGELOGLOCK row is necessarily changed. Liquibase can "
                        + "continue while LOCKED or LOCKEDBY still contain old values. If another Liquibase process runs "
                        + "during this window, processes can observe stale lock state, both can begin migrations, and "
                        + "changelog state or DDL execution can race. Lock recheck logic can also read null or stale "
                        + "LOCKEDBY before the mutation is applied. Use mutations_sync=0 only when an external scheduler, "
                        + "CI lock, or other serialization guarantees that no concurrent Liquibase execution can run "
                        + "against the same ClickHouse database.");
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid Clickhouse 'mutations_sync' value for "
                    + scope
                    + " '"
                    + value
                    + "' from "
                    + source
                    + "', expected one of 0, 1, 2.", e);
        }
    }

    private static String getConfigValue(Properties properties, String propertyName, String envName) {
        String envValue = System.getenv(envName);
        if (envValue != null) {
            return normalizePropertyValue(envValue);
        }

        return getPropertyValue(properties, propertyName);
    }

    private static String getPropertyValue(Properties properties, String propertyName) {
        String propertyValue = properties.getProperty(propertyName);
        return normalizePropertyValue(getEnvValueOrSelf(propertyValue));
    }

    private static String normalizePropertyValue(String propertyValue) {
        return propertyValue == null || propertyValue.isBlank()
                ? null
                : propertyValue;
    }

    private static boolean isEnvironmentValue(String value) {
        return value != null && value.startsWith("${") && value.endsWith("}");
    }

    private static String getEnvValueOrSelf(String envOrValue) {
        if (isEnvironmentValue(envOrValue)) {
            final String envProperty = envOrValue.substring(2, envOrValue.length() - 1);
            final String[] environmentAndDefault = envProperty.split("\\|");

            if (environmentAndDefault.length > 2) {
                throw new IllegalArgumentException(
                        "Property can contain only 1 ':' symbol but got: " + envProperty);
            } else if (environmentAndDefault.length == 2) {
                final String envValue = System.getenv(environmentAndDefault[0]);
                if (envValue == null) {
                    return (environmentAndDefault[1].isBlank())
                            ? null
                            : environmentAndDefault[1];
                }

                return envValue;
            }

            return System.getenv(environmentAndDefault[0]);
        } else {
            return envOrValue;
        }
    }
}
