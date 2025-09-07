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
            Map<String, String> params = new HashMap<>();
            try {
                Optional<String> propsAsString = ResourceUtils.getFileAsString(configFile);
                if (propsAsString.isEmpty()) {
                    logger.info("Clickhouse Cluster settings file not found, skipping cluster properties: " + configFile);
                    return null;
                }

                final Properties properties = new Properties();
                properties.load(new StringReader(propsAsString.get()));

                for (String clickhouseProperty : CLICKHOUSE_PROPERTIES) {
                    if (properties.getProperty(clickhouseProperty) == null) {
                        throw new IllegalArgumentException(
                                "Clickhouse Cluster Settings properties file missing property: " + clickhouseProperty);
                    }
                }

                ClusterConfig clusterConfig = new ClusterConfig(
                        getPropertyValue(properties, "clickhouse.cluster.clusterName"),
                        getPropertyValue(properties, "clickhouse.cluster.tableZooKeeperPathPrefix"),
                        getPropertyValue(properties, "clickhouse.cluster.tableReplicaName"));

                logger.info("Clickhouse Cluster settings ("
                        + configFile
                        + ".conf) are found and processed correctly. Work in cluster replicated clickhouse mode.");
                return clusterConfig;
            } catch (IllegalArgumentException e) {
                logger.severe("Clickhouse Cluster settings ("
                        + configFile
                        + ".conf) are not defined properly. Work in single-instance clickhouse mode.", e);
                logger.severe("The following properties should be defined: " + getMissingProperties(params.keySet()));
                throw new UnexpectedLiquibaseException(e);
            } catch (IOException e) {
                logger.severe("Clickhouse Cluster Settings config file ("
                        + configFile
                        + ".conf) parse exception. Work in single-instance clickhouse mode.", e);
                throw new UnexpectedLiquibaseException(e);
            }
        });
    }

    private static String getPropertyValue(Properties properties, String propertyName) {
        String propertyValue = properties.getProperty(propertyName);
        return getEnvValueOrSelf(propertyValue);
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
