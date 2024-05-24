package io.goodforgod.liquibase.extension.clickhouse.params;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import liquibase.Scope;
import liquibase.logging.Logger;

public class ParamsLoader {

    private static final Logger LOG = Scope.getCurrentScope().getLog(ParamsLoader.class);

    private static final Set<String> validProperties = new HashSet<>(
            Arrays.asList("clusterName", "tableZooKeeperPathPrefix", "tableReplicaName"));

    private static Map<String, ClusterConfig> liquibaseClickhouseProperties = new ConcurrentHashMap<>();

    private static StringBuilder appendWithComma(StringBuilder sb, String text) {
        if (sb.length() > 0) {
            sb.append(", ");
        }
        sb.append(text);
        return sb;
    }

    private static String getMissingProperties(Set<String> properties) {
        StringBuilder missingProperties = new StringBuilder();
        for (String validProperty : validProperties)
            if (!properties.contains(validProperty)) {
                appendWithComma(missingProperties, validProperty);
            }

        return missingProperties.toString();
    }

    private static void checkProperties(Map<String, String> properties) throws InvalidPropertiesFormatException {
        StringBuilder errMsg = new StringBuilder();

        for (String key : properties.keySet())
            if (!validProperties.contains(key)) {
                appendWithComma(errMsg, "unknown property: ").append(key);
            }

        if (errMsg.length() > 0 || properties.size() != validProperties.size()) {
            appendWithComma(errMsg, "the missing properties should be defined: ");
            errMsg.append(getMissingProperties(properties.keySet()));
        }

        if (errMsg.length() > 0) {
            throw new InvalidPropertiesFormatException(errMsg.toString());
        }
    }

    private static String getStackTrace(Exception e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }

    public static ClusterConfig getLiquibaseClickhouseProperties() {
        return getLiquibaseClickhouseProperties("liquibaseClickhouse");
    }

    public static ClusterConfig getLiquibaseClickhouseProperties(String configFile) {
        return liquibaseClickhouseProperties.computeIfAbsent(configFile, k -> {
            Config conf = ConfigFactory.load(configFile);
            Map<String, String> params = new HashMap<>();

            try {
                for (Map.Entry<String, ConfigValue> s : conf.getConfig("cluster").entrySet())
                    params.put(s.getKey(), s.getValue().unwrapped().toString());

                checkProperties(params);
                ClusterConfig result = new ClusterConfig(
                        params.get("clusterName"),
                        params.get("tableZooKeeperPathPrefix"),
                        params.get("tableReplicaName"));

                LOG.info("Cluster settings ("
                        + configFile
                        + ".conf) are found. Work in cluster replicated clickhouse mode.");
                return result;
            } catch (ConfigException.Missing e) {
                LOG.info("Cluster settings ("
                        + configFile
                        + ".conf) are not defined. Work in single-instance clickhouse mode.");
                LOG.info("The following properties should be defined: " + getMissingProperties(new HashSet<>()));
                return null;
            } catch (InvalidPropertiesFormatException e) {
                LOG.severe(getStackTrace(e));
                LOG.severe("Work in single-instance clickhouse mode.");
                return null;
            }
        });
    }
}
