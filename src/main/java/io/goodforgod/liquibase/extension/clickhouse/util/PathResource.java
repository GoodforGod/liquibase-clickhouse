package io.goodforgod.liquibase.extension.clickhouse.util;

import java.io.InputStream;

/**
 * @author Anton Kurako (GoodforGod)
 * @since 18.12.2021
 */
public interface PathResource extends Resource {

    /**
     * @return path to resource
     */

    String getPath();

    @Override
    default InputStream getStream() {
        final String path = getPath();
        final ClassLoader classLoader = getClass().getClassLoader();
        final InputStream stream = classLoader.getResourceAsStream(path);
        return (stream == null)
                ? classLoader.getResourceAsStream("/" + path)
                : stream;
    }
}
