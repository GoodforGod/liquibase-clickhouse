package io.goodforgod.liquibase.extension.clickhouse.util;

import java.io.InputStream;

/**
 * @author Anton Kurako (GoodforGod)
 * @since 18.12.2021
 */
public interface Resource {

    /**
     * @return resource data as input stream
     */
    InputStream getStream();
}
