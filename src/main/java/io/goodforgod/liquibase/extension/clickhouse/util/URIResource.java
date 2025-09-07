package io.goodforgod.liquibase.extension.clickhouse.util;

import java.net.URI;

/**
 * Model for representing inner resource
 *
 * @author Anton Kurako (GoodforGod)
 * @since 18.12.2021
 */
public final class URIResource implements PathResource {

    private final URI uri;

    private URIResource(URI uri) {
        this.uri = uri;
    }

    public static URIResource of(URI uri) {
        return new URIResource(uri);
    }

    /**
     * @return path to resource
     */
    @Override
    public String getPath() {
        return uri.getPath();
    }
}
