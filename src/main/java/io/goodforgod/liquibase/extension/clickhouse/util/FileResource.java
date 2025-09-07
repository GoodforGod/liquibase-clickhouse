package io.goodforgod.liquibase.extension.clickhouse.util;

import java.io.File;

/**
 * Model for representing inner resource
 *
 * @author Anton Kurako (GoodforGod)
 * @since 18.12.2021
 */
public final class FileResource implements PathResource {

    private final File file;

    private FileResource(File file) {
        this.file = file;
    }

    public static FileResource of(File file) {
        return new FileResource(file);
    }

    /**
     * @return resource as file
     */
    public File getFile() {
        return file;
    }

    @Override
    public String getPath() {
        return file.toURI().getPath();
    }
}
