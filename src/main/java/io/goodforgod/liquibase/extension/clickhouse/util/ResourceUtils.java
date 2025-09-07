package io.goodforgod.liquibase.extension.clickhouse.util;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Predicate;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

/**
 * Utils for extraction resources inside JAR and outside JAR
 *
 * @since 07.09.2025
 */
public final class ResourceUtils {

    private ResourceUtils() {}

    /**
     * @param path under which to look for resources
     * @return resources under given path
     */
    public static List<PathResource> getResources(String path) {
        return getResources(path, p -> true);
    }

    /**
     * @param path          under which to look for resources
     * @param pathPredicate predicate to validate paths
     * @return resources under given path
     */

    public static List<PathResource> getResources(String path,
                                                  Predicate<String> pathPredicate) {
        try {
            final URL url = ResourceUtils.class.getClassLoader().getResource(path);
            if (url == null) {
                return Collections.emptyList();
            }

            final String jarPath = url.getPath()
                    .replaceFirst("[.]jar!.*", ".jar")
                    .replaceFirst("file:", "")
                    .replace(" ", "\\ ");

            try (final JarFile jarFile = new JarFile(jarPath)) {
                final Enumeration<JarEntry> entries = jarFile.entries();
                final List<PathResource> resources = new ArrayList<>();

                while (entries.hasMoreElements()) {
                    final JarEntry entry = entries.nextElement();
                    final String name = entry.getName();
                    if (name.startsWith(path) && pathPredicate.test(name)) {
                        final URI uri = new URI(entry.getName());
                        resources.add(URIResource.of(uri));
                    }
                }

                return resources;
            }
        } catch (IOException | URISyntaxException e) {
            final String filePath = "/" + path;

            final URL resource = ResourceUtils.class.getResource(filePath);
            if (resource == null) {
                return Collections.emptyList();
            }

            final File[] files = new File(resource.getPath()).listFiles();
            if (files == null || files.length == 0) {
                return Collections.emptyList();
            }

            return Arrays.stream(files)
                    .filter(f -> pathPredicate.test(f.getPath()))
                    .map(FileResource::of)
                    .collect(Collectors.toList());
        }
    }

    public static Optional<InputStream> getFileAsStream(String path) {
        return Optional.ofNullable(Optional.ofNullable(ResourceUtils.class.getResourceAsStream(path))
                .orElseGet(() -> ResourceUtils.class.getResourceAsStream("/" + path)));
    }

    public static Optional<String> getFileAsString(String path) {
        return ResourceUtils.getFileAsStream(path)
                .map(s -> {
                    try {
                        return readFileFromStream(s);
                    } catch (IOException e) {
                        throw new IllegalArgumentException("Can't read file: " + path, e);
                    }
                });
    }

    public static String readFileFromStream(InputStream stream) throws IOException {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            return in.lines().collect(Collectors.joining("\n"));
        }
    }
}
