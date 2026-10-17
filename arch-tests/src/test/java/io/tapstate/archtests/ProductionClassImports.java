package io.tapstate.archtests;

import com.tngtech.archunit.ArchConfiguration;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.core.importer.Location;
import com.tngtech.archunit.core.importer.Locations;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Supplier;

/** Reuses only immutable parsed inputs. Every caller still executes its own rule. */
final class ProductionClassImports {
    private static final Map<Key, JavaClasses> CACHE = new HashMap<>();

    private ProductionClassImports() { }

    static JavaClasses packages(String... packages) {
        return cachedPackages(List.of(packages), List.of(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS));
    }

    static JavaClasses cachedPackages(List<String> packages, List<ImportOption.Predefined> options) {
        List<String> scope = new TreeSet<>(packages).stream().toList();
        Set<Location> locations = new java.util.HashSet<>();
        scope.forEach(name -> locations.addAll(Locations.ofPackage(name)));
        return cached("packages", scope, locations, options, () -> importer(options).importPackages(scope));
    }

    static JavaClasses cachedLocations(Collection<Location> locations, List<ImportOption.Predefined> options) {
        return cached("locations", locationNames(locations), locations, options,
                () -> importer(options).importLocations(locations));
    }

    static JavaClasses rawPackages(List<String> packages, List<ImportOption.Predefined> options) {
        return importer(options).importPackages(packages);
    }

    static JavaClasses rawLocations(Collection<Location> locations, List<ImportOption.Predefined> options) {
        return importer(options).importLocations(locations);
    }

    private static ClassFileImporter importer(List<ImportOption.Predefined> options) {
        return new ClassFileImporter(new ArrayList<ImportOption>(options));
    }

    private static synchronized JavaClasses cached(String selector, List<String> scope,
            Collection<Location> locations, List<ImportOption.Predefined> options, Supplier<JavaClasses> parse) {
        // A custom resolver can carry invisible mutable state; preserve the original
        // import rather than claiming that a type name identifies its behavior.
        if (ArchConfiguration.get().getClassResolver().isPresent()) {
            return parse.get();
        }
        Key key;
        try {
            key = key(selector, scope, locations, options);
        } catch (IOException | IllegalArgumentException unsupportedLocation) {
            return parse.get();
        }
        return CACHE.computeIfAbsent(key, ignored -> parse.get());
    }

    static Key key(String selector, List<String> scope, Collection<Location> locations,
            List<ImportOption.Predefined> options) throws IOException {
        Set<Location> completeClasspath = new java.util.HashSet<>(Locations.inClassPath());
        completeClasspath.addAll(locations);
        String content = fingerprint(completeClasspath);
        ArchConfiguration configuration = ArchConfiguration.get();
        TreeMap<String, String> settings = new TreeMap<>();
        Properties importSettings = configuration.getSubProperties("import");
        importSettings.stringPropertyNames().forEach(name -> settings.put(name, importSettings.getProperty(name)));
        settings.put("resolveMissing", Boolean.toString(configuration.resolveMissingDependenciesFromClassPath()));
        settings.put("md5", Boolean.toString(configuration.md5InClassSourcesEnabled()));
        settings.put("resolver", configuration.getClassResolver().orElse("default"));
        settings.put("resolverArgs", configuration.getClassResolverArguments().toString());
        settings.put("javaRuntime", System.getProperty("java.runtime.version", ""));
        settings.put("javaHome", System.getProperty("java.home", ""));
        settings.put("classPathOrder", System.getProperty("java.class.path", ""));
        settings.put("testClassPathOrder", System.getProperty("surefire.test.class.path", ""));
        return new Key(selector, List.copyOf(scope), locationNames(locations),
                options.stream().map(Enum::name).distinct().sorted().toList(),
                new LoaderIdentity(Thread.currentThread().getContextClassLoader()), new LoaderIdentity(ProductionClassImports.class.getClassLoader()),
                Map.copyOf(settings), content);
    }

    private static List<String> locationNames(Collection<Location> locations) {
        return locations.stream().map(location -> location.asURI().toString()).distinct().sorted().toList();
    }

    private static String fingerprint(Collection<Location> locations) throws IOException {
        MessageDigest hash = digest();
        Set<Path> physical = new TreeSet<>();
        byte[] buffer = new byte[65536];
        for (Location location : locations.stream().sorted(java.util.Comparator.comparing(value -> value.asURI().toString())).toList()) {
            URI uri = location.asURI();
            update(hash, uri.toString());
            if ("jrt".equals(uri.getScheme())) {
                update(hash, System.getProperty("java.runtime.version", ""));
            } else if ("file".equals(uri.getScheme())) {
                physical.add(Path.of(uri).toAbsolutePath().normalize());
            } else if ("jar".equals(uri.getScheme())) {
                String nested = uri.getSchemeSpecificPart();
                int boundary = nested.indexOf("!/");
                if (boundary < 0) throw new IllegalArgumentException("Unknown class archive location");
                URI file = URI.create(nested.substring(0, boundary));
                if (!"file".equals(file.getScheme())) throw new IllegalArgumentException("Unknown class archive source");
                physical.add(Path.of(file).toAbsolutePath().normalize());
            } else {
                throw new IllegalArgumentException("Unsupported class location");
            }
        }
        for (Path root : physical) {
            update(hash, root.toString());
            if (Files.isRegularFile(root)) {
                update(hash, "file");
                hash.update(fileDigest(root, buffer));
            } else if (Files.isDirectory(root)) {
                update(hash, "directory");
                try (var paths = Files.walk(root)) {
                    List<Path> entries = paths.sorted().toList();
                    if (entries.stream().anyMatch(Files::isSymbolicLink)) {
                        throw new IllegalArgumentException("Class directory contains a symbolic link");
                    }
                    for (Path entry : entries) {
                        if (Files.isRegularFile(entry) && entry.toString().endsWith(".class")) {
                            update(hash, root.relativize(entry).toString());
                            hash.update(fileDigest(entry, buffer));
                        }
                    }
                }
            } else {
                update(hash, "absent");
            }
        }
        return HexFormat.of().formatHex(hash.digest());
    }

    private static byte[] fileDigest(Path path, byte[] buffer) throws IOException {
        MessageDigest digest = digest();
        try (var input = Files.newInputStream(path)) {
            int count;
            while ((count = input.read(buffer)) >= 0) {
                if (count > 0) digest.update(buffer, 0, count);
            }
        }
        return digest.digest();
    }

    static String graphSourceDigest(Map<String, String> sources) {
        MessageDigest digest = digest();
        new TreeMap<>(sources).forEach((name, source) -> { update(digest, name); update(digest, source); });
        return HexFormat.of().formatHex(digest.digest());
    }

    private static void update(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(java.nio.ByteBuffer.allocate(4).putInt(bytes.length).array());
        digest.update(bytes);
    }

    private static MessageDigest digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    static Map<String, String> graphSources(JavaClasses classes) {
        Map<String, String> sources = new TreeMap<>();
        classes.forEach(type -> sources.put(type.getName(), type.getSource()
                .map(source -> source.getUri().toString()).orElse("unresolved")));
        return Map.copyOf(sources);
    }

    record Key(String selector, List<String> scope, List<String> locations, List<String> options,
               LoaderIdentity contextLoader, LoaderIdentity helperLoader, Map<String, String> settings, String contentSha256) { }

    static final class LoaderIdentity {
        private final ClassLoader loader;

        LoaderIdentity(ClassLoader loader) { this.loader = loader; }

        @Override public boolean equals(Object other) {
            return other instanceof LoaderIdentity value && loader == value.loader;
        }

        @Override public int hashCode() { return System.identityHashCode(loader); }
    }
}
