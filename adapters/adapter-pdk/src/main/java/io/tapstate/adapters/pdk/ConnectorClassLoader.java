package io.tapstate.adapters.pdk;

import io.tapdata.pdk.apis.TapConnector;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A live, isolated class loader for one connector.
 *
 * <p>Each connector runs on its own class loader over its own jar (plus any bundled dependencies),
 * so two connectors never see each other's classes and a connector can be dropped by closing its
 * loader. What is shared across the boundary is the PDK runtime, delegated to the host: the frozen
 * contract ({@code io.tapdata.*}), so every connector binds to the same {@code TapConnector} /
 * {@code TapEvent} types and events cross as one contract; and the runtime's own infrastructure a
 * connector links against but does not bundle — bytecode generation ({@code net.sf.cglib.*}) and the
 * logging facade ({@code org.slf4j.*}). Log4j is shared only when the connector classpath does not
 * bundle its own API; a bundled Log4j stays entirely isolated. Everything else on the host —
 * the tapstate application classes and the service framework libraries — is hidden from the connector.
 *
 * <p>Introspection opens a private loader. Connector handles lease a shared loader. Loaders for
 * connectors with process-wide native state stay available for the life of the process.
 */
public final class ConnectorClassLoader implements AutoCloseable {

    /**
     * The host layers a connector is allowed to see: the PDK runtime it is built against, and nothing
     * else. {@code io.tapdata.*} is the frozen contract. {@code net.sf.cglib.*} is the runtime's codegen
     * library — mapping connection config generates a {@code BeanMap} subclass and defines it into this
     * loader (the config bean lives here), so the generated subclass cannot link unless this loader
     * resolves cglib from the host. {@code org.slf4j.*} is the logging facade a thin connector and its
     * bundled driver log through and do not carry themselves. Log4j is handled separately by the
     * connector loader so its API and implementation cannot split across the host and connector. The tapstate
     * application classes and the service framework (Spring, Hazelcast, Mongo, the web container) stay hidden.
     */
    private static final List<String> SHARED_HOST_PREFIXES =
            List.of("io.tapdata.", "net.sf.cglib.", "org.slf4j.");

    /** Shared loaders and their use counts are guarded by this map. */
    private static final Map<ArtifactKey, ConnectorClassLoader> SHARED = new HashMap<>();

    private final URLClassLoader loader;
    private final boolean sharedLoader;
    private int users;
    private boolean pinned;

    private ConnectorClassLoader(URLClassLoader loader, boolean sharedLoader) {
        this.loader = loader;
        this.sharedLoader = sharedLoader;
    }

    /** Opens an isolated loader over {@code classpath} (the connector jar plus any bundled deps). */
    public static ConnectorClassLoader open(List<Path> classpath) {
        return new ConnectorClassLoader(newLoader(classpath), false);
    }

    /** A shared loader borrowed without a release handle stays pinned for this JVM. */
    public static ConnectorClassLoader shared(List<Path> classpath) {
        synchronized (SHARED) {
            var loader = sharedLoader(classpath);
            loader.pinned = true;
            return loader;
        }
    }

    static SharedUse acquire(List<Path> classpath) {
        synchronized (SHARED) {
            var loader = sharedLoader(classpath);
            loader.users++;
            return new SharedUse(loader);
        }
    }

    private static ConnectorClassLoader sharedLoader(List<Path> classpath) {
        return SHARED.computeIfAbsent(ArtifactKey.of(classpath),
                key -> new ConnectorClassLoader(newLoader(classpath), true));
    }

    /** Retires idle matching loaders before the owner makes a local artifact reclaimable. */
    static boolean retireUnused(Path jar) {
        var path = jar.toAbsolutePath().normalize();
        synchronized (SHARED) {
            var keys = SHARED.keySet().stream()
                    .filter(key -> key.entries().stream().anyMatch(entry -> entry.path().equals(path)))
                    .toList();
            if (keys.isEmpty()) return true;
            if (keys.stream().map(SHARED::get).anyMatch(loader -> loader.pinned || loader.users != 0)) return false;
            var threads = platformThreads();
            if (keys.stream().map(SHARED::get).anyMatch(loader -> usedBy(threads, loader.loader))) return false;
            for (var key : keys) {
                var loader = SHARED.get(key);
                try {
                    loader.closeLoader();
                } catch (RuntimeException | Error failure) {
                    loader.pinned = true;
                    throw failure;
                }
                SHARED.remove(key);
            }
            return true;
        }
    }

    /** Platform workers inherit the connector context loader and may finish after a handle closes. */
    private static boolean usedBy(Thread[] threads, ClassLoader loader) {
        for (var thread : threads) {
            if (thread == null || !thread.isAlive()) continue;
            for (var context = thread.getContextClassLoader(); context != null; context = context.getParent()) {
                if (context == loader) return true;
            }
        }
        return false;
    }

    private static Thread[] platformThreads() {
        var group = Thread.currentThread().getThreadGroup();
        while (group.getParent() != null) group = group.getParent();
        int size = Math.max(16, group.activeCount());
        while (true) {
            var threads = new Thread[size];
            if (group.enumerate(threads, true) < threads.length) return threads;
            size = Math.multiplyExact(size, 2);
        }
    }

    static final class SharedUse implements AutoCloseable {
        private final ConnectorClassLoader loader;
        private boolean closed;

        private SharedUse(ConnectorClassLoader loader) { this.loader = loader; }

        ConnectorClassLoader loader() { return loader; }

        void keep() {
            synchronized (SHARED) { loader.pinned = true; }
        }

        @Override public void close() {
            synchronized (SHARED) {
                if (closed) return;
                closed = true;
                loader.users--;
            }
        }
    }

    private static URLClassLoader newLoader(List<Path> classpath) {
        URL[] urls = classpath.stream().map(ConnectorClassLoader::toUrl).toArray(URL[]::new);
        ClassLoader host = ConnectorClassLoader.class.getClassLoader();
        ClassLoader sharedContract = new SharedContractClassLoader(host);
        return new ConnectorJarLoader(urls, sharedContract, host);
    }

    /** Loads {@code className} in isolation (from the connector jar or the shared PDK contract). */
    public Class<?> load(String className) throws ClassNotFoundException {
        return loader.loadClass(className);
    }

    /**
     * Loads {@code className} and confirms it is a connector entry class. Only checks the type; it
     * does not instantiate or initialize the connector.
     */
    public Class<? extends TapConnector> loadConnectorClass(String className)
            throws ClassNotFoundException {
        Class<?> loaded = load(className);
        if (!TapConnector.class.isAssignableFrom(loaded)) {
            throw new IllegalStateException(
                    className + " is not a " + TapConnector.class.getName());
        }
        return loaded.asSubclass(TapConnector.class);
    }

    @Override
    public void close() {
        if (!sharedLoader) closeLoader();
    }

    private void closeLoader() {
        try {
            loader.close();
        } catch (IOException e) {
            throw new UncheckedIOException("closing connector class loader", e);
        }
    }

    /** One classpath entry as it stands on disk: where it is, and enough to notice it was replaced. */
    private record ArtifactEntry(Path path, long size, long modifiedMillis) {

        static ArtifactEntry of(Path jar) {
            Path path = jar.toAbsolutePath().normalize();
            try {
                return new ArtifactEntry(path, Files.size(path), Files.getLastModifiedTime(path).toMillis());
            } catch (IOException unreadable) {
                // Nothing on disk to compare: the path alone, and the loader reports the missing jar itself.
                return new ArtifactEntry(path, -1, -1);
            }
        }
    }

    /** The identity a shared loader is kept under: the whole classpath, in order. */
    private record ArtifactKey(List<ArtifactEntry> entries) {

        static ArtifactKey of(List<Path> classpath) {
            return new ArtifactKey(classpath.stream().map(ArtifactEntry::of).toList());
        }
    }

    private static URL toUrl(Path jar) {
        try {
            return jar.toUri().toURL();
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("bad connector jar path " + jar, e);
        }
    }

    /**
     * A connector's jar, whose close reaches no read but the loader's own.
     *
     * <p>A resource read through its URL - the way a resource bundle loads its text - goes through one cached
     * jar file per jar path, shared by every reader in the process. {@link URLClassLoader#getResourceAsStream}
     * reads through that same shared file and records it as the loader's own, so {@link URLClassLoader#close()}
     * closes it under every other reader. Two connectors over the same jar - the two sources of one pipeline -
     * then meet: closing the first ends a read the second is part way through with "Stream closed". A driver
     * reading its messages in a static initializer fails that initializer, and a class whose initializer
     * failed stays unusable in its loader, so the connector cannot connect at all. Read uncached here, each of
     * this loader's streams opens the jar for itself and closes it with the stream; the loader records nothing
     * it does not own, and the shared file stays open for the readers sharing it.
     */
    private static final class ConnectorJarLoader extends URLClassLoader {

        static {
            // As its superclass is: without this a subclass loads one class at a time per loader.
            ClassLoader.registerAsParallelCapable();
        }

        private final ClassLoader host;
        private final boolean bundledLog4j;

        private ConnectorJarLoader(URL[] urls, ClassLoader parent, ClassLoader host) {
            super(urls, parent);
            this.host = host;
            bundledLog4j = findResource("org/apache/logging/log4j/LogManager.class") != null;
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (!bundledLog4j && name.startsWith("org.apache.logging.log4j.")) {
                // Thin connectors use the host facade as a whole; never mix in connector fallbacks.
                return host.loadClass(name);
            }
            return super.loadClass(name, resolve);
        }

        @Override
        public InputStream getResourceAsStream(String name) {
            URL url = getResource(Objects.requireNonNull(name, "name"));
            if (url == null) {
                return null;
            }
            try {
                URLConnection connection = url.openConnection();
                connection.setUseCaches(false);
                return connection.getInputStream();
            } catch (IOException unreadable) {
                // The superclass's answer to a resource it found but could not open: there is none to read.
                return null;
            }
        }
    }

    /**
     * The parent a connector loader delegates to: it exposes only the shared PDK contract from the
     * host and hides everything else. Its own parent is the platform loader, so {@code java.*} /
     * {@code javax.*} still resolve; any non-contract host class (tapstate, Spring, Hazelcast, Mongo)
     * is not found, so the connector cannot bind to it.
     */
    private static final class SharedContractClassLoader extends ClassLoader {

        private final ClassLoader host;

        SharedContractClassLoader(ClassLoader host) {
            super(ClassLoader.getPlatformClassLoader());
            this.host = host;
        }

        @Override
        protected Class<?> findClass(String name) throws ClassNotFoundException {
            for (String prefix : SHARED_HOST_PREFIXES) {
                if (name.startsWith(prefix)) {
                    return host.loadClass(name);
                }
            }
            throw new ClassNotFoundException(name);
        }
    }
}
