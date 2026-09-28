package io.tapstate.adapters.pdk;

import io.tapdata.pdk.apis.TapConnector;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.nio.file.Path;
import java.util.List;
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
 * logging facade ({@code org.slf4j.*}). Everything else on the host — the tapstate application classes and
 * the service framework libraries — is hidden from the connector.
 */
public final class ConnectorClassLoader implements AutoCloseable {

    /**
     * The host layers a connector is allowed to see: the PDK runtime it is built against, and nothing
     * else. {@code io.tapdata.*} is the frozen contract. {@code net.sf.cglib.*} is the runtime's codegen
     * library — mapping connection config generates a {@code BeanMap} subclass and defines it into this
     * loader (the config bean lives here), so the generated subclass cannot link unless this loader
     * resolves cglib from the host. {@code org.slf4j.*} is the logging facade a thin connector and its
     * bundled driver log through and do not carry themselves. The tapstate application classes and the
     * service framework (Spring, Hazelcast, Mongo, the web container) stay hidden.
     */
    private static final List<String> SHARED_HOST_PREFIXES =
            List.of("io.tapdata.", "net.sf.cglib.", "org.slf4j.");

    private final URLClassLoader loader;

    private ConnectorClassLoader(URLClassLoader loader) {
        this.loader = loader;
    }

    /** Opens an isolated loader over {@code classpath} (the connector jar plus any bundled deps). */
    public static ConnectorClassLoader open(List<Path> classpath) {
        URL[] urls = classpath.stream().map(ConnectorClassLoader::toUrl).toArray(URL[]::new);
        ClassLoader host = ConnectorClassLoader.class.getClassLoader();
        ClassLoader sharedContract = new SharedContractClassLoader(host);
        return new ConnectorClassLoader(new ConnectorJarLoader(urls, sharedContract));
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
        try {
            loader.close();
        } catch (IOException e) {
            throw new UncheckedIOException("closing connector class loader", e);
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

        private ConnectorJarLoader(URL[] urls, ClassLoader parent) {
            super(urls, parent);
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
