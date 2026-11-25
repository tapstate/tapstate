package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.StorageSetupScope;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import javax.net.ssl.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.KeyStore;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;

/** Native configuration and cache trust use separate real TLS endpoints, including a new JVM. */
@RequiresDocker
class MongoNativeTlsIsolationIT {
    private static final String PASSWORD = "fixture-key-password";
    private static final Trust CONFIG_TRUST = Trust.create("localhost", false);
    private static final Trust CACHE_TRUST = Trust.create("localhost", false);
    @Container private static final GenericContainer<?> CONFIG = mongo(CONFIG_TRUST);
    @Container private static final GenericContainer<?> CACHE = mongo(CACHE_TRUST);
    @TempDir Path work;

    private static GenericContainer<?> mongo(Trust trust) {
        return new GenericContainer<>(DockerImageName.parse("mongo:7.0")).withExposedPorts(27017)
                .withCopyToContainer(Transferable.of(trust.server(), 0400), "/data/configdb/server.pem")
                .withCopyToContainer(Transferable.of(trust.ca().getBytes(StandardCharsets.UTF_8), 0444), "/data/configdb/ca.pem")
                .withCommand("--replSet", "tls-rs", "--bind_ip_all", "--tlsMode", "requireTLS",
                        "--tlsCertificateKeyFile", "/data/configdb/server.pem", "--tlsCAFile", "/data/configdb/ca.pem",
                        "--tlsAllowConnectionsWithoutCertificates")
                .waitingFor(Wait.forLogMessage("(?i).*waiting for connections.*", 1))
                .withStartupTimeout(Duration.ofSeconds(60));
    }

    @BeforeAll static void initializeReplicas() throws Exception {
        for (var container : List.of(CONFIG, CACHE)) {
            var result = container.execInContainer("mongosh", "--quiet", "--tls", "--tlsCAFile", "/data/configdb/ca.pem",
                    "--host", "localhost", "--eval", "rs.initiate({_id:'tls-rs',members:[{_id:0,host:'localhost:27017'}]}); for(let n=0;n<100;n++){if(db.hello().isWritablePrimary)quit(0);sleep(100)};quit(2)");
            assertThat(result.getExitCode()).as("the test-owned TLS replica elects its primary").isZero();
        }
    }

    @Test void separateTrustSurvivesEncryptedBindingReloadAndANewProcessWithoutGlobalTrustChanges() throws Exception {
        SSLContext global = SSLContext.getDefault();
        Path configCa = work.resolve("config-ca.pem"); Files.writeString(configCa, CONFIG_TRUST.ca());
        var settings = settings(uri(CONFIG, "tls_config"), configCa);
        var endpoint = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, uri(CACHE, "cache"), "cache", CACHE_TRUST.ca());
        String incarnation;
        try (var configuration = MongoConfigurationBootstrap.prepare(settings, "tls-cluster", "installation", "setup", 1)) {
            var scope = new StorageSetupScope("tls-cluster", "installation", "setup", 1);
            var source = new SourceResource("views", null, "mongodb", Map.of("uri", "mongodb://master.invalid/views"), null, null, null, null);
            var committed = configuration.bindings().commitWithFirstCache(scope, StorageBinding.Mode.OP, endpoint, source);
            incarnation = committed.preparation().authScope().cacheIncarnation();
            try (var cache = MongoCacheDomain.connect(endpoint, Duration.ofSeconds(3), "tls-first")) {
                cache.prepareFirstAuth(committed.firstCreation().orElseThrow());
                cache.prepareInitialBusiness(committed.preparation(), () -> {});
                cache.verifyBusinessReady(committed.preparation().businessScope());
            }
            configuration.bindings().complete(scope);
        }
        try (var reopened = MongoConfigurationDomain.openExisting(settings)) {
            var saved = reopened.bindings().load().orElseThrow();
            assertThat(saved.cache().caPem()).isEqualTo(CACHE_TRUST.ca()).isNotEqualTo(CONFIG_TRUST.ca());
            try (var cache = MongoCacheDomain.connect(saved.cache(), Duration.ofSeconds(3), "tls-reopened")) {
                cache.verifyBusinessReady(reopened.bindings().cachePreparation().orElseThrow().businessScope());
            }
        }
        Path output = work.resolve("reopened.log");
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        var process = new ProcessBuilder(javaTool("java"), "-Xmx128m", "-cp", classpath,
                MongoNativeTlsReopenProcess.class.getName(), settings.uri(), configCa.toString())
                .redirectErrorStream(true).redirectOutput(output.toFile()).start();
        try {
            assertThat(process.waitFor(30, TimeUnit.SECONDS)).as("the new JVM completed its bounded TLS reopen").isTrue();
            assertThat(process.exitValue()).withFailMessage(Files.readString(output)).isZero();
            assertThat(Files.readString(output)).contains("TLS_REOPEN_OK " + incarnation);
        } finally { if (process.isAlive()) process.destroyForcibly(); }
        assertThat(SSLContext.getDefault()).isSameAs(global);
    }

    @Test void crossedAuthoritiesAndPlaintextAreRejectedByTlsRequiredMongoServers() throws Exception {
        Path configCa = work.resolve("config.pem"), cacheCa = work.resolve("cache.pem");
        Files.writeString(configCa, CONFIG_TRUST.ca()); Files.writeString(cacheCa, CACHE_TRUST.ca());
        for (var connection : List.of(settings(uri(CONFIG, "tls_config"), cacheCa), settings(uri(CACHE, "cache"), configCa),
                settings(uri(CONFIG, "tls_config").replace("tls=true", "tls=false"), null))) {
            try (var client = MongoConnection.forConfiguration(connection)) {
                assertThatThrownBy(client::verifyConnectivity).isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(StoreError.UNREACHABLE));
            }
        }
        // Failed candidates do not change the other connection's trust or disable hostname verification.
        try (var first = MongoConnection.forConfiguration(settings(uri(CONFIG, "tls_config"), configCa));
             var second = MongoConnection.forConfiguration(settings(uri(CACHE, "cache"), cacheCa))) {
            first.verifyConnectivity(); second.verifyConnectivity();
        }
    }

    @ParameterizedTest @ValueSource(strings = {"unknown-ca", "expired", "hostname", "client-certificate-required"})
    void invalidServerTrustOrRequiredClientIdentityCannotCompleteTheTlsHandshake(String fault) throws Exception {
        Trust trust = Trust.create(fault.equals("hostname") ? "wrong-host.invalid" : "localhost", fault.equals("expired"));
        Path ca = work.resolve("candidate.pem"); Files.writeString(ca, fault.equals("unknown-ca") ? CONFIG_TRUST.ca() : trust.ca());
        var factory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm()); factory.init(trust.keys(), PASSWORD.toCharArray());
        var serverContext = SSLContext.getInstance("TLS"); serverContext.init(factory.getKeyManagers(), null, null);
        var server = (SSLServerSocket) serverContext.getServerSocketFactory().createServerSocket(0, 10, java.net.InetAddress.getLoopbackAddress());
        server.setNeedClientAuth(fault.equals("client-certificate-required"));
        var attempts = new AtomicInteger(); var completed = new AtomicInteger();
        var worker = Executors.newSingleThreadExecutor();
        var serving = worker.submit(() -> {
            while (!server.isClosed()) {
                try (var peer = (SSLSocket) server.accept()) {
                    attempts.incrementAndGet(); peer.setSoTimeout(3000); peer.startHandshake(); completed.incrementAndGet();
                } catch (IOException refused) { /* Expected failed handshakes; no application protocol is served. */ }
            }
        });
        try (var client = MongoConnection.forConfiguration(settings("mongodb://localhost:" + server.getLocalPort() + "/negative?tls=true&directConnection=true", ca))) {
            assertThatThrownBy(client::verifyConnectivity).isInstanceOf(TapstateException.class);
        } finally { server.close(); serving.get(5, TimeUnit.SECONDS); worker.shutdownNow(); }
        assertThat(attempts).hasPositiveValue();
        // This assertion distinguishes a rejected handshake from a later missing Mongo protocol response.
        assertThat(completed).hasValue(0);
    }

    private static MongoConnectionSettings settings(String uri, Path ca) {
        return new MongoConnectionSettings(uri, ca == null ? null : ca.toString(), Duration.ofSeconds(2));
    }
    private static String uri(GenericContainer<?> container, String database) {
        return "mongodb://" + container.getHost() + ":" + container.getMappedPort(27017) + "/" + database + "?replicaSet=tls-rs&directConnection=true&tls=true";
    }
    private static String javaTool(String tool) { return Path.of(System.getProperty("java.home"), "bin", tool).toString(); }

    /** Ephemeral fixture certificates never read, replace or trust an operator's retained TLS material. */
    private record Trust(String ca, byte[] server, KeyStore keys) {
        static Trust create(String hostname, boolean expired) {
            Path dir = null;
            try {
                dir = Files.createTempDirectory("native-tls-fixture-");
                Path caStore = dir.resolve("ca.p12"), leafStore = dir.resolve("leaf.p12"), request = dir.resolve("leaf.csr"), reply = dir.resolve("leaf.pem"), ca = dir.resolve("ca.pem");
                keytool(dir, "-genkeypair", "-alias", "ca", "-keystore", caStore.toString(), "-dname", "CN=Fixture CA", "-ext", "BC=ca:true", "-validity", "365");
                keytool(dir, "-exportcert", "-rfc", "-alias", "ca", "-keystore", caStore.toString(), "-file", ca.toString());
                keytool(dir, "-genkeypair", "-alias", "server", "-keystore", leafStore.toString(), "-dname", "CN=" + hostname, "-validity", "30");
                keytool(dir, "-certreq", "-alias", "server", "-keystore", leafStore.toString(), "-file", request.toString());
                String san = "SAN=dns:" + hostname + (hostname.equals("localhost") ? ",ip:127.0.0.1" : "");
                keytool(dir, "-gencert", "-rfc", "-alias", "ca", "-keystore", caStore.toString(), "-infile", request.toString(), "-outfile", reply.toString(),
                        "-ext", san, "-ext", "EKU=serverAuth", "-ext", "KU=digitalSignature,keyEncipherment", "-startdate", expired ? "-4d" : "-1d", "-validity", expired ? "1" : "30");
                keytool(dir, "-importcert", "-alias", "ca", "-keystore", leafStore.toString(), "-file", ca.toString());
                keytool(dir, "-importcert", "-alias", "server", "-keystore", leafStore.toString(), "-file", reply.toString());
                var keys = KeyStore.getInstance("PKCS12"); try (var input = Files.newInputStream(leafStore)) { keys.load(input, PASSWORD.toCharArray()); }
                StringBuilder server = new StringBuilder(pem("PRIVATE KEY", keys.getKey("server", PASSWORD.toCharArray()).getEncoded()));
                for (var certificate : keys.getCertificateChain("server")) server.append(pem("CERTIFICATE", certificate.getEncoded()));
                return new Trust(Files.readString(ca), server.toString().getBytes(StandardCharsets.UTF_8), keys);
            } catch (Exception failure) { throw new IllegalStateException("cannot create owned TLS test material", failure); }
            finally {
                if (dir != null) try (var files = Files.list(dir)) {
                    for (Path file : files.toList()) Files.delete(file);
                    Files.delete(dir);
                } catch (IOException cleanup) { throw new IllegalStateException("cannot remove owned TLS test material", cleanup); }
            }
        }
        private static void keytool(Path dir, String... arguments) throws Exception {
            var command = new ArrayList<String>(); command.add(javaTool("keytool")); command.addAll(List.of(arguments));
            command.addAll(List.of("-storepass", PASSWORD, "-noprompt"));
            if (List.of(arguments).contains("-genkeypair")) command.addAll(List.of("-keyalg", "RSA", "-keysize", "2048", "-storetype", "PKCS12"));
            var process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(dir.resolve("keytool.log").toFile()).start();
            if (!process.waitFor(30, TimeUnit.SECONDS)) { process.destroyForcibly(); throw new AssertionError("fixture keytool timed out"); }
            if (process.exitValue() != 0) throw new AssertionError("fixture keytool failed: " + Files.readString(dir.resolve("keytool.log")));
        }
        private static String pem(String type, byte[] bytes) {
            return "-----BEGIN " + type + "-----\n" + Base64.getMimeEncoder(64, new byte[]{'\n'}).encodeToString(bytes) + "\n-----END " + type + "-----\n";
        }
    }
}
