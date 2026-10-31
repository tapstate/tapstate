package io.tapstate.adapters.alerting;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/** A test-owned ephemeral certificate, never an application/runtime TLS identity. */
final class TlsTestMaterial {
    private static TlsTestMaterial cached;
    final SSLContext server;
    final SSLContext client;
    private TlsTestMaterial(SSLContext server, SSLContext client) { this.server = server; this.client = client; }
    static synchronized TlsTestMaterial load(Path directory) throws Exception {
        if (cached != null) { return cached; }
        Path store = directory.resolve("notification-test.p12");
        Path output = directory.resolve("keytool.log");
        String keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
        Process process = new ProcessBuilder(keytool, "-genkeypair", "-alias", "fixture", "-keyalg", "RSA", "-keysize", "2048",
                "-validity", "2", "-dname", "CN=localhost", "-ext", "SAN=dns:localhost", "-storetype", "PKCS12",
                "-keystore", store.toString(), "-storepass", "fixture-only", "-keypass", "fixture-only", "-noprompt")
                .redirectErrorStream(true).redirectOutput(output.toFile()).start();
        if (!process.waitFor(10, TimeUnit.SECONDS)) { process.destroyForcibly(); throw new AssertionError("fixture keytool timed out"); }
        if (process.exitValue() != 0) { throw new AssertionError(Files.readString(output)); }
        KeyStore keys = KeyStore.getInstance("PKCS12");
        try (var in = Files.newInputStream(store)) { keys.load(in, "fixture-only".toCharArray()); }
        KeyManagerFactory key = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        key.init(keys, "fixture-only".toCharArray());
        KeyStore trust = KeyStore.getInstance("PKCS12"); trust.load(null, null);
        trust.setCertificateEntry("fixture", keys.getCertificate("fixture"));
        TrustManagerFactory manager = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm()); manager.init(trust);
        SSLContext server = SSLContext.getInstance("TLS"); server.init(key.getKeyManagers(), manager.getTrustManagers(), null);
        SSLContext client = SSLContext.getInstance("TLS"); client.init(null, manager.getTrustManagers(), null);
        cached = new TlsTestMaterial(server, client);
        return cached;
    }
}
