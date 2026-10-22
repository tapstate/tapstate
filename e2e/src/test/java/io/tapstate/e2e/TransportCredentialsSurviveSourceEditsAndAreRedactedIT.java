package io.tapstate.e2e;

import io.tapstate.core.common.JsonWriter;
import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** A saved Source keeps transport credentials across an edit while both read and test faces hide them. */
class TransportCredentialsSurviveSourceEditsAndAreRedactedIT {

    private static final String SOURCE_ID = "src_transport_credentials";
    private static final String TLS_KEY = "tls-private-key-sentinel";
    private static final String TLS_KEY_PASSWORD = "tls-key-password-sentinel";
    private static final String SSH_PASSWORD = "ssh-password-sentinel";

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void aRedactedSavedSourceRetainsTlsAndSshCredentialsAcrossAnEdit(@TempDir Path directory) throws Exception {
        Path data = Files.createDirectory(directory.resolve("data"));
        Path jar = E2eConnectorJar.buildInto(directory);
        String database = "e2e_transport_credentials_" + UUID.randomUUID().toString().replace("-", "");

        try (ServerHandle server = InProcessServer.start(SharedMongo.replicaSetUrl(database))) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID, Files.readAllBytes(jar));

            Map<String, Object> config = new LinkedHashMap<>();
            config.put("uri", data.toString());
            config.put("require_transport_credentials", true);
            config.put("sslKey", TLS_KEY);
            config.put("sslKeyPassword", TLS_KEY_PASSWORD);
            config.put("__TAPDATA", Map.of(
                    "useSSH", true,
                    "sshHost", "bastion.example",
                    "sshPort", 22,
                    "sshUsername", "reader",
                    "sshPassword", SSH_PASSWORD));

            var created = control.sourceRequest("POST", "/api/sources",
                    Map.of("id", SOURCE_ID, "connector", E2eConnectorJar.CONNECTOR_ID, "config", config),
                    null, 201);
            assertThat(created.body()).doesNotContain(TLS_KEY, TLS_KEY_PASSWORD, SSH_PASSWORD);

            Map<String, Object> saved = control.source(SOURCE_ID);
            assertTransportSecretsRedacted(saved, TLS_KEY, TLS_KEY_PASSWORD, SSH_PASSWORD);
            Map<String, Object> safeConfig = object(saved.get("config"));

            control.sourceRequest("PUT", "/api/sources/" + SOURCE_ID,
                    Map.of("id", SOURCE_ID,
                            "connector", E2eConnectorJar.CONNECTOR_ID,
                            "config", safeConfig,
                            "metadata", Map.of("description", "updated without resending secrets")),
                    control.sourceEtag(SOURCE_ID), 200);

            Map<String, Object> edited = control.source(SOURCE_ID);
            assertTransportSecretsRedacted(edited, TLS_KEY, TLS_KEY_PASSWORD, SSH_PASSWORD);
            String report = control.testConnectionBody(SOURCE_ID, E2eConnectorJar.CONNECTOR_ID,
                    object(edited.get("config")));
            assertThat(report)
                    .contains("PASSED", "transport credentials")
                    .doesNotContain(TLS_KEY, TLS_KEY_PASSWORD, SSH_PASSWORD);
        }
    }

    private static void assertTransportSecretsRedacted(
            Map<String, Object> source, String tlsKey, String tlsKeyPassword, String sshPassword) {
        String encoded = JsonWriter.write(source);
        assertThat(encoded).doesNotContain(tlsKey, tlsKeyPassword, sshPassword);
        Map<String, Object> config = object(source.get("config"));
        assertThat(config).doesNotContainKeys("sslKey", "sslKeyPassword");
        Map<String, Object> tapdata = object(config.get("__TAPDATA"));
        assertThat(tapdata).doesNotContainKey("sshPassword");
        assertThat(source.get("configuredSecrets")).asList()
                .contains("sslKey", "sslKeyPassword", "__TAPDATA.sshPassword");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> object(Object value) {
        if (!(value instanceof Map<?, ?> map)) {
            throw new AssertionError("expected a JSON object, got " + value);
        }
        return (Map<String, Object>) map;
    }
}
