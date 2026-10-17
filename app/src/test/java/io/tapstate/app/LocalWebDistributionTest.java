package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.SpringApplication;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LocalWebDistributionTest {
    @TempDir
    Path directory;

    private URL origin(String metadata) throws Exception {
        Path root = Files.createTempDirectory(directory, "local-web-");
        Files.createDirectories(root.resolve("io/tapstate/app"));
        Files.write(root.resolve("io/tapstate/app/WebDistribution.class"), new byte[] {0});
        Files.createDirectories(root.resolve("META-INF"));
        Files.writeString(root.resolve("META-INF/tapstate-web.properties"), metadata);
        Files.createDirectories(root.resolve("static"));
        Files.writeString(root.resolve("static/index.html"), "<!doctype html><title>Local fixture</title>");
        return root.toUri().toURL();
    }

    private void prepare(URL origin, boolean cloud, String confirmation) {
        var values = new LinkedHashMap<String, Object>();
        if (cloud) {
            values.put("tapstate.cloud.base-url", "http://control-plane:8080");
            values.put("tapstate.cloud.token", "sentinel-token");
            values.put("tapstate.cloud.atlas-uri", "mongodb://mongo:27017/metadata?replicaSet=owned");
            values.put("tapstate.cloud.cluster-id", "0123456789abcdef01234567");
        }
        if (confirmation != null) values.put("tapstate.local-development.enabled", confirmation);
        var environment = new StandardEnvironment();
        environment.getPropertySources().addFirst(new MapPropertySource("local-fixture", values));
        new WebDistributionEnvironmentPostProcessor(() -> WebDistribution.read(origin))
                .postProcessEnvironment(environment, new SpringApplication(Object.class));
    }

    private static String cloud(String url) {
        return "purpose=local-development\nprofile=cloud\nweb.profile=cloud\ncloud.console.url=" + url + "\n";
    }

    @Test
    void confirmedLocalCloudAcceptsOnlyItsPackagedLiteralLocalhostOrigin() throws Exception {
        URL owned = origin(cloud("http://localhost:39175/"));
        prepare(owned, true, "true");
        assertThat(WebDistribution.read(owned).consoleUrl()).isEqualTo("http://localhost:39175/");
    }

    @Test
    void localOnPremAlsoRequiresConfirmationBeforeAnyApplicationContext() throws Exception {
        URL owned = origin("purpose=local-development\nprofile=op\nweb.profile=onprem\n");
        prepare(owned, false, "true");
        for (String missing : new String[] {null, "false", "TRUE", " true"}) {
            assertThatThrownBy(() -> prepare(owned, false, missing))
                    .isInstanceOfSatisfying(TapstateException.class,
                            failure -> assertThat(failure.code()).isEqualTo(BootError.WEB_DEVELOPMENT_CONFIRMATION_REQUIRED));
        }
    }

    @Test
    void existingLocalOpProducerProfileRemainsRestrictedAndCompatible() throws Exception {
        URL owned = origin("purpose=local-development\nprofile=op\n");
        prepare(owned, false, "true");
        assertThatThrownBy(() -> prepare(owned, true, "true"))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(BootError.WEB_PROFILE_MODE_MISMATCH));
    }

    @Test
    void ordinaryOrRelabeledReleaseNeverUsesTheLocalHttpException() throws Exception {
        for (String purpose : new String[] {"", "purpose=release\n"}) {
            URL owned = origin(purpose + "web.profile=cloud\ncloud.console.url=http://localhost:39175/\n");
            assertThatThrownBy(() -> prepare(owned, true, "true"))
                    .isInstanceOfSatisfying(TapstateException.class,
                            failure -> assertThat(failure.code()).isEqualTo(BootError.WEB_CONSOLE_URL_INVALID));
        }
    }

    @Test
    void localCloudRefusesRemoteAddressesCredentialsQueryFragmentsAndNoncanonicalPorts() throws Exception {
        for (String url : new String[] {
                "http://127.0.0.1:39175/", "http://remote.example:39175/", "https://localhost:39175/",
                "http://localhost/", "http://localhost:0/", "http://localhost:65536/", "http://localhost:039175/",
                "http://user:sentinel-password@localhost:39175/", "http://localhost:39175/#fragment",
                "http://localhost:39175/?code=sentinel-token", "http://localhost:39175/another/path",
        }) {
            URL owned = origin(cloud(url));
            assertThatThrownBy(() -> prepare(owned, true, "true"))
                    .isInstanceOfSatisfying(TapstateException.class, failure -> {
                        assertThat(failure.code()).isEqualTo(BootError.WEB_CONSOLE_URL_INVALID);
                        assertThat(failure.getCause()).isNull();
                        assertThat(failure.getMessage()).doesNotContain("sentinel-password", "sentinel-token");
                    });
        }
    }

    @Test
    void conflictingOrUnknownLocalProfileAndPurposeCannotBorrowAnotherModesAllowance() throws Exception {
        for (String metadata : new String[] {
                "purpose=local-development\nprofile=op\nweb.profile=cloud\ncloud.console.url=http://localhost:39175/\n",
                "purpose=local-development\nprofile=cloud\nweb.profile=onprem\n",
                "purpose=unknown\nweb.profile=cloud\ncloud.console.url=https://console.example/\n",
        }) {
            URL owned = origin(metadata);
            assertThatThrownBy(() -> prepare(owned, true, "true"))
                    .isInstanceOfSatisfying(TapstateException.class,
                            failure -> assertThat(failure.code()).isEqualTo(BootError.WEB_DEVELOPMENT_ARTIFACT_INVALID));
        }
    }
}
