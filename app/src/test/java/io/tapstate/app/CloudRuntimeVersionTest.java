package io.tapstate.app;

import io.tapstate.control.core.CloudRuntimeStatusProvider;
import io.tapstate.spi.store.StorePort;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.StaticListableBeanFactory;

import java.io.InputStream;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class CloudRuntimeVersionTest {

    @Test
    void cloudStatusUsesTheBuildFilteredVersionEvenWithoutPackageManifestMetadata() throws Exception {
        assertThat(Bootstrap.class.getPackage().getImplementationVersion()).isNull();
        String expected;
        try (InputStream resource = CloudRuntimeVersionTest.class.getResourceAsStream("/tapstate-version.properties")) {
            assertThat(resource).isNotNull();
            Properties properties = new Properties();
            properties.load(resource);
            expected = properties.getProperty("version");
        }
        assertThat(expected).isNotBlank().doesNotStartWith("${");
        StaticListableBeanFactory beans = beans();

        CloudRuntimeStatusProvider provider = new CloudRuntimeConfiguration().cloudRuntimeStatusProvider(
                cloudSettings(), beans.getBeanProvider(StorePort.class), beans.getBeanProvider(Clock.class));

        assertThat(provider).isNotNull();
        assertThat(provider.snapshot().runtimeVersion()).isEqualTo(expected);
    }

    @Test
    void onPremDoesNotCreateACloudVersionProjection() {
        StaticListableBeanFactory beans = beans();

        CloudRuntimeStatusProvider provider = new CloudRuntimeConfiguration().cloudRuntimeStatusProvider(
                CloudRuntimeSettings.resolve(new CloudProperties()),
                beans.getBeanProvider(StorePort.class), beans.getBeanProvider(Clock.class));

        assertThat(provider).isNull();
    }

    private static StaticListableBeanFactory beans() {
        return new StaticListableBeanFactory(Map.of(
                "store", new InMemoryStorePort(),
                "clock", Clock.fixed(Instant.parse("2026-09-30T00:00:00Z"), ZoneOffset.UTC)));
    }

    private static CloudRuntimeSettings cloudSettings() {
        CloudProperties properties = new CloudProperties();
        properties.setBaseUrl("https://cloud.example");
        properties.setToken("fixture-static-token");
        properties.setAtlasUri("mongodb://atlas.example/metadata");
        properties.setClusterId("version-fixture-cluster");
        return CloudRuntimeSettings.resolve(properties);
    }
}
