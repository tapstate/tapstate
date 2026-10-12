package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CloudRuntimeSettingsTest {

    @Test
    void noCloudKeysPreserveOnPremAndItsOrdinaryMongoUri() {
        CloudRuntimeSettings settings = CloudRuntimeSettings.resolve(new CloudProperties());

        assertThat(settings.mode()).isEqualTo(CloudRuntimeSettings.Mode.ON_PREM);
        assertThat(settings.metadataUri("mongodb://localhost:27017/tapstate"))
                .isEqualTo("mongodb://localhost:27017/tapstate");
        assertThat(settings.operatorStateDatabase("custom_operator")).isEqualTo("custom_operator");
        assertThat(settings.viewsDatabase("custom_views")).isEqualTo("custom_views");
    }

    @Test
    void theCompleteSetSelectsCloudAndItsConfiguredIdentityAndAtlasMetadataUri() {
        CloudProperties properties = complete();
        properties.setBaseUrl("https://cloud.tapstate.io/");

        CloudRuntimeSettings settings = CloudRuntimeSettings.resolve(properties);

        assertThat(settings.mode()).isEqualTo(CloudRuntimeSettings.Mode.CLOUD);
        assertThat(settings.baseUrl()).hasToString("https://cloud.tapstate.io");
        assertThat(settings.clusterId()).isEqualTo("configured-cluster");
        assertThat(settings.metadataUri("mongodb://localhost:27017/tapstate"))
                .isEqualTo("mongodb+srv://cluster.example/tapstate");
        assertThat(settings.operatorStateDatabase("onprem_operator")).isEqualTo("tapstate_operator");
        assertThat(settings.viewsDatabase("views")).isEqualTo("tapstate_views");
        assertThat(settings.toString()).doesNotContain("secret-token", "cluster.example");
    }

    @Test
    void aPartialSetFailsBeforeOpeningEitherStoreOrCloud() {
        CloudProperties properties = new CloudProperties();
        properties.setBaseUrl("https://cloud.tapstate.io");
        properties.setToken("secret-token");

        assertThatThrownBy(() -> CloudRuntimeSettings.resolve(properties))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(BootError.CLOUD_CONFIG_INCOMPLETE));
    }

    @Test
    void aNonHttpControlPlaneAddressIsRejectedWithoutEchoingIt() {
        CloudProperties properties = complete();
        properties.setBaseUrl("file:///tmp/cloud");

        assertThatThrownBy(() -> CloudRuntimeSettings.resolve(properties))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> {
                            assertThat(error.code()).isEqualTo(BootError.CLOUD_BASE_URL_INVALID);
                            assertThat(error.args()).isEmpty();
                            assertThat(error.getCause()).isNull();
                        });
    }

    @Test
    void aBaseUrlWithUserInfoIsRejectedWithoutRetainingTheSecret() {
        CloudProperties properties = complete();
        properties.setBaseUrl("https://user:sentinel@cloud.tapstate.io");

        assertThatThrownBy(() -> CloudRuntimeSettings.resolve(properties))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(BootError.CLOUD_BASE_URL_INVALID);
                    assertThat(error.toString()).doesNotContain("sentinel");
                    assertThat(error.getCause()).isNull();
                });
    }

    @Test
    void aNonMongoMetadataUriIsRejectedWithoutEchoingIt() {
        CloudProperties properties = complete();
        properties.setAtlasUri("https://user:password@example.invalid/metadata");

        assertThatThrownBy(() -> CloudRuntimeSettings.resolve(properties))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(BootError.CLOUD_ATLAS_URI_INVALID));
    }

    @Test
    void cloudMetadataRequiresAnExplicitDatabaseForItsDerivedStores() {
        CloudProperties properties = complete();
        properties.setAtlasUri("mongodb+srv://cluster.example");

        assertThatThrownBy(() -> CloudRuntimeSettings.resolve(properties))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(BootError.CLOUD_ATLAS_URI_INVALID);
                    assertThat(error.args()).isEmpty();
                    assertThat(error.getCause()).isNull();
                });
    }

    private static CloudProperties complete() {
        CloudProperties properties = new CloudProperties();
        properties.setBaseUrl("https://cloud.tapstate.io");
        properties.setToken("secret-token");
        properties.setAtlasUri("mongodb+srv://cluster.example/tapstate");
        properties.setClusterId("configured-cluster");
        return properties;
    }
}
