package io.tapstate.control.restapi;

import io.tapstate.core.model.Metadata;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ResourceMetadataJsonContractTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void onPremMetadataDoesNotGrowNullCloudFields() throws Exception {
        String encoded = JSON.writeValueAsString(new Metadata(Map.of("team", "data"), "Orders"));

        assertThat(encoded).isEqualTo(
                "{\"labels\":{\"team\":\"data\"},\"description\":\"Orders\",\"empty\":false}");
        assertThat(encoded).doesNotContain("cloud", "user_id", "userId");
    }

    @Test
    void managedMetadataUsesTheStableExternalFieldNames() throws Exception {
        String encoded = JSON.writeValueAsString(
                new Metadata(Map.of(), null, true, "cloud-user-7"));

        assertThat(encoded).isEqualTo(
                "{\"labels\":{},\"cloud\":true,\"user_id\":\"cloud-user-7\",\"empty\":false}");
        assertThat(encoded).doesNotContain("userId");
    }
}
