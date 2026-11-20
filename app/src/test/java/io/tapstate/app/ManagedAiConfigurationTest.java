package io.tapstate.app;

import io.tapstate.control.core.ControlError;
import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner;

import java.net.URI;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ManagedAiConfigurationTest {
    @Test void bedrockSignsTheRequestWithTheWorkloadSessionAndRejectsLongTermKeys() {
        var endpoint = URI.create("https://bedrock-runtime.ap-southeast-1.amazonaws.com/openai/v1/chat/completions");
        var signer = AwsV4HttpSigner.create();
        var json = "{}".getBytes(StandardCharsets.UTF_8);
        var request = HttpRequest.newBuilder(endpoint).header("Content-Type", "application/json");
        ManagedAiConfiguration.sign(() -> AwsSessionCredentials.create("session-id", "test-secret", "session-token"),
                signer, "ap-southeast-1", request, endpoint, json);
        var signed = request.POST(HttpRequest.BodyPublishers.ofByteArray(json)).build();
        assertThat(signed.headers().firstValue("Authorization")).hasValueSatisfying(value ->
                assertThat(value).startsWith("AWS4-HMAC-SHA256 Credential=session-id/")
                        .contains("/ap-southeast-1/bedrock/aws4_request").doesNotContain("test-secret"));
        assertThat(signed.headers().firstValue("X-Amz-Security-Token")).contains("session-token");
        assertThatThrownBy(() -> ManagedAiConfiguration.sign(
                () -> AwsBasicCredentials.create("test-access-key", "test-secret"),
                signer, "ap-southeast-1", HttpRequest.newBuilder(endpoint), endpoint, json))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(ControlError.AI_PROVIDER_AUTH_FAILED));
    }
}
