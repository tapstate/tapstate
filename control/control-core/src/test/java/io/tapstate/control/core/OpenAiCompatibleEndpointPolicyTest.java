package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import java.net.InetAddress;
import java.net.URI;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class OpenAiCompatibleEndpointPolicyTest {
    @Test void acceptsUserConfiguredRelaysWithoutAProviderHostAllowlist() {
        assertThat(AiProviderService.validated(new AiProviderSettings(
                "openai-compatible", "https://api.super-nb.me", "model", "sk-test")).baseUrl())
                .isEqualTo("https://api.super-nb.me/v1");
        assertThat(AiProviderService.validated(new AiProviderSettings(
                "openai-compatible", "https://relay.example.com:8443/openai/v1/", "model", "sk-test")).baseUrl())
                .isEqualTo("https://relay.example.com:8443/openai/v1");
        assertThat(AiProviderService.validated(new AiProviderSettings(
                "openai-compatible", "https://198.18.0.230/v1", "model", "sk-test")).baseUrl())
                .isEqualTo("https://198.18.0.230/v1");
    }

    @Test void buildsChatEndpointForRootAndVersionedBaseUrls() throws Exception {
        assertThat(requestUri("https://api.super-nb.me"))
                .isEqualTo(URI.create("https://api.super-nb.me/v1/chat/completions"));
        assertThat(requestUri("https://relay.example.com/openai/v1/"))
                .isEqualTo(URI.create("https://relay.example.com/openai/v1/chat/completions"));
        assertThat(requestUri("https://relay.example.com/v1/chat/completions/"))
                .isEqualTo(URI.create("https://relay.example.com/v1/chat/completions"));
    }

    @Test void allowsPublicRelaysAndRejectsPrivateOrMixedDnsAnswersBeforeSendingCredentials() throws Exception {
        var publicAddress = InetAddress.getByAddress(new byte[] {1, 1, 1, 1});
        var privateAddress = InetAddress.getByAddress(new byte[] {10, 0, 0, 8});
        assertThat(OpenAiCompatibleEndpointPolicy.requestUri("https://relay.example.com/v1",
                host -> new InetAddress[] {publicAddress}))
                .isEqualTo(URI.create("https://relay.example.com/v1/chat/completions"));
        for (InetAddress[] answers : List.of(new InetAddress[] {privateAddress},
                new InetAddress[] {publicAddress, InetAddress.getByAddress(new byte[] {(byte) 169, (byte) 254, (byte) 169, (byte) 254})})) {
            assertThatThrownBy(() -> OpenAiCompatibleEndpointPolicy.requestUri("https://relay.example.com/v1",
                    host -> answers))
                    .isInstanceOfSatisfying(TapstateException.class,
                            error -> assertThat(error.code()).isEqualTo(ControlError.AI_PROVIDER_FAILED));
        }
    }

    @Test void rejectsUrlShapesThatCouldLeakCredentialsOrSecrets() {
        for (String endpoint : java.util.List.of("http://relay.example.com/v1", "https://user:pass@relay.example.com/v1",
                "https://relay.example.com/v1?key=secret", "https://relay.example.com/v1#fragment", "https:///v1")) {
            assertThatThrownBy(() -> OpenAiCompatibleEndpointPolicy.normalizedBaseUrl(endpoint))
                    .isInstanceOfSatisfying(TapstateException.class,
                            error -> assertThat(error.code()).isEqualTo(ControlError.MALFORMED_REQUEST))
                    .hasMessageNotContaining("secret");
        }
    }

    @Test void rejectsAProviderBaseUrlThatIsNotAnHttpsUrl() {
        assertThatThrownBy(() -> AiProviderService.validated(new AiProviderSettings(
                "openai-compatible", "http://relay.example.com/v1", "model", "sk-test")))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(ControlError.MALFORMED_REQUEST))
                .hasMessageNotContaining("sk-test");
    }

    private static URI requestUri(String value) throws Exception {
        InetAddress publicAddress = InetAddress.getByAddress(new byte[] {1, 1, 1, 1});
        return OpenAiCompatibleEndpointPolicy.requestUri(value, host -> new InetAddress[] {publicAddress});
    }
}
