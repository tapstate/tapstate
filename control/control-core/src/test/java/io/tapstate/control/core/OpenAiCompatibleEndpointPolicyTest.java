package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.control.core.AiProviderSettings;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;
import java.net.Proxy;
import java.net.ProxySelector;
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
                "https://relay.example.com/v1?key=secret", "https://relay.example.com/v1#fragment", "https:///v1",
                "https://relay.example.com:0/v1", "https://relay.example.com:65536/v1", "https://relay.example.com/%zz")) {
            assertThatThrownBy(() -> OpenAiCompatibleEndpointPolicy.normalizedBaseUrl(endpoint))
                    .isInstanceOfSatisfying(TapstateException.class,
                            error -> assertThat(error.code()).isEqualTo(ControlError.MALFORMED_REQUEST))
                    .hasMessageNotContaining("secret");
        }
    }

    @Test void rejectsReservedIpv4AndIpv6RangesBeforeOpeningAProviderConnection() throws Exception {
        for (String address : List.of("0.0.0.0", "0.0.0.1", "10.0.0.1", "100.64.0.1", "127.0.0.1",
                "169.254.1.1", "172.16.0.1", "172.31.255.254", "192.0.0.1", "192.0.2.1",
                "192.88.99.1", "192.168.0.1", "198.18.0.1", "198.19.255.254", "198.51.100.1",
                "203.0.113.1", "224.0.0.1", "240.0.0.1", "::", "::1", "fc00::1", "fdff::1",
                "fe80::1", "febf::1", "ff02::1", "2001:db8::1", "2002:0a00:0001::",
                "2001:0:0:0:0:0:f5ff:fffe", "::ffff:10.0.0.1", "64:ff9b::a00:1", "64:ff9b:1::1",
                "2001:4860::5efe:a00:1")) {
            InetAddress resolved = InetAddress.getByName(address);
            assertThatThrownBy(() -> OpenAiCompatibleEndpointPolicy.requestUri("https://relay.example.com/v1",
                    host -> new InetAddress[] {resolved}))
                    .as("reserved provider address %s", address)
                    .isInstanceOfSatisfying(TapstateException.class,
                            error -> assertThat(error.code()).isEqualTo(ControlError.AI_PROVIDER_FAILED));
        }
        assertThat(OpenAiCompatibleEndpointPolicy.requestUri("https://relay.example.com/v1",
                host -> new InetAddress[] {InetAddress.getByName("2606:4700:4700::1111")}))
                .isEqualTo(URI.create("https://relay.example.com/v1/chat/completions"));
        assertThat(OpenAiCompatibleEndpointPolicy.requestUri("https://relay.example.com/v1",
                host -> new InetAddress[] {InetAddress.getByName("64:ff9b::808:808")}))
                .isEqualTo(URI.create("https://relay.example.com/v1/chat/completions"));
    }

    @Test void permitsTheFakeProxyRangeOnlyWhenAnHttpProxyWillHandleTheConnection() throws Exception {
        ProxySelector httpProxy = proxySelector(Proxy.Type.HTTP);
        ProxySelector socksProxy = proxySelector(Proxy.Type.SOCKS);
        var fakeProxyAddress = InetAddress.getByName("198.18.0.1");

        assertThat(OpenAiCompatibleEndpointPolicy.requestUri("https://relay.example.com/v1",
                host -> new InetAddress[] {fakeProxyAddress}, httpProxy))
                .isEqualTo(URI.create("https://relay.example.com/v1/chat/completions"));
        assertUnavailable("https://relay.example.com/v1", fakeProxyAddress, socksProxy);
        assertUnavailable("https://relay.example.com/v1", InetAddress.getByName("10.0.0.1"), httpProxy);
    }

    @Test void mapsResolverAndProxySelectorFailuresToProviderUnavailable() throws Exception {
        assertThatThrownBy(() -> OpenAiCompatibleEndpointPolicy.requestUri("https://relay.example.com/v1",
                host -> { throw new java.net.UnknownHostException("private diagnostic"); }))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(ControlError.AI_PROVIDER_FAILED));
        assertThatThrownBy(() -> OpenAiCompatibleEndpointPolicy.requestUri("https://relay.example.com/v1",
                host -> new InetAddress[0]))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(ControlError.AI_PROVIDER_FAILED));
        ProxySelector failingProxy = new ProxySelector() {
            @Override public List<Proxy> select(URI uri) { throw new IllegalStateException("proxy diagnostics"); }
            @Override public void connectFailed(URI uri, java.net.SocketAddress socket, java.io.IOException error) { }
        };
        assertThatThrownBy(() -> OpenAiCompatibleEndpointPolicy.requestUri("https://relay.example.com/v1",
                host -> new InetAddress[] {InetAddress.getByName("1.1.1.1")}, failingProxy))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(ControlError.AI_PROVIDER_FAILED));
        assertThatThrownBy(() -> OpenAiCompatibleEndpointPolicy.requestUri("https://127.0.0.1/v1"))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(ControlError.AI_PROVIDER_FAILED));
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

    private static void assertUnavailable(String baseUrl, InetAddress address, ProxySelector proxySelector) {
        assertThatThrownBy(() -> OpenAiCompatibleEndpointPolicy.requestUri(baseUrl,
                host -> new InetAddress[] {address}, proxySelector))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(ControlError.AI_PROVIDER_FAILED));
    }

    private static ProxySelector proxySelector(Proxy.Type type) {
        return new ProxySelector() {
            @Override public List<Proxy> select(URI uri) {
                return List.of(new Proxy(type, InetSocketAddress.createUnresolved("proxy.example.com", 8080)));
            }
            @Override public void connectFailed(URI uri, java.net.SocketAddress socket, java.io.IOException error) { }
        };
    }
}
