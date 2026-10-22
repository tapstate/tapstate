package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.URI;
import java.net.ProxySelector;
import java.net.UnknownHostException;
import java.util.Map;

/** Validates the user-configured HTTPS base URL for an OpenAI-compatible API. */
final class OpenAiCompatibleEndpointPolicy {
    private static final String DEFAULT_BASE_URL = "https://api.openai.com/v1";

    private OpenAiCompatibleEndpointPolicy() { }

    static String normalizedBaseUrl(String baseUrl) {
        String value = baseUrl == null || baseUrl.isBlank() ? DEFAULT_BASE_URL : baseUrl.trim();
        value = value.replaceAll("/+$", "");
        URI uri;
        try {
            uri = URI.create(value);
        } catch (IllegalArgumentException invalid) {
            throw malformed();
        }
        if (!"https".equalsIgnoreCase(uri.getScheme()) || !uri.isAbsolute()
                || uri.getHost() == null || uri.getRawAuthority() == null
                || uri.getRawUserInfo() != null || uri.getRawQuery() != null
                || uri.getRawFragment() != null || uri.getPort() == 0 || uri.getPort() > 65535) {
            throw malformed();
        }
        String path = uri.getRawPath() == null ? "" : uri.getRawPath().replaceAll("/+$", "");
        if (path.isEmpty()) path = "/v1";
        return "https://" + uri.getRawAuthority() + path;
    }

    static URI requestUri(String baseUrl) {
        return requestUri(baseUrl, InetAddress::getAllByName, ProxySelector.getDefault());
    }

    static URI requestUri(String baseUrl, AddressResolver resolver) {
        return requestUri(baseUrl, resolver, null);
    }

    static URI requestUri(String baseUrl, AddressResolver resolver, ProxySelector proxySelector) {
        String normalized = normalizedBaseUrl(baseUrl);
        URI base = URI.create(normalized);
        InetAddress[] addresses;
        try {
            addresses = resolver.resolve(base.getHost());
        } catch (UnknownHostException | RuntimeException error) {
            throw providerUnavailable();
        }
        boolean proxied;
        try {
            proxied = proxySelector != null && !base.getHost().matches("[0-9.]+")
                    && proxySelector.select(base).stream()
                    .anyMatch(proxy -> proxy.type() == java.net.Proxy.Type.HTTP);
        } catch (RuntimeException error) {
            throw providerUnavailable();
        }
        if (addresses == null || addresses.length == 0
                || java.util.Arrays.stream(addresses).anyMatch(address ->
                        !isPublicAddress(address) && !(proxied && isFakeProxyAddress(address)))) {
            throw providerUnavailable();
        }
        String path = base.getRawPath();
        if (path != null && path.endsWith("/chat/completions")) {
            return URI.create(normalized);
        }
        return URI.create(normalized + "/chat/completions");
    }

    @FunctionalInterface
    interface AddressResolver {
        InetAddress[] resolve(String host) throws UnknownHostException;
    }

    private static boolean isPublicAddress(InetAddress address) {
        if (address == null || address.isAnyLocalAddress() || address.isLoopbackAddress()
                || address.isLinkLocalAddress() || address.isSiteLocalAddress() || address.isMulticastAddress()) {
            return false;
        }
        byte[] bytes = address.getAddress();
        if (address instanceof Inet4Address) {
            int first = Byte.toUnsignedInt(bytes[0]);
            int second = Byte.toUnsignedInt(bytes[1]);
            int third = Byte.toUnsignedInt(bytes[2]);
            if (first == 0 || first == 10 || first == 127 || first >= 224
                    || (first == 100 && second >= 64 && second <= 127)
                    || (first == 169 && second == 254)
                    || (first == 172 && second >= 16 && second <= 31)
                    || (first == 192 && (second == 168 || (second == 0 && (third == 0 || third == 2))
                            || (second == 88 && third == 99)))
                    || (first == 198 && (second == 18 || second == 19 || (second == 51 && third == 100)))
                    || (first == 203 && second == 0 && third == 113)) {
                return false;
            }
            return true;
        }
        if (address instanceof Inet6Address) {
            int first = Byte.toUnsignedInt(bytes[0]);
            int second = Byte.toUnsignedInt(bytes[1]);
            return first != 0 && (first & 0xfe) != 0xfc
                    && !(first == 0xfe && (second & 0xc0) == 0x80)
                    && first != 0xff
                    && !(first == 0x20 && second == 0x01
                            && bytes[2] == 0x0d && Byte.toUnsignedInt(bytes[3]) == 0xb8);
        }
        return false;
    }

    private static boolean isFakeProxyAddress(InetAddress address) {
        if (!(address instanceof Inet4Address)) return false;
        byte[] bytes = address.getAddress();
        return Byte.toUnsignedInt(bytes[0]) == 198
                && (Byte.toUnsignedInt(bytes[1]) == 18 || Byte.toUnsignedInt(bytes[1]) == 19);
    }

    private static TapstateException providerUnavailable() {
        return AiProviderService.error(ControlError.AI_PROVIDER_FAILED);
    }

    private static TapstateException malformed() {
        return new TapstateException(ControlError.MALFORMED_REQUEST,
                Map.of("reason", "provider settings require a valid HTTPS API base URL, model and API key"), null);
    }
}
