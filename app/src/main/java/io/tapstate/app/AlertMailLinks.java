package io.tapstate.app;

import io.tapstate.core.alerting.AlertIntent;
import java.net.URI;
import java.util.List;

/** Links derive only from an operator-configured Web origin, never request headers or SMTP input. */
final class AlertMailLinks {
    private final URI origin;
    private final boolean allowLoopback;
    AlertMailLinks(String explicit, String configuredOrigins, boolean allowInsecureLoopback) {
        allowLoopback = allowInsecureLoopback;
        String selected = explicit;
        if (selected == null || selected.isBlank()) {
            var values = configuredOrigins == null ? List.<String>of()
                    : java.util.Arrays.stream(configuredOrigins.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList();
            selected = values.size() == 1 ? values.getFirst() : null;
        }
        origin = selected == null ? null : origin(selected.trim(), allowInsecureLoopback);
    }
    static AlertMailLinks none() { return new AlertMailLinks(null, null, false); }
    boolean allowLoopback() { return allowLoopback; }
    URI details(List<AlertIntent.Member> members, boolean recovery) {
        if (origin == null) return null;
        if (members.size() == 1) {
            var member = members.getFirst();
            if (member.episode()>0 && member.episode() <= 9_007_199_254_740_991L) {
                String key = AlertHttpModel.key(member.evaluation().key());
                URI url = origin.resolve("/alerts?view=" + (recovery ? "history" : "active") + "&key=" + key + "&episode=" + member.episode());
                if (key.length()<=1800 && url.toASCIIString().length() <= 2048) return url;
            }
        }
        return origin.resolve("/alerts");
    }
    private static URI origin(String value, boolean allowLoopback) {
        try {
            URI uri = URI.create(value);
            boolean local = List.of("localhost", "127.0.0.1", "[::1]").stream().anyMatch(host -> host.equalsIgnoreCase(uri.getHost()));
            if (uri.getHost() == null || uri.getRawUserInfo() != null || uri.getRawQuery() != null || uri.getRawFragment() != null
                    || !(uri.getPath().isEmpty() || uri.getPath().equals("/"))
                    || !("https".equalsIgnoreCase(uri.getScheme()) || allowLoopback && local && "http".equalsIgnoreCase(uri.getScheme()))
                    || uri.toASCIIString().length() > 1024 || uri.getPort() == 0 || uri.getPort() > 65535) {
                throw CloudAlertEventWire.invalid("alertWebOrigin");
            }
            return URI.create(uri.getScheme().toLowerCase(java.util.Locale.ROOT) + "://" + uri.getRawAuthority());
        } catch (IllegalArgumentException invalid) { throw CloudAlertEventWire.invalid("alertWebOrigin"); }
    }
}
