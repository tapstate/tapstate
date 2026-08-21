package io.tapstate.cli;

import io.tapstate.core.common.TapstateException;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

/** Establishes the anonymous multi-seed issuer gate before exposing any cached credential. */
final class IssuerBinding {

    private static final String ISSUER_PREFIX = "urn:tapstate:cluster:";
    private final ControlPlaneClient client;

    IssuerBinding(ControlPlaneClient client) {
        this.client = Objects.requireNonNull(client, "client");
    }

    Verified verify(ContextDefinition context, String expectedIssuer) {
        Objects.requireNonNull(context, "context");
        return verify(context.seeds(), expectedIssuer);
    }

    Verified verify(List<URI> seeds, String expectedIssuer) {
        Objects.requireNonNull(seeds, "seeds");
        if (seeds.isEmpty()) {
            throw new IllegalArgumentException("seeds must not be empty");
        }
        for (URI seed : seeds) {
            requireSecureTransport(seed);
        }

        DiscoveryOutcome.Discovered agreed = null;
        URI selected = null;
        for (URI seed : seeds) {
            DiscoveryOutcome outcome = client.discover(seed);
            if (outcome instanceof DiscoveryOutcome.Invalid invalid) {
                throw failure(CliError.ISSUER_DISCOVERY_INVALID,
                        Map.of("seed", seed.toString(), "reason", invalid.reason()));
            }
            if (!(outcome instanceof DiscoveryOutcome.Discovered discovered)) {
                throw failure(CliError.ISSUER_DISCOVERY_FAILED, Map.of("seed", seed.toString()));
            }
            validate(seed, discovered);
            if (agreed == null) {
                agreed = discovered;
                selected = seed;
            } else if (!agreed.issuer().equals(discovered.issuer())) {
                throw mismatch(agreed.issuer(), discovered.issuer(), seed);
            }
            if (expectedIssuer != null && !expectedIssuer.equals(discovered.issuer())) {
                throw mismatch(expectedIssuer, discovered.issuer(), seed);
            }
        }
        return new Verified(selected, agreed);
    }

    private static void requireSecureTransport(URI seed) {
        if ("https".equalsIgnoreCase(seed.getScheme())) {
            return;
        }
        if (!"http".equalsIgnoreCase(seed.getScheme()) || !isLiteralLoopback(seed.getHost())) {
            throw failure(CliError.REMOTE_PLAINTEXT, Map.of("seed", seed.toString()));
        }
    }

    private static boolean isLiteralLoopback(String host) {
        if (host == null) {
            return false;
        }
        String normalized = host.toLowerCase(java.util.Locale.ROOT);
        if (normalized.startsWith("[") && normalized.endsWith("]")) {
            normalized = normalized.substring(1, normalized.length() - 1);
        }
        if ("localhost".equals(normalized) || "::1".equals(normalized)
                || "0:0:0:0:0:0:0:1".equals(normalized)) {
            return true;
        }
        String[] octets = normalized.split("\\.", -1);
        if (octets.length != 4 || !"127".equals(octets[0])) {
            return false;
        }
        for (int i = 1; i < octets.length; i++) {
            try {
                int value = Integer.parseInt(octets[i]);
                if (value < 0 || value > 255) {
                    return false;
                }
            } catch (NumberFormatException invalid) {
                return false;
            }
        }
        return true;
    }

    private static void validate(URI seed, DiscoveryOutcome.Discovered discovered) {
        String clusterId = discovered.clusterId();
        String expected = clusterId == null ? null : ISSUER_PREFIX + clusterId;
        List<String> modes = discovered.authModes();
        if (clusterId == null
                || clusterId.isBlank()
                || clusterId.indexOf(':') >= 0
                || !Objects.equals(expected, discovered.issuer())
                || !"tapstate/v1".equals(discovered.apiVersion())
                || modes == null
                || !modes.contains("password")
                || !modes.contains("machine_token")) {
            throw failure(CliError.ISSUER_DISCOVERY_INVALID,
                    Map.of("seed", seed.toString(), "reason", "response-contract"));
        }
    }

    private static TapstateException mismatch(String expected, String actual, URI seed) {
        return failure(CliError.AUTH_ISSUER_MISMATCH,
                Map.of("expected", expected, "actual", actual, "seed", seed.toString()));
    }

    private static TapstateException failure(CliError code, Map<String, Object> args) {
        return new TapstateException(code, args, null);
    }

    /** A capability obtainable only after every seed has passed the anonymous issuer gate. */
    static final class Verified {
        private final URI seed;
        private final DiscoveryOutcome.Discovered discovery;

        private Verified(URI seed, DiscoveryOutcome.Discovered discovery) {
            this.seed = Objects.requireNonNull(seed, "seed");
            this.discovery = Objects.requireNonNull(discovery, "discovery");
        }

        String issuer() {
            return discovery.issuer();
        }

        URI seed() {
            return seed;
        }

        <T> T withCredential(String credential, BiFunction<URI, String, T> request) {
            return Objects.requireNonNull(request, "request")
                    .apply(seed, Objects.requireNonNull(credential, "credential"));
        }
    }
}
