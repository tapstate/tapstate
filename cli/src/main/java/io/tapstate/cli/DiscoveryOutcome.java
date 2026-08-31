package io.tapstate.cli;

import java.util.List;

/** Result of anonymous stable-issuer discovery against one configured seed. */
sealed interface DiscoveryOutcome {

    record Discovered(String issuer, String clusterId, String apiVersion, List<String> authModes)
            implements DiscoveryOutcome {
        public Discovered {
            authModes = authModes == null ? null : List.copyOf(authModes);
        }
    }

    record Rejected(String code, String message) implements DiscoveryOutcome {
    }

    /** The seed replied, but its success body cannot satisfy the discovery contract. */
    record Invalid(String reason) implements DiscoveryOutcome {
    }

    record Unreachable() implements DiscoveryOutcome {
    }
}
