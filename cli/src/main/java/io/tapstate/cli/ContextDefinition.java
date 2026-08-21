package io.tapstate.cli;

import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** A durable, non-secret connection target. */
record ContextDefinition(UUID id, List<URI> seeds, ContextTls tls, UUID authRef) {

    ContextDefinition {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(seeds, "seeds");
        Objects.requireNonNull(tls, "tls");
        Objects.requireNonNull(authRef, "authRef");
        seeds = List.copyOf(seeds);
        if (seeds.isEmpty()) {
            throw new IllegalArgumentException("seeds must not be empty");
        }
        for (URI seed : seeds) {
            Objects.requireNonNull(seed, "seed");
            String scheme = seed.getScheme();
            if (!seed.isAbsolute() || !("http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme))
                    || seed.getHost() == null || seed.getQuery() != null || seed.getFragment() != null) {
                throw new IllegalArgumentException("seed must be an absolute HTTP(S) URI");
            }
            if (seed.getUserInfo() != null) {
                throw new IllegalArgumentException("seed must not contain user information");
            }
        }
    }
}
