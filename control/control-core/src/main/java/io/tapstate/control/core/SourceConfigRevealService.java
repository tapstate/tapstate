package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.ArtifactStore;

import java.util.Map;
import java.util.Objects;

/** Reserved single-Source plaintext seam, fail-closed before the encrypted store is read. */
public final class SourceConfigRevealService {

    private final SourceConfigRevealAuthorizer authorizer;
    private final ArtifactStore artifacts;

    public SourceConfigRevealService(SourceConfigRevealAuthorizer authorizer, ArtifactStore artifacts) {
        this.authorizer = Objects.requireNonNull(authorizer, "authorizer");
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
    }

    public SourceConfigRevealResult reveal(String principal, SourceConfigRevealRequest request) {
        Objects.requireNonNull(principal, "principal");
        Objects.requireNonNull(request, "request");
        authorizer.requireAuthorized(principal, request.id(), request.grant());
        SourceResource source = artifacts.get(request.id())
                .filter(SourceResource.class::isInstance)
                .map(SourceResource.class::cast)
                .orElseThrow(() -> new TapstateException(
                        SourceError.NOT_FOUND, Map.of("id", String.valueOf(request.id())), null));
        return new SourceConfigRevealResult(source.id(), source.config());
    }
}
