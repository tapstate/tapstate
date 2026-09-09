package io.tapstate.control.core;

import io.tapstate.core.model.Resource;

import java.util.Objects;

/** A typed artifact read together with the hash of its canonical stored representation. */
public record StoredResource(Resource resource, String contentHash) {

    public StoredResource {
        Objects.requireNonNull(resource, "resource");
        Objects.requireNonNull(contentHash, "contentHash");
    }
}
