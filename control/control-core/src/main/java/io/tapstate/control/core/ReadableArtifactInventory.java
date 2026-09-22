package io.tapstate.control.core;

import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.Resource;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.StoredArtifactRecord;

import java.util.List;
import java.util.Objects;

/** Reconstructs only the readable rows from the store's tolerant artifact projection. */
final class ReadableArtifactInventory {

    private static final DslParser PARSER = new DslParser();

    private ReadableArtifactInventory() {
    }

    static List<Resource> list(ArtifactStore store) {
        Objects.requireNonNull(store, "store");
        return store.listStored().stream()
                .filter(StoredArtifactRecord::readable)
                .map(row -> PARSER.parse(row.canonicalForm()))
                .toList();
    }
}
