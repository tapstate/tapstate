package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.Resource;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.StoredArtifactRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Resolves the readable rows from the store's tolerant artifact projection. */
final class ReadableArtifactInventory {

    private ReadableArtifactInventory() {
    }

    static List<Resource> list(ArtifactStore store) {
        Objects.requireNonNull(store, "store");
        List<Resource> resources = new ArrayList<>();
        for (StoredArtifactRecord row : store.listStored()) {
            if (!row.readable()) {
                continue;
            }
            try {
                store.get(row.id()).ifPresent(resources::add);
            } catch (TapstateException failure) {
                if (failure.code() != IoError.DOCUMENT_UNREADABLE) {
                    throw failure;
                }
                // The row became unreadable between the browse projection and this typed lookup.
            }
        }
        return List.copyOf(resources);
    }
}
