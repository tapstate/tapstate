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
        return scan(store).resources();
    }

    /**
     * Resolves readable resources while retaining the identities of pipeline rows that could not be
     * reconstructed. Most validation can ignore those rows, but a live-change guard cannot turn an
     * unknown source edge into no edge.
     */
    static Snapshot scan(ArtifactStore store) {
        Objects.requireNonNull(store, "store");
        List<Resource> resources = new ArrayList<>();
        List<String> unreadablePipelineIds = new ArrayList<>();
        for (StoredArtifactRecord row : store.listStored()) {
            if (!row.readable()) {
                retainUnreadablePipeline(row, unreadablePipelineIds);
                continue;
            }
            try {
                store.get(row.id()).ifPresent(resources::add);
            } catch (TapstateException failure) {
                if (failure.code() != IoError.DOCUMENT_UNREADABLE) {
                    throw failure;
                }
                // The row became unreadable between the browse projection and this typed lookup.
                retainUnreadablePipeline(row, unreadablePipelineIds);
            }
        }
        return new Snapshot(resources, unreadablePipelineIds);
    }

    private static void retainUnreadablePipeline(
            StoredArtifactRecord row, List<String> unreadablePipelineIds) {
        if ("pipeline".equals(row.kind())) {
            unreadablePipelineIds.add(row.id());
        }
    }

    record Snapshot(List<Resource> resources, List<String> unreadablePipelineIds) {

        Snapshot {
            resources = List.copyOf(resources);
            unreadablePipelineIds = List.copyOf(unreadablePipelineIds);
        }
    }
}
