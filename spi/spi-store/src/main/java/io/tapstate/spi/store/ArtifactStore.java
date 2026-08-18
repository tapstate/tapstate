package io.tapstate.spi.store;

import io.tapstate.core.model.Resource;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The artifact truth layer: the canonical, authoritative store of the resources a workspace applies.
 * A pure interface over the resource model; it depends on the core ring only (rule R2).
 *
 * <p>The identity throughout is the resource's top-level id. {@link #saveAll} upserts a batch of
 * resources by that id as one atomic unit — either every one is stored or, on any failure, none is —
 * and the stored form is canonical; {@link #save} is the single-artifact case of it. {@link #get}
 * returns the stored resource for an id, or empty when none is stored. {@link #list} returns every
 * stored resource.
 */
public interface ArtifactStore {

    /**
     * Atomically applies every requested resource write while evaluating each write's condition in the
     * same store operation. A refused condition leaves the entire batch unchanged and identifies the
     * resource and condition outcome that refused it.
     *
     * <p>Adapters should override this for mixed batches. The default keeps existing single-write
     * implementations useful while refusing a mixed conditional batch it cannot make atomic.
     */
    default ArtifactBatchWrite writeAll(List<ArtifactWrite> writes) {
        if (writes.isEmpty()) {
            return ArtifactBatchWrite.applied();
        }
        if (writes.stream().allMatch(write -> write.intent() == ArtifactWrite.Intent.UPSERT)) {
            saveAll(writes.stream().map(ArtifactWrite::resource).toList());
            return ArtifactBatchWrite.applied();
        }
        if (writes.size() == 1) {
            ArtifactWrite write = writes.getFirst();
            ArtifactMutation outcome = switch (write.intent()) {
                case CREATE_ONLY -> create(write.resource());
                case REPLACE_ONLY -> replace(write.resource().id(), write.expectedContentHash(), write.resource());
                case UPSERT -> throw new IllegalStateException("upsert batch must have been handled above");
            };
            return switch (outcome) {
                case CREATED, REPLACED -> ArtifactBatchWrite.applied();
                case NOT_FOUND, ALREADY_EXISTS, VERSION_CONFLICT ->
                        ArtifactBatchWrite.refused(write.resource().id(), outcome);
                case DELETED -> throw new IllegalStateException("artifact write cannot report deletion");
            };
        }
        if (writes.stream().noneMatch(write -> write.intent() == ArtifactWrite.Intent.CREATE_ONLY)) {
            Map<String, String> preconditions = new java.util.LinkedHashMap<>();
            for (ArtifactWrite write : writes) {
                if (write.intent() == ArtifactWrite.Intent.REPLACE_ONLY) {
                    preconditions.put(write.resource().id(), write.expectedContentHash());
                }
            }
            Optional<String> refused = saveAll(
                    writes.stream().map(ArtifactWrite::resource).toList(), preconditions);
            return refused.map(id -> ArtifactBatchWrite.refused(id, ArtifactMutation.VERSION_CONFLICT))
                    .orElseGet(ArtifactBatchWrite::applied);
        }
        throw new UnsupportedOperationException("atomic mixed artifact writes are not implemented");
    }

    /**
     * Atomically inserts {@code artifact} by its top-level id. The artifact is stored only when that
     * id is absent; an existing artifact is left unchanged and returns {@link
     * ArtifactMutation#ALREADY_EXISTS}.
     */
    default ArtifactMutation create(Resource artifact) {
        throw new UnsupportedOperationException("atomic artifact create is not implemented");
    }

    /**
     * Atomically replaces the artifact identified by {@code id} only when its stored content hash
     * equals {@code expectedContentHash}. The expected hash is the 64-character lowercase SHA-256 of
     * the stored canonical UTF-8 bytes. {@code replacement.id()} must equal {@code id}; identity cannot
     * change during replacement. The version check and replacement are one indivisible store
     * operation, so a stale writer never changes the stored canonical bytes.
     */
    default ArtifactMutation replace(String id, String expectedContentHash, Resource replacement) {
        throw new UnsupportedOperationException("atomic artifact replace is not implemented");
    }

    /**
     * Atomically deletes the artifact identified by {@code id} only when its stored content hash
     * equals {@code expectedContentHash}. The expected hash is the 64-character lowercase SHA-256 of
     * the stored canonical UTF-8 bytes. The version check and deletion are one indivisible store
     * operation, so a stale writer never removes the stored artifact.
     */
    default ArtifactMutation delete(String id, String expectedContentHash) {
        throw new UnsupportedOperationException("atomic artifact delete is not implemented");
    }

    /**
     * Atomically upserts every resource in {@code artifacts} by its top-level id: either all are
     * stored or, on any failure, none is — there is no partial batch. The stored form is canonical, and
     * an empty batch writes nothing. Ordering follows the list, though the atomic outcome does not
     * depend on it.
     */
    void saveAll(List<Resource> artifacts);

    /**
     * Atomically upserts {@code artifacts} — the same all-or-nothing batch as {@link #saveAll(List)} —
     * but only while every id named in {@code expectedContentHashes} still stores the hash named
     * against it. The comparison and the writes are one indivisible store operation, so a batch whose
     * declared version has moved on never overwrites the version it did not read. A precondition that
     * is only checked before the write is not this: between the check and the write another writer
     * lands, both writes are accepted, and the earlier one is lost with nothing reporting it.
     *
     * <p>Returns the id of a precondition that did not hold, or empty when the batch was written. A
     * refused batch writes nothing. An id that is named here and is no longer stored at all does not
     * hold either — it has no version to equal the one declared — so it is returned the same way.
     *
     * <p>An empty map is exactly {@link #saveAll(List)}, which is why the default answers it that way.
     * A non-empty one has no unconditional fallback on purpose: silently downgrading to an
     * unconditional batch would leave every caller believing in a check the store never made.
     */
    default Optional<String> saveAll(List<Resource> artifacts, Map<String, String> expectedContentHashes) {
        if (expectedContentHashes.isEmpty()) {
            saveAll(artifacts);
            return Optional.empty();
        }
        throw new UnsupportedOperationException("conditional artifact batch upsert is not implemented");
    }

    /** Upserts a single resource by its top-level id — the single-artifact case of {@link #saveAll}. */
    default void save(Resource artifact) {
        saveAll(List.of(artifact));
    }

    /** Returns the stored resource for the id, or empty if none is stored. */
    Optional<Resource> get(String id);

    /** Lists every stored resource. */
    List<Resource> list();
}
