package io.tapstate.spi.store;

import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Witnesses the atomic versioned-mutation contract independently of a storage adapter. */
class ArtifactStoreTest {

    private static final CanonicalWriter WRITER = new CanonicalWriter();

    @Test
    void versionedMutationsUseCanonicalHashesAndLeaveStaleWritesUnapplied() {
        ContractStore store = new ContractStore();
        Resource source = source("localhost");
        Resource changed = source("replica");
        Resource changedAgain = source("stale-writer");
        String oldHash = hash(source);
        String newHash = hash(changed);

        assertThat(store.create(source)).isEqualTo(ArtifactMutation.CREATED);
        assertThat(store.create(source)).isEqualTo(ArtifactMutation.ALREADY_EXISTS);
        assertThat(store.replace("orders", oldHash, changed)).isEqualTo(ArtifactMutation.REPLACED);

        String canonicalAfterReplace = store.storedCanonical("orders");
        assertThat(store.replace("orders", oldHash, changedAgain))
                .isEqualTo(ArtifactMutation.VERSION_CONFLICT);
        assertThat(store.storedCanonical("orders")).isEqualTo(canonicalAfterReplace);

        assertThat(store.delete("orders", oldHash)).isEqualTo(ArtifactMutation.VERSION_CONFLICT);
        assertThat(store.storedCanonical("orders")).isEqualTo(canonicalAfterReplace);

        assertThat(store.delete("orders", newHash)).isEqualTo(ArtifactMutation.DELETED);
        assertThat(store.delete("orders", newHash)).isEqualTo(ArtifactMutation.NOT_FOUND);
    }

    @Test
    void replacementIdMustMatchAndAMismatchLeavesCanonicalBytesUnchanged() {
        ContractStore store = new ContractStore();
        Resource source = source("localhost");
        Resource differentId = source("customers", "replica");
        store.create(source);
        String canonicalBeforeReplace = store.storedCanonical("orders");

        assertThatThrownBy(() -> store.replace("orders", hash(source), differentId))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(store.storedCanonical("orders")).isEqualTo(canonicalBeforeReplace);
    }

    @Test
    void aConditionalBatchWritesEverythingOrNothingAndNamesTheVersionThatMovedOn() {
        ContractStore store = new ContractStore();
        Resource stored = source("localhost");
        Resource edited = source("replica");
        Resource sibling = source("customers", "localhost");
        store.create(stored);

        assertThat(store.saveAll(List.of(edited), Map.of("orders", hash(stored)))).isEmpty();
        assertThat(store.storedCanonical("orders")).isEqualTo(WRITER.write(edited));

        // The stale batch names its own loser and writes none of itself — the valid sibling included,
        // since the batch is one unit. A store that wrote the sibling would leave the caller half an
        // edit it never asked to be split.
        assertThat(store.saveAll(List.of(source("stale-writer"), sibling), Map.of("orders", hash(stored))))
                .contains("orders");
        assertThat(store.storedCanonical("orders")).isEqualTo(WRITER.write(edited));
        assertThat(store.get("customers")).isEmpty();
    }

    @Test
    void anIdThatIsNoLongerStoredFailsItsDeclaredVersionRatherThanBeingRecreated() {
        ContractStore store = new ContractStore();
        Resource stored = source("localhost");
        store.create(stored);
        store.delete("orders", hash(stored));

        // A declared version says "I am editing what I read". Nothing is stored, so nothing equals it —
        // upserting here would silently resurrect a resource its author believes they are amending.
        assertThat(store.saveAll(List.of(source("replica")), Map.of("orders", hash(stored))))
                .contains("orders");
        assertThat(store.get("orders")).isEmpty();
    }

    @Test
    void anEmptyPreconditionMapIsTheUnconditionalBatch() {
        ContractStore store = new ContractStore();
        // The compatibility case the default relies on: a caller that declares nothing keeps writing
        // exactly as it did, and a store that implements no conditional batch still serves it.
        assertThat(new DefaultingStore().saveAll(List.of(source("localhost")), Map.of())).isEmpty();
        assertThatThrownBy(() -> new DefaultingStore()
                .saveAll(List.of(source("localhost")), Map.of("orders", hash(source("localhost")))))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(store.saveAll(List.of(source("localhost")), Map.of())).isEmpty();
    }

    @Test
    void defaultWriteAllPreservesLegacyUpsertAndSingleReplaceSemantics() {
        LegacySingleWriteStore store = new LegacySingleWriteStore();
        Resource original = source("localhost");
        Resource replacement = source("replica");
        store.seed(original);

        assertThat(store.writeAll(List.of(ArtifactWrite.upsert(replacement))))
                .isEqualTo(ArtifactBatchWrite.applied());
        assertThat(store.saveAllCalls).isEqualTo(1);
        assertThat(store.storedCanonical("orders")).isEqualTo(WRITER.write(replacement));

        ArtifactBatchWrite stale = store.writeAll(List.of(ArtifactWrite.replaceOnly(
                original, hash(original))));
        assertThat(stale.refusedId()).isEqualTo("orders");
        assertThat(stale.refusal()).isEqualTo(ArtifactMutation.VERSION_CONFLICT);

        ArtifactBatchWrite missing = store.writeAll(List.of(ArtifactWrite.replaceOnly(
                source("missing", "replica"), hash(original))));
        assertThat(missing.refusedId()).isEqualTo("missing");
        assertThat(missing.refusal()).isEqualTo(ArtifactMutation.NOT_FOUND);
    }

    private static Resource source(String host) {
        return source("orders", host);
    }

    private static Resource source(String id, String host) {
        return new SourceResource(id, null, "mysql", Map.of("host", host),
                null, null, null, null, null);
    }

    private static String hash(Resource artifact) {
        return CanonicalHash.of(WRITER.write(artifact));
    }

    /**
     * A store that implements only the unconditional batch, to witness what the port's default does for
     * an adapter that has not implemented the conditional one: it serves an empty precondition map and
     * refuses a non-empty one, rather than quietly writing unconditionally and leaving every caller
     * believing in a check that was never made.
     */
    private static final class DefaultingStore implements ArtifactStore {

        private final Map<String, Resource> resources = new LinkedHashMap<>();

        @Override
        public void saveAll(List<Resource> artifacts) {
            artifacts.forEach(artifact -> resources.put(artifact.id(), artifact));
        }

        @Override
        public Optional<Resource> get(String id) {
            return Optional.ofNullable(resources.get(id));
        }

        @Override
        public List<Resource> list() {
            return new ArrayList<>(resources.values());
        }
    }

    /**
     * An adapter that predates conditional batches but implements the original single-replace
     * contract. The default command adapter must preserve these exact outcomes.
     */
    private static final class LegacySingleWriteStore implements ArtifactStore {

        private final Map<String, Resource> resources = new LinkedHashMap<>();
        private int saveAllCalls;

        void seed(Resource resource) {
            resources.put(resource.id(), resource);
        }

        @Override
        public void saveAll(List<Resource> artifacts) {
            saveAllCalls++;
            artifacts.forEach(artifact -> resources.put(artifact.id(), artifact));
        }

        @Override
        public ArtifactMutation replace(String id, String expectedContentHash, Resource replacement) {
            Resource current = resources.get(id);
            if (current == null) {
                return ArtifactMutation.NOT_FOUND;
            }
            if (!hash(current).equals(expectedContentHash)) {
                return ArtifactMutation.VERSION_CONFLICT;
            }
            resources.put(id, replacement);
            return ArtifactMutation.REPLACED;
        }

        @Override
        public Optional<Resource> get(String id) {
            return Optional.ofNullable(resources.get(id));
        }

        @Override
        public List<Resource> list() {
            return new ArrayList<>(resources.values());
        }

        private String storedCanonical(String id) {
            return WRITER.write(resources.get(id));
        }
    }

    /** Minimal canonical-byte store used only to witness the SPI's required outcomes. */
    private static final class ContractStore implements ArtifactStore {

        private final Map<String, Resource> resources = new LinkedHashMap<>();

        @Override
        public void saveAll(List<Resource> artifacts) {
            saveAll(artifacts, Map.of());
        }

        @Override
        public Optional<String> saveAll(List<Resource> artifacts, Map<String, String> expectedContentHashes) {
            for (Map.Entry<String, String> expected : expectedContentHashes.entrySet()) {
                Resource current = resources.get(expected.getKey());
                if (current == null || !hash(current).equals(expected.getValue())) {
                    return Optional.of(expected.getKey());
                }
            }
            for (Resource artifact : artifacts) {
                resources.put(artifact.id(), artifact);
            }
            return Optional.empty();
        }

        @Override
        public ArtifactMutation create(Resource artifact) {
            return resources.putIfAbsent(artifact.id(), artifact) == null
                    ? ArtifactMutation.CREATED
                    : ArtifactMutation.ALREADY_EXISTS;
        }

        @Override
        public ArtifactMutation replace(String id, String expectedContentHash, Resource replacement) {
            if (!id.equals(replacement.id())) {
                throw new IllegalArgumentException("replacement id must equal the artifact id");
            }
            Resource current = resources.get(id);
            if (current == null) {
                return ArtifactMutation.NOT_FOUND;
            }
            if (!hash(current).equals(expectedContentHash)) {
                return ArtifactMutation.VERSION_CONFLICT;
            }
            resources.put(id, replacement);
            return ArtifactMutation.REPLACED;
        }

        @Override
        public ArtifactMutation delete(String id, String expectedContentHash) {
            Resource current = resources.get(id);
            if (current == null) {
                return ArtifactMutation.NOT_FOUND;
            }
            if (!hash(current).equals(expectedContentHash)) {
                return ArtifactMutation.VERSION_CONFLICT;
            }
            resources.remove(id);
            return ArtifactMutation.DELETED;
        }

        @Override
        public Optional<Resource> get(String id) {
            return Optional.ofNullable(resources.get(id));
        }

        @Override
        public List<Resource> list() {
            return new ArrayList<>(resources.values());
        }

        private String storedCanonical(String id) {
            return WRITER.write(resources.get(id));
        }
    }
}
