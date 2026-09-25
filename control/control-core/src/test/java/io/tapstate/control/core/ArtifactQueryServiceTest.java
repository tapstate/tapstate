package io.tapstate.control.core;

import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.StoredArtifactRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The read side of the double-layer model: the store is the truth layer. Every public Source read omits
 * connector config, while its authoritative model and content hash stay unchanged. Non-Source resources
 * retain their byte-stable canonical round trip.
 */
class ArtifactQueryServiceTest {

    private final InMemoryArtifactStore store = new InMemoryArtifactStore();
    private final ApplyService apply =
            new ApplyService(TapstateCatalog::load, store, new AuditGate(record -> { }, Clock.systemUTC()),
                    new EmptySchemaStore(), PlanAdvisories.none(), SchemaDerivation.none());
    private final ArtifactQueryService query = new ArtifactQueryService(store);

    private static ArtifactDraft draft(String content) {
        return new ArtifactDraft(null, content);
    }

    /** The offline canonical contract for a draft: the exact bytes the authoring corpus golden is generated from. */
    private static String offlineCanonical(String draft) {
        return new CanonicalWriter().write(new DslParser().parse(draft));
    }

    @Test
    void getReadsBackSourceIdentityButOmitsConnectorConfig() {
        apply.apply("alice", List.of(draft(TGT_MG)));

        Optional<StoredArtifact> got = query.get("tgt_mg");

        assertThat(got).isPresent();
        assertThat(got.get().id()).isEqualTo("tgt_mg");
        assertThat(got.get().kind()).isEqualTo("source");
        assertThat(got.get().canonicalForm()).contains("id: tgt_mg", "connector: mongodb")
                .doesNotContain("config:", "10.30.0.11");
    }

    @Test
    void getReturnsEmptyForAnUnstoredId() {
        apply.apply("alice", List.of(draft(TGT_MG)));

        assertThat(query.get("no_such_id")).isEmpty();
    }

    @Test
    void sourceOutersRemainReadableWhileEveryConnectorConfigIsOmitted() {
        apply.apply("alice", List.of(draft(SRC_ORA), draft(TGT_MG), draft(PIPELINE)));

        assertThat(query.get("src_ora")).get().extracting(StoredArtifact::canonicalForm)
                .asString().contains("id: src_ora", "connector: oracle", "mode: cdc")
                .doesNotContain("config:", "Ora_2026");
        assertThat(query.get("tgt_mg")).get().extracting(StoredArtifact::canonicalForm)
                .asString().contains("id: tgt_mg", "connector: mongodb")
                .doesNotContain("config:", "10.30.0.11");
    }

    /**
     * A pipeline is the one kind whose stored form is not the offline canonical of the text that was
     * applied, and the difference is deliberate: apply records this pipeline's own srs switch for each
     * source it reads, which the author's text does not carry.
     *
     * <p>So the equality is asserted where it still means what it was written to mean. "The online path
     * did not fork the canonical form" reduces, once the two inputs legitimately differ, to: the stored
     * text is a fixed point of the offline writer. A second writer would have to agree with the first
     * byte-for-byte on its own output to pass this, which is the drift the original case guarded.
     *
     * <p>The second assertion is what keeps the first from passing vacuously -- were materialization
     * dropped, the fixed point would still hold and only this would notice.
     */
    @Test
    void aPipelinesStoredFormIsTheOfflineCanonicalPlusTheSwitchesApplyRecorded() {
        apply.apply("alice", List.of(draft(SRC_ORA), draft(TGT_MG), draft(PIPELINE)));

        String stored = query.get("ora2my_ods").orElseThrow().canonicalForm();

        assertThat(offlineCanonical(stored))
                .as("the online path writes what the offline writer writes for the same artifact")
                .isEqualTo(stored);
        assertThat(stored)
                .as("and it differs from the author's text by exactly the recorded switch")
                .isNotEqualTo(offlineCanonical(PIPELINE))
                .contains("srs:");
    }

    @Test
    void listReturnsTheSamePublicProjectionAsGet() {
        apply.apply("alice", List.of(draft(SRC_ORA), draft(TGT_MG), draft(PIPELINE)));

        assertThat(query.list()).extracting(ArtifactListEntry::id)
                .containsExactlyInAnyOrder("src_ora", "tgt_mg", "ora2my_ods");
        // Each listed artifact carries the same public representation its own get returns.
        assertThat(query.list()).allSatisfy(a ->
                assertThat(a.canonicalForm())
                        .isEqualTo(query.get(a.id()).orElseThrow().canonicalForm()));
    }

    @Test
    void genericArtifactReadsDoNotExposeAtlasUriCredentials() {
        String uri = "mongodb+srv://alice:pa%40ss@cluster.example/test";
        SourceResource atlas = new SourceResource(
                "atlas", null, "mongodb-atlas", Map.of("isUri", true, "uri", uri),
                null, null, null, null);
        store.save(atlas);

        StoredArtifact got = query.get("atlas").orElseThrow();
        ArtifactListEntry listed = query.list("source").stream()
                .filter(entry -> entry.id().equals("atlas")).findFirst().orElseThrow();

        assertThat(got.canonicalForm()).contains("id: atlas", "connector: mongodb-atlas")
                .doesNotContain("config:", "cluster.example/test", "alice", "pa%40ss");
        assertThat(listed.canonicalForm()).isEqualTo(got.canonicalForm());
        assertThat(got.contentHash()).isEqualTo(CanonicalHash.of(atlas));
        assertThat(((SourceResource) query.getResource("atlas").orElseThrow().resource()).config())
                .containsEntry("uri", uri);
    }

    @Test
    void genericReadsOmitConfigEvenWhenTheSourceHasNoPassword() {
        apply.apply("alice", List.of(draft(TGT_MG)));

        StoredArtifact got = query.get("tgt_mg").orElseThrow();
        ArtifactListEntry listed = query.list("source").getFirst();

        assertThat(got.canonicalForm()).contains("kind: source", "id: tgt_mg", "connector: mongodb")
                .doesNotContain("config:", "10.30.0.11");
        assertThat(listed.canonicalForm()).isEqualTo(got.canonicalForm());
        assertThat(got.contentHash()).isEqualTo(CanonicalHash.of(store.get("tgt_mg").orElseThrow()));
    }

    @Test
    void allDeclaredSourceConfigIsOmittedWithoutChangingTheAuthoritativeHash() {
        SourceResource oracle = new SourceResource(
                "oracle", null, "oracle",
                Map.of("host", "db.example", "user", "alice", "password", "sentinel-secret"),
                null, null, null, null);
        store.save(oracle);

        StoredArtifact got = query.get("oracle").orElseThrow();

        assertThat(got.canonicalForm()).contains("id: oracle", "connector: oracle")
                .doesNotContain("config:", "db.example", "alice", "sentinel-secret");
        assertThat(got.contentHash()).isEqualTo(CanonicalHash.of(oracle));
    }

    @Test
    void unreadableSourceInventoryDoesNotReturnUnparsedCredentials() {
        store.putUnreadable("broken", "source", "uri: mongodb+srv://alice:pa%40ss@cluster.example/test");

        ArtifactListEntry listed = query.list("source").getFirst();

        assertThat(listed.readable()).isFalse();
        assertThat(listed.canonicalForm()).doesNotContain("alice", "pa%40ss");
    }

    @Test
    void unknownConnectorsStillExposeOnlyTheSourceOuterFields() {
        SourceResource first = new SourceResource(
                "first", null, "unregistered-connector",
                Map.of("isUri", true, "uri", "mongodb+srv://alice:first@db.example/test"),
                null, null, null, null);
        SourceResource second = new SourceResource(
                "second", null, "unregistered-connector",
                Map.of("isUri", true, "uri", "mongodb+srv://alice:second@db.example/test"),
                null, null, null, null);
        store.save(first);
        store.save(second);
        assertThat(query.list("source")).hasSize(2)
                .allSatisfy(row -> assertThat(row.canonicalForm()).contains("kind: source")
                        .doesNotContain("config:", "alice:"));
    }

    @Test
    void anUnboundedNestedConfigIsOmittedWithTheRestOfTheConnectionConfig() {
        SourceResource malformed = new SourceResource(
                "nested", null, "mongodb-atlas",
                Map.of("isUri", true, "additionalString", Map.of("opaque", "sentinel-secret")),
                null, null, null, null);
        store.save(malformed);

        assertThat(query.get("nested").orElseThrow().canonicalForm())
                .contains("id: nested", "connector: mongodb-atlas")
                .doesNotContain("config:", "sentinel-secret");
    }

    @Test
    void listKeepsAnUnreadableStoredRowVisibleWhileGetRemainsStrict() {
        apply.apply("alice", List.of(draft(TGT_MG)));
        store.putUnreadable("corrupt", "pipeline", "not: [valid");

        List<ArtifactListEntry> listed = query.list();

        assertThat(listed).extracting(ArtifactListEntry::id)
                .containsExactlyInAnyOrder("tgt_mg", "corrupt");
        assertThat(listed).filteredOn(a -> a.id().equals("tgt_mg")).singleElement()
                .satisfies(a -> assertThat(a.readable()).isTrue());
        assertThat(listed).filteredOn(a -> a.id().equals("corrupt")).singleElement()
                .satisfies(a -> {
                    assertThat(a.kind()).isEqualTo("pipeline");
                    assertThat(a.canonicalForm()).isEqualTo("not: [valid");
                    assertThat(a.readable()).isFalse();
                });
        assertThatThrownBy(() -> query.get("corrupt"))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void listIsEmptyWhenNothingIsStored() {
        assertThat(query.list()).isEmpty();
    }

    @Test
    void aReadCarriesTheContentHashOfTheVersionItReturns() {
        // The hash is the precondition an edit or a removal has to supply, and it is taken over the
        // resource's structure -- so the canonical bytes returned beside it are not enough to derive it,
        // and this read is the only place a caller can get it. get then delete needs no second source.
        apply.apply("alice", List.of(draft(TGT_MG)));

        StoredArtifact got = query.get("tgt_mg").orElseThrow();

        assertThat(got.contentHash()).isEqualTo(CanonicalHash.of(store.get("tgt_mg").orElseThrow()));
    }

    @Test
    void theHashAReadReturnsIsTheOneTheWriteSideIssued() {
        // The read hash and the write hash must be the same value, not merely the same shape: a removal
        // compares what a caller read against what the store holds. Deriving the read hash from anything
        // else — the raw draft text before canonicalization, a second writer — still yields a well-formed
        // 64-char string that every shape assertion accepts, and every delete-after-get then fails as a
        // version conflict. Pinning it against the apply outcome is what catches that.
        ApplyResult applied = apply.apply("alice", List.of(draft(TGT_MG)));
        String issuedOnWrite = applied.outcomes().stream()
                .filter(o -> o.id().equals("tgt_mg"))
                .findFirst().orElseThrow()
                .contentHash();

        assertThat(query.get("tgt_mg").orElseThrow().contentHash()).isEqualTo(issuedOnWrite);
    }

    @Test
    void aChangedArtifactReadsBackWithADifferentHash() {
        // Discriminating against a hash taken over the id (or any other per-resource constant): the id is
        // unchanged across this edit, so such an implementation returns the same hash for both versions
        // and a stale precondition would be accepted as current.
        apply.apply("alice", List.of(draft(TGT_MG)));
        String before = query.get("tgt_mg").orElseThrow().contentHash();

        apply.apply("alice", List.of(draft(TGT_MG_CHANGED)));

        assertThat(query.get("tgt_mg").orElseThrow().contentHash()).isNotEqualTo(before);
    }

    @Test
    void everyListedArtifactCarriesTheHashItsOwnGetReturns() {
        apply.apply("alice", List.of(draft(SRC_ORA), draft(TGT_MG), draft(PIPELINE)));

        assertThat(query.list()).allSatisfy(a ->
                assertThat(a.contentHash())
                        .isEqualTo(query.get(a.id()).orElseThrow().contentHash()));
    }

    @Test
    void listByKindReturnsOnlyArtifactsOfThatKind() {
        // The read-by-kind query lives in the read service (server-as-truth read semantics), so a face
        // stays a pure projection: list("source") returns the two sources, list("pipeline") the pipeline.
        apply.apply("alice", List.of(draft(SRC_ORA), draft(TGT_MG), draft(PIPELINE)));

        assertThat(query.list("source")).extracting(ArtifactListEntry::id)
                .containsExactlyInAnyOrder("src_ora", "tgt_mg");
        assertThat(query.list("pipeline")).extracting(ArtifactListEntry::id)
                .containsExactly("ora2my_ods");
    }

    @Test
    void listByBlankKindReturnsEveryKind() {
        // A blank or absent kind filter is "no filter": the query returns every stored artifact across
        // kinds, the same as the unfiltered list, so the endpoint's optional ?kind= parameter degrades
        // to list-all.
        apply.apply("alice", List.of(draft(SRC_ORA), draft(TGT_MG), draft(PIPELINE)));

        assertThat(query.list((String) null)).extracting(ArtifactListEntry::id)
                .containsExactlyInAnyOrder("src_ora", "tgt_mg", "ora2my_ods");
        assertThat(query.list("   ")).extracting(ArtifactListEntry::id)
                .containsExactlyInAnyOrder("src_ora", "tgt_mg", "ora2my_ods");
    }

    @Test
    void onlyApplyMovesTheTruthLayerNotAPreparedEdit() {
        // Server-as-truth: the store is the read source and only apply mutates it. Apply v1 -> get is v1;
        // preparing the edit through plan (the write-free validate + canonicalize front half, which writes
        // nothing) leaves the store — and get — at v1; applying the edit is what finally moves get to v2.
        apply.apply("alice", List.of(draft(TGT_MG)));
        StoredArtifact before = query.get("tgt_mg").orElseThrow();
        assertThat(before.canonicalForm()).doesNotContain("config:");
        assertThat(((SourceResource) query.getResource("tgt_mg").orElseThrow().resource()).config())
                .containsEntry("uri", "mongodb://10.30.0.11:27017/ods");

        // The edit is only prepared, never applied — plan writes no store state — so get still reads v1.
        apply.plan(List.of(draft(TGT_MG_CHANGED)));
        assertThat(query.get("tgt_mg").orElseThrow().contentHash())
                .as("a prepared-but-unapplied edit does not reach the truth layer")
                .isEqualTo(before.contentHash());

        apply.apply("alice", List.of(draft(TGT_MG_CHANGED)));
        assertThat(query.get("tgt_mg").orElseThrow().contentHash())
                .as("the new config changed the authoritative version even though it is not displayed")
                .isNotEqualTo(before.contentHash());
        assertThat(query.get("tgt_mg").orElseThrow().canonicalForm()).isEqualTo(before.canonicalForm());
        assertThat(((SourceResource) query.getResource("tgt_mg").orElseThrow().resource()).config())
                .containsEntry("uri", "mongodb://10.30.0.12:27017/ods");
    }

    @Test
    void aNullStoreIsRejected() {
        assertThatThrownBy(() -> new ArtifactQueryService(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void aNullIdIsRejected() {
        assertThatThrownBy(() -> query.get(null))
                .isInstanceOf(NullPointerException.class);
    }

    // ---- fixtures ----

    private static final String TGT_MG = """
            version: tapstate/v1
            kind: source
            id: tgt_mg
            connector: mongodb
            config: { uri: "mongodb://10.30.0.11:27017/ods" }
            """;

    private static final String TGT_MG_CHANGED = """
            version: tapstate/v1
            kind: source
            id: tgt_mg
            connector: mongodb
            config: { uri: "mongodb://10.30.0.12:27017/ods" }
            """;

    private static final String SRC_ORA = """
            version: tapstate/v1
            kind: source
            id: src_ora
            connector: oracle
            config: { host: 10.20.0.15, port: 1521, service_name: ORCL,
                      username: cdc_user, password: Ora_2026 }
            mode: cdc
            tables: [ ORDERS, ORDER_ITEMS, CUSTOMERS ]
            """;

    private static final String PIPELINE = """
            version: tapstate/v1
            kind: pipeline
            id: ora2my_ods
            source: src_ora
            settings: { read_mode: snapshot_and_cdc }
            serve:
              from: /.*/
              sync:
                - id: my_ods
                  source: tgt_mg
                  write_mode: upsert
                  ddl: apply
            """;

    /**
     * An in-memory {@link ArtifactStore} that mirrors the Mongo store's canonical round-trip: it holds
     * each artifact as its canonical text and reconstructs it on read through the parser, so a read
     * exercises the same write-then-parse the real store does.
     */
    private static final class InMemoryArtifactStore implements ArtifactStore {

        private final CanonicalWriter writer = new CanonicalWriter();
        private final DslParser parser = new DslParser();
        private final Map<String, String> byId = new LinkedHashMap<>();
        private final Map<String, String> kindById = new LinkedHashMap<>();

        void putUnreadable(String id, String kind, String canonical) {
            byId.put(id, canonical);
            kindById.put(id, kind);
        }

        @Override
        public void saveAll(List<Resource> artifacts) {
            // Atomic on this fake by construction: the whole valid batch stages into the map at once.
            Map<String, String> staged = new LinkedHashMap<>();
            for (Resource artifact : artifacts) {
                staged.put(artifact.id(), writer.write(artifact));
            }
            byId.putAll(staged);
            for (Resource artifact : artifacts) {
                kindById.put(artifact.id(), artifact.kind());
            }
        }

        @Override
        public Optional<String> saveAll(List<Resource> artifacts, Map<String, String> expectedContentHashes) {
            for (Map.Entry<String, String> expected : expectedContentHashes.entrySet()) {
                String canonical = byId.get(expected.getKey());
                if (canonical == null
                        || !CanonicalHash.of(parser.parse(canonical)).equals(expected.getValue())) {
                    return Optional.of(expected.getKey());
                }
            }
            saveAll(artifacts);
            return Optional.empty();
        }

        @Override
        public Optional<Resource> get(String id) {
            String canonical = byId.get(id);
            return canonical == null ? Optional.empty() : Optional.of(parser.parse(canonical));
        }

        @Override
        public List<Resource> list() {
            List<Resource> resources = new ArrayList<>();
            for (String canonical : byId.values()) {
                resources.add(parser.parse(canonical));
            }
            return resources;
        }

        @Override
        public List<StoredArtifactRecord> listStored() {
            List<StoredArtifactRecord> rows = new ArrayList<>();
            for (Map.Entry<String, String> entry : byId.entrySet()) {
                try {
                    rows.add(StoredArtifactRecord.of(parser.parse(entry.getValue())));
                } catch (RuntimeException unreadable) {
                    rows.add(new StoredArtifactRecord(
                            entry.getKey(), kindById.getOrDefault(entry.getKey(), "unknown"),
                            entry.getValue(), null, false));
                }
            }
            return rows;
        }
    }
}
