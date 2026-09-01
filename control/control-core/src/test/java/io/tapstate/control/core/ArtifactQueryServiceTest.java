package io.tapstate.control.core;

import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.Resource;
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
 * The read side of the double-layer model: the store is the truth layer, and a read returns an
 * artifact as its canonical form straight from that layer (server-as-truth). Its central guarantee is
 * that the online read path — apply -> store -> get — reproduces the offline canonical contract
 * byte-for-byte, the same {@link CanonicalWriter} the authoring corpus golden locks, so the online side
 * never forks the canonical form.
 */
class ArtifactQueryServiceTest {

    private final InMemoryArtifactStore store = new InMemoryArtifactStore();
    private final ApplyService apply =
            new ApplyService(TapstateCatalog::load, store, new AuditGate(record -> { }, Clock.systemUTC()),
                    new EmptySchemaStore(), PlanAdvisories.none());
    private final ArtifactQueryService query = new ArtifactQueryService(store);

    private static ArtifactDraft draft(String content) {
        return new ArtifactDraft(null, content);
    }

    /** The offline canonical contract for a draft: the exact bytes the authoring corpus golden is generated from. */
    private static String offlineCanonical(String draft) {
        return new CanonicalWriter().write(new DslParser().parse(draft));
    }

    @Test
    void getReadsBackAnAppliedArtifactAsItsCanonicalForm() {
        apply.apply("alice", List.of(draft(TGT_MY)));

        Optional<StoredArtifact> got = query.get("tgt_my");

        assertThat(got).isPresent();
        assertThat(got.get().id()).isEqualTo("tgt_my");
        assertThat(got.get().kind()).isEqualTo("source");
        assertThat(got.get().canonicalForm())
                .as("get reads back the stored canonical form")
                .isEqualTo(offlineCanonical(TGT_MY));
    }

    @Test
    void getReturnsEmptyForAnUnstoredId() {
        apply.apply("alice", List.of(draft(TGT_MY)));

        assertThat(query.get("no_such_id")).isEmpty();
    }

    @Test
    void appliedArtifactsReadBackByteStableAsTheOfflineCanonical() {
        // The core golden: the online read path (apply -> store -> get) reproduces the offline canonical
        // form byte-for-byte across kinds — source and pipeline here — using the one CanonicalWriter the
        // authoring corpus golden locks. No second baseline is checked in on the online side: forking the
        // canonical form here is exactly the drift this guards, so the expectation is the offline contract.
        apply.apply("alice", List.of(draft(SRC_ORA), draft(TGT_MY), draft(PIPELINE)));

        assertThat(query.get("src_ora")).get().extracting(StoredArtifact::canonicalForm)
                .isEqualTo(offlineCanonical(SRC_ORA));
        assertThat(query.get("tgt_my")).get().extracting(StoredArtifact::canonicalForm)
                .isEqualTo(offlineCanonical(TGT_MY));
        assertThat(query.get("ora2my_ods")).get().extracting(StoredArtifact::canonicalForm)
                .isEqualTo(offlineCanonical(PIPELINE));
    }

    @Test
    void listReturnsEveryStoredArtifactAsItsCanonicalForm() {
        apply.apply("alice", List.of(draft(SRC_ORA), draft(TGT_MY), draft(PIPELINE)));

        assertThat(query.list()).extracting(ArtifactListEntry::id)
                .containsExactlyInAnyOrder("src_ora", "tgt_my", "ora2my_ods");
        // Each listed artifact carries the same canonical form its own get returns.
        assertThat(query.list()).allSatisfy(a ->
                assertThat(a.canonicalForm())
                        .isEqualTo(query.get(a.id()).orElseThrow().canonicalForm()));
    }

    @Test
    void listKeepsAnUnreadableStoredRowVisibleWhileGetRemainsStrict() {
        apply.apply("alice", List.of(draft(TGT_MY)));
        store.putUnreadable("corrupt", "pipeline", "not: [valid");

        List<ArtifactListEntry> listed = query.list();

        assertThat(listed).extracting(ArtifactListEntry::id)
                .containsExactlyInAnyOrder("tgt_my", "corrupt");
        assertThat(listed).filteredOn(a -> a.id().equals("tgt_my")).singleElement()
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
        // The hash is the precondition an edit or a removal has to supply, and a remote model calling
        // this read cannot compute SHA-256 for itself, so the read is what hands it over. It is taken
        // from the very bytes this read returned: get then delete needs no second source for it.
        apply.apply("alice", List.of(draft(TGT_MY)));

        StoredArtifact got = query.get("tgt_my").orElseThrow();

        assertThat(got.contentHash()).isEqualTo(CanonicalHash.of(got.canonicalForm()));
    }

    @Test
    void theHashAReadReturnsIsTheOneTheWriteSideIssued() {
        // The read hash and the write hash must be the same value, not merely the same shape: a removal
        // compares what a caller read against what the store holds. Deriving the read hash from anything
        // else — the raw draft text before canonicalization, a second writer — still yields a well-formed
        // 64-char string that every shape assertion accepts, and every delete-after-get then fails as a
        // version conflict. Pinning it against the apply outcome is what catches that.
        ApplyResult applied = apply.apply("alice", List.of(draft(TGT_MY)));
        String issuedOnWrite = applied.outcomes().stream()
                .filter(o -> o.id().equals("tgt_my"))
                .findFirst().orElseThrow()
                .contentHash();

        assertThat(query.get("tgt_my").orElseThrow().contentHash()).isEqualTo(issuedOnWrite);
    }

    @Test
    void aChangedArtifactReadsBackWithADifferentHash() {
        // Discriminating against a hash taken over the id (or any other per-resource constant): the id is
        // unchanged across this edit, so such an implementation returns the same hash for both versions
        // and a stale precondition would be accepted as current.
        apply.apply("alice", List.of(draft(TGT_MY)));
        String before = query.get("tgt_my").orElseThrow().contentHash();

        apply.apply("alice", List.of(draft(TGT_MY_CHANGED)));

        assertThat(query.get("tgt_my").orElseThrow().contentHash()).isNotEqualTo(before);
    }

    @Test
    void everyListedArtifactCarriesTheHashItsOwnGetReturns() {
        apply.apply("alice", List.of(draft(SRC_ORA), draft(TGT_MY), draft(PIPELINE)));

        assertThat(query.list()).allSatisfy(a ->
                assertThat(a.contentHash())
                        .isEqualTo(query.get(a.id()).orElseThrow().contentHash()));
    }

    @Test
    void listByKindReturnsOnlyArtifactsOfThatKind() {
        // The read-by-kind query lives in the read service (server-as-truth read semantics), so a face
        // stays a pure projection: list("source") returns the two sources, list("pipeline") the pipeline.
        apply.apply("alice", List.of(draft(SRC_ORA), draft(TGT_MY), draft(PIPELINE)));

        assertThat(query.list("source")).extracting(ArtifactListEntry::id)
                .containsExactlyInAnyOrder("src_ora", "tgt_my");
        assertThat(query.list("pipeline")).extracting(ArtifactListEntry::id)
                .containsExactly("ora2my_ods");
    }

    @Test
    void listByBlankKindReturnsEveryKind() {
        // A blank or absent kind filter is "no filter": the query returns every stored artifact across
        // kinds, the same as the unfiltered list, so the endpoint's optional ?kind= parameter degrades
        // to list-all.
        apply.apply("alice", List.of(draft(SRC_ORA), draft(TGT_MY), draft(PIPELINE)));

        assertThat(query.list((String) null)).extracting(ArtifactListEntry::id)
                .containsExactlyInAnyOrder("src_ora", "tgt_my", "ora2my_ods");
        assertThat(query.list("   ")).extracting(ArtifactListEntry::id)
                .containsExactlyInAnyOrder("src_ora", "tgt_my", "ora2my_ods");
    }

    @Test
    void onlyApplyMovesTheTruthLayerNotAPreparedEdit() {
        // Server-as-truth: the store is the read source and only apply mutates it. Apply v1 -> get is v1;
        // preparing the edit through plan (the store-free validate + canonicalize front half, which writes
        // nothing) leaves the store — and get — at v1; applying the edit is what finally moves get to v2.
        apply.apply("alice", List.of(draft(TGT_MY)));
        assertThat(query.get("tgt_my")).get().extracting(StoredArtifact::canonicalForm)
                .isEqualTo(offlineCanonical(TGT_MY));

        // The edit is only prepared, never applied — plan touches no store — so get still reads v1.
        apply.plan(List.of(draft(TGT_MY_CHANGED)));
        assertThat(query.get("tgt_my")).get().extracting(StoredArtifact::canonicalForm)
                .as("a prepared-but-unapplied edit does not reach the truth layer")
                .isEqualTo(offlineCanonical(TGT_MY));

        apply.apply("alice", List.of(draft(TGT_MY_CHANGED)));
        assertThat(query.get("tgt_my")).get().extracting(StoredArtifact::canonicalForm)
                .as("get reflects the last apply — server-as-truth, last write wins")
                .isEqualTo(offlineCanonical(TGT_MY_CHANGED));
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

    private static final String TGT_MY = """
            version: tapstate/v1
            kind: source
            id: tgt_my
            connector: mysql
            config: { host: 10.30.0.5, username: writer, password: My_2026 }
            """;

    private static final String TGT_MY_CHANGED = """
            version: tapstate/v1
            kind: source
            id: tgt_my
            connector: mysql
            config: { host: 10.30.0.5, username: writer, password: Changed_2026 }
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
                  source: tgt_my
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
