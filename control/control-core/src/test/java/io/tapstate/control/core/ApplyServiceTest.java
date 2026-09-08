package io.tapstate.control.core;

import io.tapstate.core.catalog.CatalogEntryReader;
import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.dsl.DslError;
import io.tapstate.core.dsl.DslException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.AuditRecord;
import io.tapstate.spi.store.AuditStore;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.tuple;

/**
 * The resource-type-agnostic apply pipeline. {@code plan} validates a batch (structural, reference
 * closure, connector capability matrix, batch duplicate id) and emits each resource's canonical form
 * and content hash, touching no store. {@code apply} runs a plan and then upserts each artifact by id
 * into the store, skipping the write when the stored artifact's content hash is unchanged — the
 * idempotency key is the hash over the canonical form, so re-applying unchanged content writes
 * nothing. A validation failure aborts before any write.
 */
class ApplyServiceTest {

    private static final Clock FIXED_CLOCK = Clock.fixed(Instant.parse("2026-07-15T10:15:30Z"), ZoneOffset.UTC);

    private final RecordingArtifactStore store = new RecordingArtifactStore();
    private final RecordingAuditStore auditStore = new RecordingAuditStore();
    private final ApplyService service = new ApplyService(
            TapstateCatalog::load, store, new AuditGate(auditStore, FIXED_CLOCK), new EmptySchemaStore(),
            PlanAdvisories.none());

    /** An audit store that captures every record it is asked to write. */
    private static final class RecordingAuditStore implements AuditStore {
        final List<AuditRecord> records = new ArrayList<>();

        @Override
        public void record(AuditRecord record) {
            records.add(record);
        }
    }

    /** An audit store that always fails, standing in for an unavailable audit backend. */
    private static final class FailingAuditStore implements AuditStore {
        @Override
        public void record(AuditRecord record) {
            throw new IllegalStateException("audit backend down");
        }
    }

    // A guaranteed-valid mysql source used as a pure connection (X18 dual-role: no mode / tables).
    private static final String TGT_MY = """
            version: tapstate/v1
            kind: source
            id: tgt_my
            connector: mysql
            config: { host: 10.30.0.5, username: writer, password: My_2026 }
            """;

    /**
     * The refusal is reached through apply, which is the path an edit to a stored Source usually takes.
     *
     * <p>Guarding only the other write path would be a guard in name: the two services write through
     * different calls, so a check on one of them leaves the other wide open, and the open one here is
     * the one the command line uses.
     */
    @Test
    void applyRefusesToTurnTheReplayStoreOffWhileAPipelineReadingItIsUp() {
        service.apply("author", List.of(draft(BUFFERED_SRC), draft(READER_PIPELINE)));
        TestLifecycleStores.Desired desired = new TestLifecycleStores.Desired();
        TestLifecycleStores.State actual = new TestLifecycleStores.State();
        desired.put("p1", PipelineState.RUNNING);
        actual.put("p1", PipelineState.RUNNING);
        ApplyService guarded = new ApplyService(
                TapstateCatalog::load, store, new AuditGate(auditStore, FIXED_CLOCK),
                new EmptySchemaStore(), PlanAdvisories.none(), new LivePipelines(desired, actual));

        assertThatThrownBy(() -> guarded.apply("author", List.of(draft(UNBUFFERED_SRC))))
                .isInstanceOfSatisfying(TapstateException.class, refused ->
                        assertThat(refused.code()).isEqualTo(SourceError.SRS_CHANGE_WHILE_RUNNING));
    }

    /** The same edit lands once the pipeline reading the source is stopped. */
    @Test
    void applyAllowsTheChangeOnceThePipelineReadingItIsStopped() {
        service.apply("author", List.of(draft(BUFFERED_SRC), draft(READER_PIPELINE)));
        TestLifecycleStores.Desired desired = new TestLifecycleStores.Desired();
        TestLifecycleStores.State actual = new TestLifecycleStores.State();
        desired.put("p1", PipelineState.STOPPED);
        actual.put("p1", PipelineState.STOPPED);
        ApplyService guarded = new ApplyService(
                TapstateCatalog::load, store, new AuditGate(auditStore, FIXED_CLOCK),
                new EmptySchemaStore(), PlanAdvisories.none(), new LivePipelines(desired, actual));

        guarded.apply("author", List.of(draft(UNBUFFERED_SRC)));

        assertThat(stored("orders_src")).contains("enabled: false");
    }

    /** A cdc source whose changes are buffered through the shared replay store. */
    private static final String BUFFERED_SRC = """
            version: tapstate/v1
            kind: source
            id: orders_src
            connector: mysql
            mode: cdc
            config: { host: 10.30.0.5, username: writer, password: My_2026 }
            tables: [orders]
            srs: { enabled: true }
            """;

    /** The same source with the buffering turned off -- the one field the guard watches. */
    private static final String UNBUFFERED_SRC = BUFFERED_SRC.replace("enabled: true", "enabled: false");

    /**
     * The peer of the two cases above, for the half of the same switch that lives on the pipeline.
     *
     * <p>Guarding only the source side would now be a guard in name: after the switch moved, the value
     * the capture path actually reads is the pipeline's own, so an author who edits it there reaches
     * exactly the state the source-side refusal exists to prevent, by the shorter route.
     */
    @Test
    void applyRefusesToChangeAPipelinesOwnBufferingSwitchWhileThatPipelineIsUp() {
        service.apply("author", List.of(draft(BUFFERED_SRC), draft(READER_PIPELINE)));
        ApplyService guarded = guardedWith(PipelineState.RUNNING);

        assertThatThrownBy(() -> guarded.apply(
                "author", List.of(draft(BUFFERED_SRC), draft(PINNED_OFF_PIPELINE))))
                .isInstanceOfSatisfying(TapstateException.class, refused ->
                        assertThat(refused.code()).isEqualTo(SourceError.SRS_CHANGE_WHILE_RUNNING));
    }

    /** And it lands once that pipeline is stopped -- the refusal is about timing, not about the edit. */
    @Test
    void theSameEditToAPipelinesOwnSwitchLandsOnceItIsStopped() {
        service.apply("author", List.of(draft(BUFFERED_SRC), draft(READER_PIPELINE)));
        ApplyService guarded = guardedWith(PipelineState.STOPPED);

        guarded.apply("author", List.of(draft(BUFFERED_SRC), draft(PINNED_OFF_PIPELINE)));

        assertThat(stored("p1")).contains("srs: false");
    }

    /**
     * A live pipeline whose switch did not move is left alone. Without this, the guard could be
     * satisfied by refusing every edit to a running pipeline, which is a different rule and one this
     * layer does not make.
     */
    @Test
    void aLivePipelineEditedAnywhereElseIsNotRefused() {
        service.apply("author", List.of(draft(BUFFERED_SRC), draft(READER_PIPELINE)));
        ApplyService guarded = guardedWith(PipelineState.RUNNING);

        assertThatCode(() -> guarded.apply("author", List.of(
                draft(BUFFERED_SRC), draft(READER_PIPELINE.replace("op != 'd'", "op != 'u'")))))
                .doesNotThrowAnyException();
    }

    /**
     * Recording a switch for the first time is materialization, not an edit: it is how the value gets
     * onto the artifact at all, so a pipeline that is already up must not be refused for it. This is the
     * case a guard written as "the stored and incoming switches differ" gets wrong, because before the
     * first apply the stored one is absent rather than equal.
     */
    @Test
    void aFirstRecordingOnALivePipelineIsNotRefused() {
        // Land a pipeline with no switch recorded at all, the way one stored before this field existed
        // reads, then bring it up and re-apply the same text.
        service.apply("author", List.of(draft(BUFFERED_SRC), draft(READER_PIPELINE)));
        store.landDirectly(new DslParser().parse(READER_PIPELINE));
        ApplyService guarded = guardedWith(PipelineState.RUNNING);

        assertThatCode(() -> guarded.apply(
                "author", List.of(draft(BUFFERED_SRC), draft(READER_PIPELINE))))
                .doesNotThrowAnyException();
    }

    private ApplyService guardedWith(PipelineState state) {
        TestLifecycleStores.Desired desired = new TestLifecycleStores.Desired();
        TestLifecycleStores.State actual = new TestLifecycleStores.State();
        desired.put("p1", state);
        actual.put("p1", state);
        return new ApplyService(
                TapstateCatalog::load, store, new AuditGate(auditStore, FIXED_CLOCK),
                new EmptySchemaStore(), PlanAdvisories.none(), new LivePipelines(desired, actual));
    }

    /** A pipeline reading that source, in the minimal valid shape. */
    private static final String READER_PIPELINE = """
            version: tapstate/v1
            kind: pipeline
            id: p1
            source: orders_src
            transforms:
              - id: selected
                type: filter
                from: [orders]
                expr: "op != 'd'"
            view:
              id: orders_view
              from: selected
              primary_key: id
              storage:
                warm:
                  collection: orders_view
            """;

    /** The same pipeline with its own switch for that source written off. */
    private static final String PINNED_OFF_PIPELINE =
            READER_PIPELINE.replace("source: orders_src", "source: [ { id: orders_src, srs: false } ]");

    private static ArtifactDraft draft(String content) {
        return new ArtifactDraft(null, content);
    }

    /** A draft carrying the version of the stored artifact the author based this edit on. */
    private static ArtifactDraft draft(String content, String expectedContentHash) {
        return new ArtifactDraft(null, content, expectedContentHash);
    }

    /** The canonical text a stored artifact currently holds. */
    private String stored(String id) {
        return new CanonicalWriter().write(store.get(id).orElseThrow());
    }

    /** The canonical text the given authored YAML would be stored as. */
    private static String canonicalOf(String yaml) {
        return new CanonicalWriter().write(new DslParser().parse(yaml));
    }

    // ---- the store-free front half: validate -> canonical -> hash ----

    @Test
    void planCanonicalizesAndHashesAValidResource() {
        ApplyPlan plan = service.plan(List.of(draft(TGT_MY)));

        assertThat(plan.artifacts()).hasSize(1);
        PreparedArtifact prepared = plan.artifacts().get(0);
        assertThat(prepared.id()).isEqualTo("tgt_my");
        assertThat(prepared.kind()).isEqualTo("source");
        String expectedCanonical = new CanonicalWriter().write(prepared.resource());
        assertThat(prepared.canonicalForm())
                .as("the canonical form is the deterministic serializer's output")
                .isEqualTo(expectedCanonical);
        assertThat(prepared.contentHash())
                .as("the content hash is taken over the canonical form")
                .isEqualTo(CanonicalHash.of(expectedCanonical));
    }

    @Test
    void validateReportsThePlannedChangesWithoutWritingOrAuditing() {
        service.apply("alice", List.of(draft(TGT_MY)));
        int writesBeforeValidation = store.saveCount;
        auditStore.records.clear();

        ArtifactValidationResult result = service.validate(List.of(
                draft(TGT_MY), draft(SRC_ORA_STANDALONE)));

        assertThat(result.valid()).isTrue();
        assertThat(result.diagnostics()).isEmpty();
        assertThat(result.outcomes()).extracting(ArtifactOutcome::id)
                .containsExactly("tgt_my", "src_ora");
        assertThat(result.outcomes()).extracting(ArtifactOutcome::change)
                .containsExactly(ArtifactOutcome.Change.UNCHANGED, ArtifactOutcome.Change.CREATED);
        assertThat(store.saveCount).isEqualTo(writesBeforeValidation);
        assertThat(auditStore.records).isEmpty();
    }

    @Test
    void invalidValidationReturnsOneStructuredDiagnosticWithoutWritingOrAuditing() {
        ArtifactValidationResult result = service.validate(List.of(
                draft(TGT_MY + "bogus_field: 1\n")));

        assertThat(result.valid()).isFalse();
        assertThat(result.outcomes()).isEmpty();
        assertThat(result.diagnostics()).singleElement().satisfies(diagnostic -> {
            assertThat(diagnostic.code()).isEqualTo("dsl.unknown-field");
            assertThat(diagnostic.params()).containsEntry("field", "bogus_field");
        });
        assertThat(store.saveCount).isZero();
        assertThat(auditStore.records).isEmpty();
    }

    @Test
    void anUnknownFieldIsRejectedAtValidationWithItsDslCode() {
        // The structural tier: a field outside the tapstate/v1 schema.
        String withUnknownField = TGT_MY + "bogus_field: 1\n";

        Throwable t = catchThrowable(() -> service.plan(List.of(draft(withUnknownField))));

        assertThat(t).isInstanceOf(DslException.class);
        assertThat(((DslException) t).code()).isEqualTo(DslError.UNKNOWN_FIELD);
    }

    @Test
    void aMissingReferenceInTheBatchIsRejected() {
        // The reference-closure tier: a pipeline whose source is not in the batch.
        String pipeline = """
                version: tapstate/v1
                kind: pipeline
                id: mirror
                source: src_absent
                serve:
                  from: /.*/
                  sync:
                    - id: out
                      source: tgt_absent
                      write_mode: upsert
                """;

        Throwable t = catchThrowable(() -> service.plan(List.of(draft(pipeline))));

        assertThat(t).isInstanceOf(DslException.class);
        assertThat(((DslException) t).code()).isEqualTo(DslError.MISSING_REFERENCE);
    }

    @Test
    void aCapabilityViolationIsRejected() {
        // The capability-matrix tier: kafka declares only [stream]; cdc is outside its matrix.
        String kafkaCdc = """
                version: tapstate/v1
                kind: source
                id: src_k
                connector: kafka
                config: { nameSrvAddr: "k1:9092" }
                mode: cdc
                tables: [ events ]
                """;

        Throwable t = catchThrowable(() -> service.plan(List.of(draft(kafkaCdc))));

        assertThat(t).isInstanceOf(DslException.class);
        assertThat(((DslException) t).code()).isEqualTo(DslError.UNSUPPORTED_MODE);
    }

    // A registered connector 'acme' (absent from the bundled snapshot) whose declared source modes differ
    // between the two rows — [cdc] then [snapshot] — so a cdc source is legal against the first, illegal
    // against the second; and a config field 'host' so the source's config validates.
    private static String acmeRow(String mode) {
        return ("""
                {
                  "id": "acme", "name": "Acme", "displayName": "Acme", "icon": null,
                  "group": "database", "modes": ["%MODE%"], "discovery": "catalog",
                  "sink": {"capable": false, "writeSemantics": []}, "pushOut": false,
                  "config": [{"name": "host", "type": "string", "label": {}, "required": false,
                    "default": null, "secret": false, "options": [], "visibleWhen": null}],
                  "provenance": {"specPath": "spec.json", "specContentHash": "h",
                    "pdkApiVersion": "1.0.0", "requiredLevel": null, "modeSource": {"%MODE%": "declared"}}
                }
                """).replace("%MODE%", mode);
    }

    private static final String ACME_CDC_SOURCE = """
            version: tapstate/v1
            kind: source
            id: src_a
            connector: acme
            config: { host: db }
            mode: cdc
            tables: [ orders ]
            """;

    @Test
    void planValidatesAgainstTheLiveMergedViewSoARuntimeRegisteredConnectorIsHonoured() {
        // The change's headline: plan() reads the catalog supplier per call, so a connector registered at
        // runtime is honoured without a restart and its capability matrix is enforced live. acme is
        // registered supporting [cdc] -> a cdc source validates; re-registered supporting only [snapshot] ->
        // the same source now violates its matrix. This fails if plan captured the catalog once (both plans
        // would see [cdc]) or were reverted to the fixed bundled snapshot (acme absent, the flip impossible).
        List<ConnectorCatalogEntry> registered = new ArrayList<>();
        registered.add(CatalogEntryReader.read(acmeRow("cdc")));
        Supplier<TapstateCatalog> live = () -> TapstateCatalog.merged(TapstateCatalog.load(), List.copyOf(registered));
        ApplyService liveService = new ApplyService(
                live, store, new AuditGate(auditStore, FIXED_CLOCK), new EmptySchemaStore(), PlanAdvisories.none());

        assertThatCode(() -> liveService.plan(List.of(draft(ACME_CDC_SOURCE)))).doesNotThrowAnyException();

        registered.clear();
        registered.add(CatalogEntryReader.read(acmeRow("snapshot")));

        Throwable t = catchThrowable(() -> liveService.plan(List.of(draft(ACME_CDC_SOURCE))));
        assertThat(t).isInstanceOf(DslException.class);
        assertThat(((DslException) t).code()).isEqualTo(DslError.UNSUPPORTED_MODE);
    }

    @Test
    void anUnknownKindIsRejectedWithACodedDiagnostic() {
        // An illegal resource must surface a coded dsl.* diagnostic at validation, never an uncoded
        // exception that a caller (rest-api) would map to a 500 for a plain user typo.
        String unknownKind = """
                version: tapstate/v1
                kind: bogus
                id: x
                """;

        Throwable t = catchThrowable(() -> service.plan(List.of(draft(unknownKind))));

        assertThat(t).isInstanceOf(DslException.class);
        assertThat(((DslException) t).code()).isEqualTo(DslError.ILLEGAL_VALUE);
    }

    @Test
    void aDuplicateIdAcrossTheBatchIsRejected() {
        Throwable t = catchThrowable(() -> service.plan(List.of(draft(TGT_MY), draft(TGT_MY))));

        assertThat(t).isInstanceOf(DslException.class);
        assertThat(((DslException) t).code()).isEqualTo(DslError.DUPLICATE_ID);
    }

    @Test
    void aParseErrorIsAttributedToItsDraftSource() {
        String withUnknownField = TGT_MY + "bogus_field: 1\n";

        Throwable t = catchThrowable(() ->
                service.plan(List.of(new ArtifactDraft("tgt_my.tap.yml", withUnknownField))));

        assertThat(t).isInstanceOf(DslException.class);
        assertThat(((DslException) t).source())
                .as("a parse error is located at its originating draft")
                .isEqualTo("tgt_my.tap.yml");
    }

    @Test
    void theContentHashIsOverTheCanonicalFormNotTheRawText() {
        // Same resource, different raw key order in config — canonical sorts free-map keys, so both
        // canonicalize identically and must hash identically (the idempotency key property).
        String reordered = """
                version: tapstate/v1
                kind: source
                id: tgt_my
                connector: mysql
                config: { password: My_2026, username: writer, host: 10.30.0.5 }
                """;

        String hashA = service.plan(List.of(draft(TGT_MY))).artifacts().get(0).contentHash();
        String hashB = service.plan(List.of(draft(reordered))).artifacts().get(0).contentHash();

        assertThat(hashA).isEqualTo(hashB);
    }

    @Test
    void aMultiResourceWorkspaceIsPreparedPerResource() {
        ApplyPlan plan = service.plan(List.of(draft(SRC_ORA), draft(PIPELINE), draft(TGT_MY)));

        assertThat(plan.artifacts()).extracting(PreparedArtifact::id)
                .containsExactly("src_ora", "ora2my_ods", "tgt_my");
        assertThat(plan.artifacts()).allSatisfy(a ->
                assertThat(a.contentHash()).matches("[0-9a-f]{64}"));
    }

    @Test
    void anEmptyBatchProducesAnEmptyPlan() {
        assertThat(service.plan(List.of()).artifacts()).isEmpty();
    }

    @Test
    void aNullCatalogIsRejected() {
        assertThatThrownBy(() -> new ApplyService(
                null, store, new AuditGate(auditStore, FIXED_CLOCK), new EmptySchemaStore(), PlanAdvisories.none()))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void aNullStoreIsRejected() {
        assertThatThrownBy(() -> new ApplyService(
                TapstateCatalog::load, null, new AuditGate(auditStore, FIXED_CLOCK), new EmptySchemaStore(),
                PlanAdvisories.none()))
                .isInstanceOf(NullPointerException.class);
    }

    // ---- upsert by id, hash-unchanged = no-op ----

    @Test
    void applyToAnEmptyStoreCreatesTheResourceAndWritesOnce() {
        ApplyResult result = service.apply("alice", List.of(draft(TGT_MY)));

        assertThat(result.outcomes()).singleElement().satisfies(o -> {
            assertThat(o.id()).isEqualTo("tgt_my");
            assertThat(o.kind()).isEqualTo("source");
            assertThat(o.change()).isEqualTo(ArtifactOutcome.Change.CREATED);
            assertThat(o.contentHash()).matches("[0-9a-f]{64}");
        });
        assertThat(store.saveCount).as("a create writes exactly once").isEqualTo(1);
        assertThat(store.get("tgt_my")).isPresent();
    }

    @Test
    void reapplyingIdenticalContentIsANoOpAndDoesNotWrite() {
        // The core no-op guarantee: applying the same resource twice writes only once; the second apply
        // reads the stored artifact, finds an equal content hash, and skips the store write.
        service.apply("alice", List.of(draft(TGT_MY)));

        ApplyResult second = service.apply("alice", List.of(draft(TGT_MY)));

        assertThat(second.outcomes()).singleElement()
                .extracting(ArtifactOutcome::change).isEqualTo(ArtifactOutcome.Change.UNCHANGED);
        assertThat(store.saveCount).as("re-applying unchanged content performs no second write").isEqualTo(1);
    }

    @Test
    void theNoOpIsKeyedByCanonicalHashNotRawText() {
        // Re-apply the same resource with a different raw config key order. It canonicalizes and hashes
        // identically, so it is still a no-op — the idempotency key is the hash over the canonical form.
        String reordered = """
                version: tapstate/v1
                kind: source
                id: tgt_my
                connector: mysql
                config: { password: My_2026, username: writer, host: 10.30.0.5 }
                """;
        service.apply("alice", List.of(draft(TGT_MY)));

        ApplyResult second = service.apply("alice", List.of(draft(reordered)));

        assertThat(second.outcomes()).singleElement()
                .extracting(ArtifactOutcome::change).isEqualTo(ArtifactOutcome.Change.UNCHANGED);
        assertThat(store.saveCount).isEqualTo(1);
    }

    @Test
    void reapplyingChangedContentUpdatesAndWritesAgain() {
        String changed = """
                version: tapstate/v1
                kind: source
                id: tgt_my
                connector: mysql
                config: { host: 10.30.0.5, username: writer, password: Changed_2026 }
                """;
        service.apply("alice", List.of(draft(TGT_MY)));

        ApplyResult second = service.apply("alice", List.of(draft(changed)));

        assertThat(second.outcomes()).singleElement()
                .extracting(ArtifactOutcome::change).isEqualTo(ArtifactOutcome.Change.UPDATED);
        assertThat(store.saveCount).as("changed content writes a second time").isEqualTo(2);
        // The store now holds the changed canonical form (server-as-truth: the last write wins).
        assertThat(new CanonicalWriter().write(store.get("tgt_my").orElseThrow()))
                .contains("Changed_2026");
    }

    @Test
    void aMultiResourceBatchUpsertsEachByIdInSubmissionOrder() {
        ApplyResult result = service.apply("alice", List.of(draft(SRC_ORA), draft(PIPELINE), draft(TGT_MY)));

        assertThat(result.outcomes()).extracting(ArtifactOutcome::id)
                .containsExactly("src_ora", "ora2my_ods", "tgt_my");
        assertThat(result.outcomes()).extracting(ArtifactOutcome::change)
                .containsOnly(ArtifactOutcome.Change.CREATED);
        assertThat(store.saveCount).isEqualTo(3);
    }

    @Test
    void aMixedBatchWritesOnlyTheChangedAndNewResources() {
        // Seed tgt_my. Then apply a batch of [tgt_my unchanged, src_ora new]: only the new resource is
        // written — the no-op is decided per artifact, not per batch.
        service.apply("alice", List.of(draft(TGT_MY)));
        assertThat(store.saveCount).isEqualTo(1);

        ApplyResult result = service.apply("alice", List.of(draft(TGT_MY), draft(SRC_ORA_STANDALONE)));

        assertThat(result.outcomes()).extracting(ArtifactOutcome::id).containsExactly("tgt_my", "src_ora");
        assertThat(result.outcomes()).extracting(ArtifactOutcome::change)
                .containsExactly(ArtifactOutcome.Change.UNCHANGED, ArtifactOutcome.Change.CREATED);
        assertThat(store.saveCount).as("only the new resource is written").isEqualTo(2);
    }

    @Test
    void anInvalidResourceInTheBatchAbortsBeforeAnyWrite() {
        // Validation runs over the whole batch before any upsert, so an invalid member leaves the store
        // untouched — no partial write. This is the validation-failure half of atomic batch; the
        // write-failure half is asserted by aWriteFailureMidBatchLeavesTheStoreUnchanged.
        String bad = TGT_MY + "bogus_field: 1\n";

        Throwable t = catchThrowable(() -> service.apply("alice", List.of(draft(SRC_ORA), draft(bad))));

        assertThat(t).isInstanceOf(DslException.class);
        assertThat(store.saveCount).as("a validation failure writes nothing").isZero();
        assertThat(store.get("src_ora")).isEmpty();
    }

    // ---- atomic batch: a write failure mid-batch rolls the whole batch back ----

    @Test
    void aWriteFailureMidBatchLeavesTheStoreUnchanged() {
        // Both resources are valid, so the batch reaches the write phase; the store then fails on the
        // second write. Because apply hands the whole changed set to one atomic saveAll, the failure
        // rolls the batch back and nothing is stored — not even the first, earlier-ordered resource.
        store.failOnId = "tgt_my";

        Throwable t = catchThrowable(() -> service.apply("alice", List.of(draft(SRC_ORA), draft(TGT_MY))));

        assertThat(t).isInstanceOf(RuntimeException.class);
        assertThat(store.get("src_ora")).as("the earlier-ordered write is rolled back, not left partial").isEmpty();
        assertThat(store.get("tgt_my")).isEmpty();
        assertThat(store.list()).isEmpty();
    }

    @Test
    void applyWritesTheChangedSetAsOneAtomicBatch() {
        // Two new resources in one apply are written as a single atomic batch, not one write per
        // artifact: the store records exactly one batch carrying both ids in submission order.
        service.apply("alice", List.of(draft(SRC_ORA), draft(TGT_MY)));

        assertThat(store.saveAllBatches).containsExactly(List.of("src_ora", "tgt_my"));
    }

    // ---- no audit, no execute: apply is an audited write and leaves a record per changed artifact ----

    @Test
    void applyRecordsOneAuditEntryPerChangedArtifactAttributedToItsOwnId() {
        // artifact.apply is a registered audited verb, so the write must leave an audit record — and the
        // record's resourceId names the artifact it changed, so the log answers "who changed which one".
        service.apply("alice", List.of(draft(SRC_ORA), draft(TGT_MY)));

        assertThat(auditStore.records).hasSize(2);
        assertThat(auditStore.records).allSatisfy(record -> {
            assertThat(record.operationId()).isEqualTo("artifact.apply");
            assertThat(record.principal()).isEqualTo("alice");
        });
        assertThat(auditStore.records).extracting(AuditRecord::resourceId)
                .containsExactly("src_ora", "tgt_my");
    }

    @Test
    void aVersionCheckedApplyRecordsTheVersionItDeclaredItWasEditing() {
        // Without this the record is byte-identical to a blind overwrite of the same id, so the log
        // cannot answer whether the writer had read what it replaced — which is the question a
        // precondition exists to make answerable in the first place.
        String current = service.apply("alice", List.of(draft(TGT_MY)))
                .outcomes().get(0).contentHash();
        auditStore.records.clear();

        service.apply("alice", List.of(draft(TGT_MY_EDITED, current)));

        assertThat(auditStore.records).singleElement().satisfies(record -> {
            assertThat(record.resourceId()).isEqualTo("tgt_my");
            assertThat(record.expectedContentHash()).isEqualTo(current);
        });
    }

    @Test
    void anApplyThatDeclaredNoVersionRecordsNone() {
        // The absence has to mean something: a record with no declared version is the unconditional
        // overwrite, and filling in the stored hash here would make every apply look version-checked.
        service.apply("alice", List.of(draft(TGT_MY)));

        assertThat(auditStore.records).singleElement()
                .satisfies(record -> assertThat(record.expectedContentHash()).isNull());
    }

    @Test
    void aBatchRecordsEachArtifactsOwnDeclaredVersionAndNotAnothers() {
        // The drafts are matched to ids by what each one parsed to, not by position in the plan: the
        // plan is built from the validated workspace, whose order is its own. Getting this wrong files
        // one artifact's declared version under a different artifact's record.
        String current = service.apply("alice", List.of(draft(TGT_MY)))
                .outcomes().get(0).contentHash();
        auditStore.records.clear();

        service.apply("alice", List.of(draft(SRC_ORA_STANDALONE), draft(TGT_MY_EDITED, current)));

        assertThat(auditStore.records)
                .extracting(AuditRecord::resourceId, AuditRecord::expectedContentHash)
                .containsExactlyInAnyOrder(
                        tuple("src_ora", null),
                        tuple("tgt_my", current));
    }

    @Test
    void applyRecordsNoAuditEntryForAnUnchangedArtifact() {
        // The no-op changes nothing, so it is not an auditable effect: re-applying identical content
        // leaves no second record, exactly as it performs no second write.
        service.apply("alice", List.of(draft(TGT_MY)));
        assertThat(auditStore.records).hasSize(1);

        service.apply("alice", List.of(draft(TGT_MY)));

        assertThat(auditStore.records)
                .as("a no-op apply mutates nothing and so records nothing")
                .hasSize(1);
    }

    @Test
    void aMixedBatchRecordsOnlyTheChangedArtifacts() {
        service.apply("alice", List.of(draft(TGT_MY)));
        auditStore.records.clear();

        service.apply("alice", List.of(draft(TGT_MY), draft(SRC_ORA_STANDALONE)));

        assertThat(auditStore.records).extracting(AuditRecord::resourceId)
                .as("the audit log mirrors the write batch — only what actually changed")
                .containsExactly("src_ora");
    }

    @Test
    void anAuditWriteFailureRefusesTheApplyAndLeavesTheStoreUntouched() {
        // No audit, no execute: if the record cannot be written the apply is refused with a coded
        // control.audit-blocked and nothing reaches the artifact store.
        ApplyService refusing = new ApplyService(
                TapstateCatalog::load, store, new AuditGate(new FailingAuditStore(), FIXED_CLOCK),
                new EmptySchemaStore(), PlanAdvisories.none());

        Throwable t = catchThrowable(() -> refusing.apply("alice", List.of(draft(TGT_MY))));

        assertThat(t).isInstanceOf(TapstateException.class);
        assertThat(((TapstateException) t).code()).isEqualTo(ControlError.AUDIT_BLOCKED);
        assertThat(store.saveCount).as("an unaudited apply does not execute").isZero();
        assertThat(store.get("tgt_my")).isEmpty();
    }

    @Test
    void aNullAuditGateIsRejected() {
        assertThatThrownBy(() -> new ApplyService(
                TapstateCatalog::load, store, null, new EmptySchemaStore(), PlanAdvisories.none()))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void aNullSchemaStoreIsRejected() {
        // The row-expression type check is not optional: a service built without a schema store would
        // silently skip it, and skipping it is exactly the state the check exists to end.
        assertThatThrownBy(() -> new ApplyService(
                TapstateCatalog::load, store, new AuditGate(auditStore, FIXED_CLOCK), null, PlanAdvisories.none()))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void applyExcludesUnchangedResourcesFromTheWriteBatch() {
        // Seed tgt_my, then apply [tgt_my unchanged, src_ora new]: the one atomic batch carries only the
        // new resource — the unchanged one is not rewritten.
        service.apply("alice", List.of(draft(TGT_MY)));

        service.apply("alice", List.of(draft(TGT_MY), draft(SRC_ORA_STANDALONE)));

        assertThat(store.saveAllBatches).containsExactly(List.of("tgt_my"), List.of("src_ora"));
    }

    @Test
    void anAllNoOpBatchPerformsNoWrite() {
        // A batch whose every member is unchanged writes nothing: the changed set is empty, so the one
        // atomic batch apply performs carries no resources.
        service.apply("alice", List.of(draft(SRC_ORA), draft(TGT_MY)));
        int writesAfterSeed = store.saveCount;

        ApplyResult result = service.apply("alice", List.of(draft(SRC_ORA), draft(TGT_MY)));

        assertThat(result.outcomes()).extracting(ArtifactOutcome::change)
                .containsOnly(ArtifactOutcome.Change.UNCHANGED);
        assertThat(store.saveCount).as("a wholly-unchanged batch writes nothing further").isEqualTo(writesAfterSeed);
    }

    // ---- the advisory channel: findings that inform the author without refusing the batch ----

    @Test
    void aPlannedBatchCarriesTheAdvisoryFindingsAndStillPreparesEveryArtifact() {
        ApplyService advised = advisedBy(reporting(WIDE_NAMESPACE));

        ApplyPlan plan = advised.plan(List.of(draft(TGT_MY), draft(SRC_ORA_STANDALONE)));

        assertThat(plan.warnings()).containsExactly(WIDE_NAMESPACE);
        assertThat(plan.artifacts()).extracting(PreparedArtifact::id)
                .as("an advisory finding is a note, not a refusal — the batch is planned in full")
                .containsExactly("tgt_my", "src_ora");
    }

    @Test
    void validationReportsAdvisoryFindingsApartFromTheDiagnostics() {
        // The two lists answer different questions — "why was this refused" and "what should you know
        // about a batch that was not". A caller must never have to read a severity field to tell them
        // apart, so a finding lands in warnings and leaves valid / diagnostics untouched.
        ApplyService advised = advisedBy(reporting(WIDE_NAMESPACE));

        ArtifactValidationResult result = advised.validate(List.of(draft(TGT_MY)));

        assertThat(result.valid()).as("a finding never invalidates the batch").isTrue();
        assertThat(result.diagnostics()).as("a finding is not merged into the refusal reasons").isEmpty();
        assertThat(result.warnings()).containsExactly(WIDE_NAMESPACE);
    }

    @Test
    void applyCarriesTheAdvisoryFindingsAndStillWritesTheBatch() {
        ApplyService advised = advisedBy(reporting(WIDE_NAMESPACE));

        ApplyResult result = advised.apply("alice", List.of(draft(TGT_MY)));

        assertThat(result.warnings()).containsExactly(WIDE_NAMESPACE);
        assertThat(result.outcomes()).extracting(ArtifactOutcome::change)
                .containsExactly(ArtifactOutcome.Change.CREATED);
        assertThat(store.get("tgt_my")).as("a warned apply is still an apply").isPresent();
    }

    @Test
    void aRefusedBatchIsNeverReviewedForAdvisoryFindings() {
        // There is no plan to review when validation failed, and the refusal is already the message. A
        // rule that ran anyway would be judging half-parsed resources and would bury the actual reason.
        List<List<Resource>> reviewed = new ArrayList<>();
        ApplyService advised = advisedBy((resources, tablesBySource) -> {
            reviewed.add(resources);
            return List.of(WIDE_NAMESPACE);
        });

        ArtifactValidationResult result = advised.validate(List.of(draft(TGT_MY + "bogus_field: 1\n")));

        assertThat(result.valid()).isFalse();
        assertThat(result.diagnostics()).extracting(ValidationDiagnostic::code)
                .containsExactly("dsl.unknown-field");
        assertThat(result.warnings()).as("a batch that was refused carries no advisory findings").isEmpty();
        assertThat(reviewed).as("the advisory pass is not reached at all").isEmpty();
    }

    @Test
    void anAssemblyWithNoAdvisoryRulesReportsNoWarningsAnywhere() {
        // The positive control for the three cases above: the same batches, reviewed by the no-op pass,
        // report nothing — so a populated warnings list can only have come from the advisory rules.
        assertThat(service.plan(List.of(draft(TGT_MY))).warnings()).isEmpty();
        assertThat(service.validate(List.of(draft(TGT_MY))).warnings()).isEmpty();
        assertThat(service.apply("alice", List.of(draft(TGT_MY))).warnings()).isEmpty();
    }

    @Test
    void aNullAdvisoryPassIsRejected() {
        // A service built without one would silently answer "nothing to report" for every batch, which
        // is indistinguishable from a clean batch — the assembly must name the no-op pass to get it.
        assertThatThrownBy(() -> new ApplyService(
                TapstateCatalog::load, store, new AuditGate(auditStore, FIXED_CLOCK), new EmptySchemaStore(), null))
                .isInstanceOf(NullPointerException.class);
    }

    /** A stand-in advisory rule that reports the same findings for every batch it is handed. */
    private static PlanAdvisories reporting(ValidationDiagnostic... findings) {
        return (resources, tablesBySource) -> List.of(findings);
    }

    /** The service under test, wired to one advisory pass instead of the default no-op. */
    private ApplyService advisedBy(PlanAdvisories advisories) {
        return new ApplyService(
                TapstateCatalog::load, store, new AuditGate(auditStore, FIXED_CLOCK), new EmptySchemaStore(),
                advisories);
    }

    // ---- optimistic concurrency: an optional per-draft precondition on apply ----

    @Test
    void applyWithoutAPreconditionBehavesExactlyAsItDidBefore() {
        // The compatibility case: an existing caller passes no hash and keeps overwriting whatever is
        // stored. Without this, adding the precondition could silently start refusing today's callers.
        service.apply("alice", List.of(draft(TGT_MY)));

        ApplyResult result = service.apply("alice", List.of(draft(TGT_MY_EDITED)));

        assertThat(result.outcomes()).extracting(ArtifactOutcome::change)
                .containsExactly(ArtifactOutcome.Change.UPDATED);
        assertThat(stored("tgt_my")).isEqualTo(canonicalOf(TGT_MY_EDITED));
    }

    @Test
    void applyWithTheCurrentPreconditionUpdatesTheArtifact() {
        String current = service.apply("alice", List.of(draft(TGT_MY)))
                .outcomes().get(0).contentHash();

        ApplyResult result = service.apply("alice", List.of(draft(TGT_MY_EDITED, current)));

        assertThat(result.outcomes()).extracting(ArtifactOutcome::change)
                .containsExactly(ArtifactOutcome.Change.UPDATED);
        assertThat(stored("tgt_my")).isEqualTo(canonicalOf(TGT_MY_EDITED));
    }

    @Test
    void applyWithAStalePreconditionIsRefusedWithTheStoredBytesUnchanged() {
        service.apply("alice", List.of(draft(TGT_MY)));
        String before = stored("tgt_my");
        int writesBefore = store.saveCount;

        assertThatThrownBy(() -> service.apply("alice", List.of(draft(TGT_MY_EDITED, "0".repeat(64)))))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(ArtifactError.VERSION_CONFLICT);
                    assertThat(error.args()).containsExactlyInAnyOrderEntriesOf(Map.of("id", "tgt_my"));
                });
        assertThat(stored("tgt_my"))
                .as("a refused apply leaves the stored canonical bytes exactly as they were")
                .isEqualTo(before);
        assertThat(store.saveCount).isEqualTo(writesBefore);
    }

    @Test
    void oneStaleDraftRefusesTheWholeBatchIncludingItsValidSiblings() {
        service.apply("alice", List.of(draft(TGT_MY)));
        int writesBefore = store.saveCount;

        assertThatThrownBy(() -> service.apply("alice", List.of(
                draft(SRC_ORA_STANDALONE), draft(TGT_MY_EDITED, "0".repeat(64)))))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(ArtifactError.VERSION_CONFLICT));
        assertThat(store.get("src_ora"))
                .as("the batch is one closure, so a sibling of a stale draft is not written either")
                .isEmpty();
        assertThat(store.saveCount).isEqualTo(writesBefore);
    }

    @Test
    void aPreconditionOnAnArtifactThatIsNotStoredIsRefusedAsNotFound() {
        assertThatThrownBy(() -> service.apply("alice", List.of(draft(TGT_MY, "0".repeat(64)))))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(ArtifactError.NOT_FOUND);
                    assertThat(error.args()).containsExactlyInAnyOrderEntriesOf(Map.of("id", "tgt_my"));
                });
        assertThat(store.saveCount).isZero();
    }

    @Test
    void aWriterThatLandsAfterThePreconditionWasComparedStillRefusesTheApply() {
        // The lost update the precondition exists to prevent, in the shape it actually happens in: not
        // "the version was already stale when the author submitted" — plan() catches that — but "it went
        // stale while this apply was validating". plan() runs a whole workspace validation and a schema
        // read between its comparison and the write, so the window is wide enough to lose a real edit.
        //
        // This is what discriminates a precondition that is only checked from one that is enforced: an
        // unconditional batch write passes every other test in this class and fails only this one.
        String readByBoth = service.apply("alice", List.of(draft(TGT_MY))).outcomes().get(0).contentHash();
        Resource alicesEdit = new DslParser().parse(TGT_MY_EDITED);
        store.concurrentWriter = () -> store.landDirectly(alicesEdit);

        assertThatThrownBy(() -> service.apply("bob", List.of(draft(TGT_MY_EDITED_AGAIN, readByBoth))))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(ArtifactError.VERSION_CONFLICT);
                    assertThat(error.args()).containsExactlyInAnyOrderEntriesOf(Map.of("id", "tgt_my"));
                });
        assertThat(stored("tgt_my"))
                .as("the edit that landed first survives; the loser is refused rather than silently dropped")
                .isEqualTo(canonicalOf(TGT_MY_EDITED));
    }

    @Test
    void anApplyWithNoDeclaredVersionIsStillWrittenWhenAnotherWriterLandsFirst() {
        // The other half of the same window: a caller that declared nothing asked for no check, and
        // must keep overwriting exactly as it always did. Guarding an unasked-for precondition would
        // turn every concurrent apply into a refusal.
        service.apply("alice", List.of(draft(TGT_MY)));
        Resource alicesEdit = new DslParser().parse(TGT_MY_EDITED);
        store.concurrentWriter = () -> store.landDirectly(alicesEdit);

        ApplyResult result = service.apply("bob", List.of(draft(TGT_MY_EDITED_AGAIN)));

        assertThat(result.outcomes()).extracting(ArtifactOutcome::change)
                .containsExactly(ArtifactOutcome.Change.UPDATED);
        assertThat(stored("tgt_my")).isEqualTo(canonicalOf(TGT_MY_EDITED_AGAIN));
    }

    @Test
    void validateReportsAStalePreconditionAsADiagnosticRatherThanThrowing() {
        service.apply("alice", List.of(draft(TGT_MY)));

        ArtifactValidationResult result = service.validate(
                List.of(draft(TGT_MY_EDITED, "0".repeat(64))));

        assertThat(result.valid()).isFalse();
        assertThat(result.diagnostics()).singleElement().satisfies(diagnostic ->
                assertThat(diagnostic.code()).isEqualTo("artifact.version-conflict"));
    }

    // ---- fixtures ----

    /** A stand-in finding, shaped like the capacity precheck's: a code plus the named subject it is about. */
    private static final ValidationDiagnostic WIDE_NAMESPACE =
            new ValidationDiagnostic("nest.resident-demand-over-budget", Map.of("path", "orders.items"));

    private static final String SRC_ORA = """
            version: tapstate/v1
            kind: source
            id: src_ora
            connector: oracle
            config: { host: 10.20.0.15, port: 1521, service_name: ORCL,
                      username: cdc_user, password: Ora_2026 }
            mode: cdc
            tables: [ ORDERS, ORDER_ITEMS, CUSTOMERS ]
            options: { include_ddl: true }
            """;

    // The same oracle source with no pipeline referencing it — a standalone resource for batch tests.
    private static final String SRC_ORA_STANDALONE = SRC_ORA;

    // TGT_MY after an edit: same id, one changed connection field, so it is an update rather than a no-op.
    private static final String TGT_MY_EDITED = """
            version: tapstate/v1
            kind: source
            id: tgt_my
            connector: mysql
            config: { host: 10.30.0.9, username: writer, password: My_2026 }
            """;

    // A third version of the same id, so two authors' edits can be told apart in the store.
    private static final String TGT_MY_EDITED_AGAIN = """
            version: tapstate/v1
            kind: source
            id: tgt_my
            connector: mysql
            config: { host: 10.30.0.11, username: writer, password: My_2026 }
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
     * An in-memory {@link ArtifactStore} that mirrors the Mongo store's canonical round-trip — it holds
     * each artifact as its canonical text and reconstructs it on read through the parser — and models
     * the store's atomic batch write: {@code saveAll} stages the whole batch and commits it only if none
     * of it is the injected poison id, so a mid-batch failure leaves nothing written, exactly as the
     * real transaction does. It records each batch (by id) and counts resources written so a test can
     * assert the write set and that a no-op performs no store write.
     */
    private static final class RecordingArtifactStore implements ArtifactStore {

        private final CanonicalWriter writer = new CanonicalWriter();
        private final DslParser parser = new DslParser();
        private final Map<String, String> byId = new LinkedHashMap<>();
        private final List<List<String>> saveAllBatches = new ArrayList<>();
        private int saveCount = 0;
        private String failOnId = null;
        /** A writer that commits between the plan's comparison and this store's write. */
        private Runnable concurrentWriter = null;

        @Override
        public void saveAll(List<Resource> artifacts) {
            saveAll(artifacts, Map.of());
        }

        @Override
        public Optional<String> saveAll(List<Resource> artifacts, Map<String, String> expectedContentHashes) {
            // The other writer lands here: after the caller planned against what it read, before this
            // write compares. Modelling it inside the store is the only place it can go — that is
            // precisely the window an outside-the-write comparison leaves open.
            if (concurrentWriter != null) {
                Runnable other = concurrentWriter;
                concurrentWriter = null;
                other.run();
            }
            // The comparison is part of the write, not a step before it: a declared version that no
            // longer names the stored bytes refuses the whole batch and stages nothing.
            for (Map.Entry<String, String> expected : expectedContentHashes.entrySet()) {
                String canonical = byId.get(expected.getKey());
                if (canonical == null || !CanonicalHash.of(canonical).equals(expected.getValue())) {
                    return Optional.of(expected.getKey());
                }
            }
            // Atomic: stage the whole batch, then commit it in one step — but if any member is the
            // injected poison, fail before committing anything, so no partial batch survives.
            Map<String, String> staged = new LinkedHashMap<>();
            for (Resource artifact : artifacts) {
                if (artifact.id().equals(failOnId)) {
                    throw new RuntimeException("simulated store write failure at " + failOnId);
                }
                staged.put(artifact.id(), writer.write(artifact));
            }
            byId.putAll(staged);
            saveCount += artifacts.size();
            saveAllBatches.add(artifacts.stream().map(Resource::id).toList());
            return Optional.empty();
        }

        /** Commits {@code canonical} for {@code id} directly, as another author's apply would. */
        void landDirectly(Resource artifact) {
            byId.put(artifact.id(), writer.write(artifact));
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
    }
}
