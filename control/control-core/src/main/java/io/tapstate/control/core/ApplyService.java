package io.tapstate.control.core;

import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.dsl.CapabilityRules;
import io.tapstate.core.dsl.DslException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.dsl.DiscoveredTable;
import io.tapstate.core.dsl.RowExpressionTypeRules;
import io.tapstate.core.dsl.ReferenceGraph;
import io.tapstate.core.dsl.Workspace;
import io.tapstate.core.dsl.WriteKeyRules;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ArtifactBatchWrite;
import io.tapstate.spi.store.ArtifactWrite;
import io.tapstate.spi.store.SchemaStore;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceTable;

import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * The resource-type-agnostic apply pipeline. {@link #plan} is the front half — validate -> canonical
 * -> hash: it parses each draft (structural + expression checks), validates the submitted batch as one
 * closure (duplicate ids, reference closure, mode rules, and the connector capability matrix against
 * the catalog), judges the batch's row expressions against the columns of the tables its sources were
 * discovered to hold, then emits each resource's canonical form and content hash. It writes nothing. It reads the
 * schema store — an observation of what discovery found, never the config truth layer, which apply is
 * the one writer of. Typed online writes additionally read the artifact truth layer so validation can
 * include only the relevant dependency/referrer closure; offline {@link #plan} keeps the historical
 * contract that the submitted batch itself is the closure. A draft carrying a precondition also reads
 * its stored version to report a stale edit before the atomic write check; a pipeline reads back the srs
 * switches it has already recorded so that an unedited file re-applies as a no-op.
 * {@link #apply} runs a plan and then upserts each artifact into the store by its id, skipping the
 * write when the stored artifact's content hash is unchanged (a no-op).
 *
 * <p>The row-expression type check is the one layer that cannot run offline: the columns' types exist
 * only once a connection has been discovered, so an expression the data cannot survive is refused
 * here rather than at the offline check, which has nothing to judge it against.
 *
 * <p>Any validation failure aborts with the first coded {@code dsl.*} diagnostic before any upsert, and a
 * draft whose precondition has gone stale aborts the same way with {@code artifact.version-conflict};
 * nothing is written on either. The refusal is of the whole submitted batch, never of the offending
 * draft alone — a batch is one closure, so letting half of it land would store a state nothing ever
 * validated. For typed online writes, the stored workspace is used only to select the validation
 * dependency/referrer closure and to guard those exact resources against concurrent drift.
 *
 * <p>The catalog is supplied per plan rather than fixed, so the online path validates against the live
 * capability view — the bundled snapshot with registered rows overlaid — and a connector registered at
 * runtime is honoured without a restart.
 *
 * <p>The no-op is keyed by the content hash over the canonical form, so re-applying identical content
 * — even with different raw key order — writes nothing. Apply writes the changed set — the created and
 * updated artifacts — as one atomic batch, so a mid-batch write failure rolls the whole batch back and
 * no partial batch is stored, matching the validation-failure guarantee on the write side.
 *
 * <p>A declared version is enforced <em>inside</em> that batch write, not only compared beforehand.
 * The comparison in {@link #plan} is what produces the diagnostic an author can read; it is not what
 * makes the edit safe, because validation runs between it and the write and a second author lands in
 * that window. Handing the declared versions to the store makes the comparison and the write one
 * indivisible operation, so the losing author is refused with {@code artifact.version-conflict}
 * rather than silently overwriting the winner.
 */
public final class ApplyService {

    private final Supplier<TapstateCatalog> catalog;
    private final ArtifactStore store;
    private final AuditGate auditGate;
    private final SchemaStore schemas;
    private final PlanAdvisories advisories;

    /**
     * The reading of which pipelines are up, or null when the caller supplied none -- see the same field
     * on the Source service. Apply is the path most edits actually arrive on, so a guard that covered
     * only the other one would be a guard in name.
     */
    private final LivePipelines live;
    private final DslParser parser = new DslParser();
    private final CanonicalWriter writer = new CanonicalWriter();

    public ApplyService(
            Supplier<TapstateCatalog> catalog, ArtifactStore store, AuditGate auditGate, SchemaStore schemas,
            PlanAdvisories advisories) {
        this(catalog, store, auditGate, schemas, advisories, null);
    }

    public ApplyService(
            Supplier<TapstateCatalog> catalog, ArtifactStore store, AuditGate auditGate, SchemaStore schemas,
            PlanAdvisories advisories, LivePipelines live) {
        this.live = live;
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.store = Objects.requireNonNull(store, "store");
        this.auditGate = Objects.requireNonNull(auditGate, "auditGate");
        this.schemas = Objects.requireNonNull(schemas, "schemas");
        // Named rather than defaulted: a service that quietly reported "nothing to advise" for every batch
        // would be indistinguishable from one whose rules all passed, so an assembly with no rules yet
        // states that by handing over PlanAdvisories.none().
        this.advisories = Objects.requireNonNull(advisories, "advisories");
    }

    /**
     * Validates and canonicalizes {@code drafts} as one batch, returning the artifacts an apply
     * would upsert together with the advisory findings over them. Throws the first {@link DslException}
     * (a coded, user-facing diagnostic) on any structural / reference / mode / capability violation.
     *
     * <p>The advisory pass runs last, over a batch every gate above it has already accepted — so a rule
     * reads resources that are known good, and a refusal is never buried under advice about a batch that
     * is not going anywhere.
     */
    public ApplyPlan plan(List<ArtifactDraft> drafts) {
        Objects.requireNonNull(drafts, "drafts");
        List<Resource> resources = new ArrayList<>();
        for (ArtifactDraft draft : drafts) {
            resources.add(parse(draft));
        }
        // Preconditions are judged once every draft has parsed, so a malformed document is reported as
        // malformed rather than as a version conflict, and before the batch is validated, so an author
        // editing a version that has moved on is told that instead of being handed diagnostics about
        // content they are about to rewrite. Each one declared is kept under the id it was declared
        // against: this is the only point at which a draft and the id it parses to are both in hand.
        Map<String, String> preconditions = new LinkedHashMap<>();
        for (int index = 0; index < drafts.size(); index++) {
            ArtifactDraft draft = drafts.get(index);
            Resource parsed = resources.get(index);
            requireCurrentVersion(draft, parsed);
            if (draft.expectedContentHash() != null) {
                preconditions.put(parsed.id(), draft.expectedContentHash());
            }
        }
        return planResources(resources, preconditions, ValidationScope.OFFLINE);
    }

    /**
     * Plans one or more already-built resources through the same candidate-workspace validation path
     * used by parsed drafts. Typed control faces call this entry after their input mapper has built a
     * resource; they do not serialize it to YAML or recreate validation beside apply.
     */
    private ApplyPlan planResources(
            List<Resource> submitted, Map<String, String> preconditions, ValidationScope validationScope) {
        Objects.requireNonNull(submitted, "submitted");
        Objects.requireNonNull(preconditions, "preconditions");
        Objects.requireNonNull(validationScope, "validationScope");
        Set<String> submittedIds = submitted.stream().map(Resource::id).collect(java.util.stream.Collectors.toSet());
        List<Resource> storedResources = store.list();
        List<Resource> candidate = new ArrayList<>();
        for (Resource stored : storedResources) {
            if (!submittedIds.contains(stored.id())) {
                candidate.add(stored);
            }
        }
        candidate.addAll(submitted);
        TapstateCatalog liveCatalog = catalog.get();
        List<Resource> validationResources = validationResources(candidate, submitted, validationScope);
        Workspace workspace = Workspace.of(validationResources, liveCatalog);
        if (validationScope == ValidationScope.ONLINE_SOURCE) {
            for (Resource resource : submitted) {
                if (resource instanceof SourceResource source) {
                    CapabilityRules.validateOnline(source, liveCatalog);
                }
            }
        }
        // Read once and handed to both: the gate judges the batch against it, then the advisory pass
        // advises on the same reading rather than paying a second round trip for a possibly different one.
        Map<String, List<DiscoveredTable>> discovered = discoveredTables(validationResources);
        RowExpressionTypeRules.validate(validationResources, discovered);
        WriteKeyRules.validate(validationResources, discovered);
        List<Resource> validated = List.copyOf(workspace.resources());
        Map<String, String> workspacePreconditions = new LinkedHashMap<>();
        for (Resource resource : validated) {
            if (!submittedIds.contains(resource.id())) {
                workspacePreconditions.put(resource.id(), storedHash(resource));
            }
        }
        Map<String, Resource> validatedById = new LinkedHashMap<>();
        Map<String, SourceResource> batchSources = new LinkedHashMap<>();
        for (Resource resource : validated) {
            validatedById.put(resource.id(), resource);
            if (resource instanceof SourceResource source) {
                batchSources.put(source.id(), source);
            }
        }
        List<PreparedArtifact> prepared = new ArrayList<>();
        for (Resource submittedResource : submitted) {
            Resource resource = validatedById.get(submittedResource.id());
            Resource recorded = resource instanceof PipelineResource pipeline
                    ? withOwnSrsSwitches(pipeline, batchSources) : resource;
            String canonicalForm = writer.write(recorded);
            prepared.add(new PreparedArtifact(recorded, canonicalForm, CanonicalHash.of(canonicalForm)));
        }
        return new ApplyPlan(prepared, advisories.review(validated, discovered), preconditions, workspacePreconditions);
    }

    /**
     * Records this pipeline's own srs switch for each source it reads. A switch the author wrote wins;
     * failing that, the value this pipeline already recorded for that source is kept; failing that,
     * the reference is new and the source's own switch is taken once -- here, and never again.
     *
     * <p>Why it is per pipeline at all: the switch decides whether a source is read through the shared
     * replay store, and editing it on the source moved every pipeline reading that source at once -- a
     * consent none of them could give individually. Recorded here, moving one pipeline is an edit to
     * that one pipeline.
     *
     * <p>"First" is judged per source, not per pipeline: a source added to an existing pipeline has no
     * recorded value, so it takes that source's, while the references beside it keep theirs.
     *
     * <p>This runs before the canonical form is written, and therefore before the content hash. After
     * it, a draft that omits the switch would hash differently from the stored artifact that carries
     * one, so every apply of an unedited file would read as a change: rewrite without the switch,
     * materialize again, one revision per apply on a file nobody touched.
     */
    private PipelineResource withOwnSrsSwitches(
            PipelineResource pipeline, Map<String, SourceResource> batchSources) {
        Map<String, Boolean> alreadyRecorded = new LinkedHashMap<>();
        if (store.get(pipeline.id()).orElse(null) instanceof PipelineResource stored) {
            for (SourceRef ref : stored.sources()) {
                if (ref instanceof SourceRef.Spec spec) {
                    alreadyRecorded.put(spec.id(), spec.srs());
                }
            }
        }
        List<SourceRef> refs = new ArrayList<>();
        boolean anyRecorded = false;
        for (SourceRef ref : pipeline.sources()) {
            if (ref instanceof SourceRef.Spec) {
                refs.add(ref);
                anyRecorded = true;
                continue;
            }
            Boolean own = alreadyRecorded.get(ref.id());
            if (own == null) {
                // The source is read from the batch alone, because the batch is the closure: a pipeline
                // referencing a source that is not in it is refused before this runs. A reference left
                // bare here therefore cannot happen; if that closure rule is ever widened, the capture
                // side names the pipeline and the source rather than guessing a value.
                SourceResource declaring = batchSources.get(ref.id());
                own = declaring == null ? null : declaring.srsEnabled();
            }
            refs.add(own == null ? ref : new SourceRef.Spec(ref.id(), own));
            anyRecorded |= own != null;
        }
        return anyRecorded
                ? new PipelineResource(pipeline.id(), pipeline.metadata(), refs, pipeline.transforms(),
                        pipeline.view(), pipeline.serve(), pipeline.settings(), pipeline.experimental())
                : pipeline;
    }

    /**
     * Chooses the resource set that semantic validation and discovered-schema gates inspect.
     *
     * <p>Offline apply/validate preserves the original contract: the submitted batch is the closure, so
     * only submitted resources are validated and unrelated stored artifacts cannot reject or warn on it.
     * Typed online Source/Pipeline writes additionally select the stored dependency/referrer closure they
     * actually read for validation. That closure is also the only set guarded against concurrent drift;
     * the rest of the store is neither revalidated nor used as a global optimistic lock.
     */
    private static List<Resource> validationResources(
            List<Resource> candidate, List<Resource> submitted, ValidationScope scope) {
        if (scope == ValidationScope.OFFLINE) {
            return List.copyOf(submitted);
        }
        Set<String> submittedSourceIds = new LinkedHashSet<>();
        Set<String> selectedIds = new LinkedHashSet<>();
        for (Resource resource : submitted) {
            selectedIds.add(resource.id());
            if (resource instanceof SourceResource) {
                submittedSourceIds.add(resource.id());
            }
        }

        ReferenceGraph graph = ReferenceGraph.of(candidate);
        ArrayDeque<String> pending = new ArrayDeque<>(submittedSourceIds);
        while (!pending.isEmpty()) {
            String id = pending.removeFirst();
            for (ReferenceGraph.Edge referrer : graph.referencedBy(id)) {
                if (selectedIds.add(referrer.id())) {
                    pending.addLast(referrer.id());
                }
            }
        }

        pending.addAll(selectedIds);
        while (!pending.isEmpty()) {
            String id = pending.removeFirst();
            for (ReferenceGraph.Edge dependency : graph.references(id)) {
                if (selectedIds.add(dependency.id())) {
                    pending.addLast(dependency.id());
                }
            }
        }
        return candidate.stream().filter(resource -> selectedIds.contains(resource.id())).toList();
    }

    /** Applies one typed resource only while its id is absent. */
    public ArtifactWriteResult create(String principal, Resource resource) {
        return create(principal, resource, ControlOperations.ARTIFACT_APPLY);
    }

    ArtifactWriteResult create(String principal, Resource resource, Operation operation) {
        return writeTyped(principal, resource, ArtifactWrite.Intent.CREATE_ONLY, null, operation);
    }

    /** Applies one typed resource only while its stored canonical hash equals {@code expectedContentHash}. */
    public ArtifactWriteResult replace(String principal, Resource resource, String expectedContentHash) {
        Objects.requireNonNull(expectedContentHash, "expectedContentHash");
        return replace(principal, resource, expectedContentHash, ControlOperations.ARTIFACT_APPLY);
    }

    ArtifactWriteResult replace(
            String principal, Resource resource, String expectedContentHash, Operation operation) {
        Objects.requireNonNull(expectedContentHash, "expectedContentHash");
        return writeTyped(principal, resource, ArtifactWrite.Intent.REPLACE_ONLY, expectedContentHash, operation);
    }

    private ArtifactWriteResult writeTyped(
            String principal,
            Resource resource,
            ArtifactWrite.Intent intent,
            String expectedContentHash,
            Operation operation) {
        Objects.requireNonNull(principal, "principal");
        Objects.requireNonNull(resource, "resource");
        Objects.requireNonNull(operation, "operation");
        ApplyPlan plan = planResources(List.of(resource), Map.of(), ValidationScope.ONLINE_SOURCE);
        PreparedArtifact prepared = plan.artifacts().getFirst();
        if (live != null) {
            List<Resource> stored = store.list();
            if (prepared.resource() instanceof SourceResource replacement) {
                live.refuseBufferingChangeWhileLive(
                        storedSource(stored, replacement.id()), replacement, stored);
            }
            if (prepared.resource() instanceof PipelineResource replacement) {
                live.refuseBufferingChangeWhileLive(
                        storedPipeline(stored, replacement.id()), replacement);
            }
        }
        ArtifactWrite write = (switch (intent) {
            case CREATE_ONLY -> ArtifactWrite.createOnly(prepared.resource());
            case REPLACE_ONLY -> ArtifactWrite.replaceOnly(prepared.resource(), expectedContentHash);
            case UPSERT -> throw new IllegalArgumentException("typed writes must be conditional");
        }).guardedBy(plan.workspacePreconditions());
        ArtifactBatchWrite outcome = auditGate.dispatchAll(
                operation,
                List.of(new AuditContext(principal, prepared.id(), expectedContentHash)),
                () -> store.writeAll(List.of(write)));
        return new ArtifactWriteResult(prepared, outcome);
    }

    /** Validates and plans a batch while performing no store or audit write. */
    public ArtifactValidationResult validate(List<ArtifactDraft> drafts) {
        final ApplyPlan planned;
        try {
            planned = plan(drafts);
        } catch (TapstateException diagnostic) {
            return new ArtifactValidationResult(
                    false,
                    List.of(),
                    List.of(new ValidationDiagnostic(diagnostic.code().code(), diagnostic.args())),
                    List.of());
        }
        List<ArtifactOutcome> outcomes = new ArrayList<>();
        for (PreparedArtifact prepared : planned.artifacts()) {
            outcomes.add(outcome(prepared));
        }
        return new ArtifactValidationResult(true, outcomes, List.of(), planned.warnings());
    }

    /**
     * Validates the batch (via {@link #plan}), then writes the changed set — created and updated
     * artifacts — into the store as one atomic batch, returning one outcome per artifact in submission
     * order. An artifact whose stored content hash already equals the applied one is a no-op and is left
     * out of the batch. A validation failure throws the first {@link DslException} before any write, and
     * a store write failure rolls the whole batch back, so nothing is stored on an invalid or a failed
     * batch.
     *
     * <p>Apply is an audited write: the changed set passes the audit gate under {@code principal}, one
     * record per changed artifact attributed by its own id and carrying the version its draft declared
     * it was editing, before any of it is stored. A no-op leaves no record because it changes nothing,
     * and an audit-write failure refuses the whole apply ({@code control.audit-blocked}) with the store
     * untouched.
     */
    public ApplyResult apply(String principal, List<ArtifactDraft> drafts) {
        Objects.requireNonNull(principal, "principal");
        ApplyPlan plan = plan(drafts);
        List<ArtifactOutcome> outcomes = new ArrayList<>();
        List<Resource> toWrite = new ArrayList<>();
        List<AuditContext> audited = new ArrayList<>();
        Map<String, String> enforced = new LinkedHashMap<>();
        // Read once for the refusal below, and only when there is a reading to judge against.
        List<Resource> stored = live == null ? List.of() : store.list();
        for (PreparedArtifact prepared : plan.artifacts()) {
            ArtifactOutcome outcome = outcome(prepared);
            if (outcome.change() != ArtifactOutcome.Change.UNCHANGED) {
                if (live != null && prepared.resource() instanceof SourceResource replacement) {
                    live.refuseBufferingChangeWhileLive(
                            storedSource(stored, replacement.id()), replacement, stored);
                }
                if (live != null && prepared.resource() instanceof PipelineResource replacement) {
                    live.refuseBufferingChangeWhileLive(
                            storedPipeline(stored, replacement.id()), replacement);
                }
                toWrite.add(prepared.resource());
                String declared = plan.precondition(prepared.id());
                audited.add(new AuditContext(principal, prepared.id(), declared));
                if (declared != null) {
                    enforced.put(prepared.id(), declared);
                }
            }
            outcomes.add(outcome);
        }
        return auditGate.dispatchAll(ControlOperations.ARTIFACT_APPLY, audited, () -> {
            String conflicted = store.saveAll(toWrite, enforced).orElse(null);
            if (conflicted != null) {
                throw new TapstateException(ArtifactError.VERSION_CONFLICT, Map.of("id", conflicted), null);
            }
            return new ApplyResult(outcomes, plan.warnings());
        });
    }

    private static PipelineResource storedPipeline(List<Resource> stored, String id) {
        return stored.stream()
                .filter(PipelineResource.class::isInstance)
                .map(PipelineResource.class::cast)
                .filter(pipeline -> pipeline.id().equals(id))
                .findFirst()
                .orElse(null);
    }

    private static SourceResource storedSource(List<Resource> stored, String id) {
        return stored.stream()
                .filter(SourceResource.class::isInstance)
                .map(SourceResource.class::cast)
                .filter(source -> source.id().equals(id))
                .findFirst()
                .orElse(null);
    }

    private enum ValidationScope {
        OFFLINE,
        ONLINE_SOURCE
    }

    private Map<String, List<DiscoveredTable>> discoveredTables(List<Resource> resources) {
        Map<String, List<DiscoveredTable>> bySource = new LinkedHashMap<>();
        if (resources.stream().noneMatch(PipelineResource.class::isInstance)) {
            return bySource;
        }
        for (Resource resource : resources) {
            if (!(resource instanceof SourceResource source)) {
                continue;
            }
            schemas.get(source.id())
                    .filter(discovered -> discovered.connectorId().equals(source.connector()))
                    .ifPresent(discovered -> {
                        List<DiscoveredTable> tables = new ArrayList<>();
                        for (SourceTable table : SourceTableScope.select(source, discovered.model().tables())) {
                            Map<String, TapstateType> columns = new LinkedHashMap<>();
                            for (SourceField field : table.fields()) {
                                columns.put(field.name(), field.type());
                            }
                            tables.add(new DiscoveredTable(
                                    table.name(), columns, table.primaryKey(),
                                    table.approximateRowCount()));
                        }
                        bySource.put(source.id(), tables);
                    });
        }
        return bySource;
    }

    private ArtifactOutcome outcome(PreparedArtifact prepared) {
        Optional<Resource> existing = store.get(prepared.id());
        ArtifactOutcome.Change change = existing.isEmpty()
                ? ArtifactOutcome.Change.CREATED
                : storedHash(existing.get()).equals(prepared.contentHash())
                        ? ArtifactOutcome.Change.UNCHANGED
                        : ArtifactOutcome.Change.UPDATED;
        return new ArtifactOutcome(prepared.id(), prepared.kind(), change, prepared.contentHash());
    }

    private String storedHash(Resource stored) {
        return CanonicalHash.of(writer.write(stored));
    }

    private void requireCurrentVersion(ArtifactDraft draft, Resource parsed) {
        String expected = draft.expectedContentHash();
        if (expected == null) {
            return;
        }
        String id = parsed.id();
        Resource stored = store.get(id).orElseThrow(() ->
                new TapstateException(ArtifactError.NOT_FOUND, Map.of("id", id), null));
        if (!storedHash(stored).equals(expected)) {
            throw new TapstateException(ArtifactError.VERSION_CONFLICT, Map.of("id", id), null);
        }
    }

    private Resource parse(ArtifactDraft draft) {
        try {
            return parser.parse(draft.content());
        } catch (DslException e) {
            throw draft.source() != null ? e.withSource(draft.source()) : e;
        }
    }
}
