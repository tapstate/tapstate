package io.tapstate.control.core;

import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.dsl.CapabilityRules;
import io.tapstate.core.dsl.DslException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.dsl.DiscoveredTable;
import io.tapstate.core.dsl.RowExpressionTypeRules;
import io.tapstate.core.dsl.Workspace;
import io.tapstate.core.dsl.WriteKeyRules;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.TableRef;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ArtifactBatchWrite;
import io.tapstate.spi.store.ArtifactWrite;
import io.tapstate.spi.store.SchemaStore;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceTable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * The resource-type-agnostic apply pipeline. {@link #plan} is the front half — validate -> canonical
 * -> hash: it parses each draft (structural + expression checks), validates the whole batch as one
 * closure (duplicate ids, reference closure, mode rules, and the connector capability matrix against
 * the catalog), judges the batch's row expressions against the columns of the tables its sources were
 * discovered to hold, then emits each resource's canonical form and content hash. It writes nothing. It reads the
 * schema store — an observation of what discovery found, never the config truth layer, which apply is
 * the one writer of — and reads the artifact truth layer to overlay submitted resources on the stored
 * workspace before validation. A draft carrying a precondition also reads its stored version to report
 * a stale edit before the atomic write check.
 * {@link #apply} runs a plan and then upserts each artifact into the store by its id, skipping the
 * write when the stored artifact's content hash is unchanged (a no-op).
 *
 * <p>The row-expression type check is the one layer that cannot run offline: the columns' types exist
 * only once a connection has been discovered, so an expression the data cannot survive is refused
 * here rather than at the offline check, which has nothing to judge it against.
 *
 * <p>Any validation failure aborts with the first coded {@code dsl.*} diagnostic before any upsert, and a
 * draft whose precondition has gone stale aborts the same way with {@code artifact.version-conflict};
 * nothing is written on either. The refusal is of the whole batch, never of the offending draft alone —
 * a batch is one closure, so letting half of it land would store a state nothing ever validated. The
 * candidate workspace is the closure: each submission overlays its id on every stored artifact, so
 * references resolve against the state that would exist after the write.
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
    private final DslParser parser = new DslParser();
    private final CanonicalWriter writer = new CanonicalWriter();

    public ApplyService(
            Supplier<TapstateCatalog> catalog, ArtifactStore store, AuditGate auditGate, SchemaStore schemas,
            PlanAdvisories advisories) {
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
        List<Resource> candidate = new ArrayList<>();
        for (Resource stored : store.list()) {
            if (!submittedIds.contains(stored.id())) {
                candidate.add(stored);
            }
        }
        candidate.addAll(submitted);
        TapstateCatalog liveCatalog = catalog.get();
        Workspace workspace = Workspace.of(candidate, liveCatalog);
        if (validationScope == ValidationScope.ONLINE_SOURCE) {
            for (Resource resource : submitted) {
                if (resource instanceof SourceResource source) {
                    CapabilityRules.validateOnline(source, liveCatalog);
                }
            }
        }
        // Read once and handed to both: the gate judges the batch against it, then the advisory pass
        // advises on the same reading rather than paying a second round trip for a possibly different one.
        Map<String, List<DiscoveredTable>> discovered = discoveredTables(candidate);
        RowExpressionTypeRules.validate(candidate, discovered);
        WriteKeyRules.validate(candidate, discovered);
        List<Resource> validated = List.copyOf(workspace.resources());
        Map<String, Resource> validatedById = new LinkedHashMap<>();
        for (Resource resource : validated) {
            validatedById.put(resource.id(), resource);
        }
        List<PreparedArtifact> prepared = new ArrayList<>();
        for (Resource submittedResource : submitted) {
            Resource resource = validatedById.get(submittedResource.id());
            String canonicalForm = writer.write(resource);
            prepared.add(new PreparedArtifact(resource, canonicalForm, CanonicalHash.of(canonicalForm)));
        }
        return new ApplyPlan(prepared, advisories.review(validated, discovered), preconditions);
    }

    /** Applies one typed resource only while its id is absent. */
    public ArtifactWriteResult create(String principal, Resource resource) {
        return writeTyped(principal, resource, ArtifactWrite.Intent.CREATE_ONLY, null);
    }

    /** Applies one typed resource only while its stored canonical hash equals {@code expectedContentHash}. */
    public ArtifactWriteResult replace(String principal, Resource resource, String expectedContentHash) {
        Objects.requireNonNull(expectedContentHash, "expectedContentHash");
        return writeTyped(principal, resource, ArtifactWrite.Intent.REPLACE_ONLY, expectedContentHash);
    }

    private ArtifactWriteResult writeTyped(
            String principal, Resource resource, ArtifactWrite.Intent intent, String expectedContentHash) {
        Objects.requireNonNull(principal, "principal");
        Objects.requireNonNull(resource, "resource");
        ApplyPlan plan = planResources(List.of(resource), Map.of(), ValidationScope.ONLINE_SOURCE);
        PreparedArtifact prepared = plan.artifacts().getFirst();
        ArtifactWrite write = switch (intent) {
            case CREATE_ONLY -> ArtifactWrite.createOnly(prepared.resource());
            case REPLACE_ONLY -> ArtifactWrite.replaceOnly(prepared.resource(), expectedContentHash);
            case UPSERT -> throw new IllegalArgumentException("typed writes must be conditional");
        };
        ArtifactBatchWrite outcome = auditGate.dispatchAll(
                ControlOperations.ARTIFACT_APPLY,
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
        for (PreparedArtifact prepared : plan.artifacts()) {
            ArtifactOutcome outcome = outcome(prepared);
            if (outcome.change() != ArtifactOutcome.Change.UNCHANGED) {
                toWrite.add(prepared.resource());
                // The declared version travels with the record, so a version-checked edit is
                // distinguishable in the audit trail from a blind overwrite of the same id. A draft that
                // declared none records none, which is what that absence then means.
                String declared = plan.precondition(prepared.id());
                audited.add(new AuditContext(principal, prepared.id(), declared));
                // Only the ids this batch actually overwrites are guarded at the write. An unchanged
                // artifact is not written, so there is nothing for its declared version to protect —
                // plan() already compared it, and the comparison is all a caller asked for.
                if (declared != null) {
                    enforced.put(prepared.id(), declared);
                }
            }
            outcomes.add(outcome);
        }
        // The changed set is audited per artifact, then written as one atomic batch: all of it lands or,
        // on a write failure, none does.
        //
        // The declared versions are handed to the write rather than only to plan(). plan()'s comparison
        // happens before a whole workspace validation and a schema-store read, so a second author
        // editing the same id inside that window passes the same comparison and both writes land — the
        // first author's edit is gone, and nothing anywhere reports it. Passing them here makes the
        // comparison and the write one store operation, which is the only form of the check that
        // survives a concurrent writer.
        return auditGate.dispatchAll(ControlOperations.ARTIFACT_APPLY, audited, () -> {
            String conflicted = store.saveAll(toWrite, enforced).orElse(null);
            if (conflicted != null) {
                throw new TapstateException(ArtifactError.VERSION_CONFLICT, Map.of("id", conflicted), null);
            }
            return new ApplyResult(outcomes, plan.warnings());
        });
    }

    private enum ValidationScope {
        OFFLINE,
        ONLINE_SOURCE
    }

    /**
     * The tables each source in the batch was discovered to hold, keyed by the source's id — which is
     * also the connection id its discovery is stored under. A source that has never been discovered is
     * absent from the result rather than present and empty, so the rules can tell "discovered nothing"
     * apart from "not discovered".
     *
     * <p>Each table keeps its own columns. Pooling a source's tables into one column list would have to
     * call a column two of them type differently unresolved, and one database naming a column
     * {@code id} in two unrelated tables, typed differently, is the ordinary shape of a database rather
     * than a corner of it — pooled, the gate would refuse most expressions on most real databases. The
     * rules judge an expression against the table it reads, so the tables are handed over apart.
     *
     * <p>A model counts only when it was discovered through the connector this source now names. Types
     * are resolved against the declaring connector's own vocabulary, so a model another connector
     * produced carries types this source's columns were never described in - reading it would be
     * judging one source's expression against a different source's answers. Keeping the connection's id
     * across such a change does not make the old model apply to the new connector, so a mismatch reads
     * as undiscovered: the author is asked to discover, and discovering is what makes it true.
     *
     * <p>What this does not check is whether the model is current. The stored model is what the last
     * discovery found, and the source it describes can change afterwards without anything here
     * changing - the connection settings can be edited, or the database itself altered under settings
     * that never moved. The check is against the last discovery, by design, and only a fresh discovery
     * makes it fresh.
     *
     * <p>A batch carrying no pipeline is answered without reading the store at all. Only a pipeline
     * holds a row expression, so there would be nothing to judge what was read against - and a batch
     * of endpoints alone is an ordinary thing to apply, which would otherwise pay a store round trip
     * per source for an answer nobody consults.
     */
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
                        for (SourceTable table : selectedTables(source, discovered.model().tables())) {
                            Map<String, TapstateType> columns = new LinkedHashMap<>();
                            for (SourceField field : table.fields()) {
                                columns.put(field.name(), field.type());
                            }
                            // The row count travels with the columns, absence and all: a table nobody
                            // counted has to stay distinguishable from one counted and found empty.
                            // The declared key travels the same way, and for the same reason a rule
                            // about writes needs it: whether a write can be matched to an existing
                            // row is a property of the table, decided where the table is described.
                            tables.add(new DiscoveredTable(
                                    table.name(), columns, table.primaryKey(),
                                    table.approximateRowCount()));
                        }
                        bySource.put(source.id(), tables);
                    });
        }
        return bySource;
    }

    /**
     * The discovered tables {@code source} reads. Discovery runs per connection, so the stored model
     * carries every table the connection can see - including the ones this source's selector leaves
     * out. Those are not this source's to answer for, and the wiring can point an expression at a table
     * only through the source that selects it.
     *
     * <p>A selector matching nothing in the model narrows nothing — the names cannot be lined up (the
     * connector may report qualified names, or the model may predate the selector), so no table is
     * ruled out. Where the wiring names the table it reads, this changes no verdict: a name absent
     * from the model is filtered out downstream either way. Where it cannot — a regex {@code from:},
     * which only a connection can resolve — it is what keeps the whole model in play; narrowing to
     * the empty set there would leave every column absent, and an absent column stays untyped and
     * passes, so the gate would quietly stop refusing anything at all for that source. A pattern that
     * will not compile matches nothing on its own rather than failing the apply.
     */
    private static List<SourceTable> selectedTables(SourceResource source, List<SourceTable> discovered) {
        List<TableRef> selectors = source.tables();
        if (selectors == null || selectors.isEmpty()) {
            return discovered;      // no selector: the source reads whatever the connection holds
        }
        List<SourceTable> selected = new ArrayList<>();
        for (SourceTable table : discovered) {
            if (selectors.stream().anyMatch(selector -> selects(selector, table.name()))) {
                selected.add(table);
            }
        }
        return selected.isEmpty() ? discovered : selected;
    }

    /** Whether one {@code tables} entry selects the named discovered table. */
    private static boolean selects(TableRef selector, String table) {
        return switch (selector) {
            case TableRef.Literal literal -> literal.name().equals(table);
            case TableRef.Spec spec -> spec.name().equals(table);
            case TableRef.Regex regex -> matches(regex.pattern(), table);
        };
    }

    private static boolean matches(String pattern, String table) {
        try {
            return Pattern.matches(pattern, table);
        } catch (PatternSyntaxException e) {
            return false;
        }
    }

    /** Classifies one prepared artifact without mutating the store. */
    private ArtifactOutcome outcome(PreparedArtifact prepared) {
        Optional<Resource> existing = store.get(prepared.id());
        ArtifactOutcome.Change change = existing.isEmpty()
                ? ArtifactOutcome.Change.CREATED
                : storedHash(existing.get()).equals(prepared.contentHash())
                        ? ArtifactOutcome.Change.UNCHANGED
                        : ArtifactOutcome.Change.UPDATED;
        return new ArtifactOutcome(prepared.id(), prepared.kind(), change, prepared.contentHash());
    }

    /** The content hash of a stored artifact, recomputed over its canonical form for the no-op check. */
    private String storedHash(Resource stored) {
        return CanonicalHash.of(writer.write(stored));
    }

    /**
     * Refuses a draft whose optional precondition no longer names the stored version. A draft without
     * one is left alone, which is what keeps a caller that never asked for the check from ever being
     * refused by it. An id that is not stored at all cannot match any version, and is reported as
     * absent rather than as a conflict, so an author whose target was deleted is told what happened.
     */
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
            // A parse error is located at exactly this draft; attribute it when the origin is known.
            throw draft.source() != null ? e.withSource(draft.source()) : e;
        }
    }
}
