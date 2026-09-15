package io.tapstate.core.dsl;

import io.tapstate.core.catalog.OfficialConnectors;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.ServeResource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.SyncElement;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Which connector a {@code serve.sync} element may write through: this release supports one database
 * kind, and a sync naming any other supported connector is refused before anything runs.
 *
 * <p>The restriction is on the write side alone. A source and a write target are the same
 * {@code kind: source} resource (X18), so nothing in the document distinguishes them — the role
 * does, and only the sync element knows which role it is asking for. Reading through every
 * supported connector stays exactly as it was.
 *
 * <p><strong>A kind, not an id.</strong> Support is certified per database kind, and the managed
 * variants of a kind are the same database underneath — which is why they are certified together and
 * why the set is taken from the one place that grouping is written down rather than restated as a
 * literal here. A deployment running a managed MongoDB registers the variant's id, and admitting the
 * kind is what lets it install a sync at all.
 *
 * <p><strong>A server-side gate, not part of {@link Workspace} validation.</strong> It runs where the
 * other rules that need more than the document run — on the apply path — and for the same kind of
 * reason: what may be written to is a property of the deployment being applied to, not of the
 * document. A deployment widens its own accepted connector set, and an offline check has no way to
 * read that set; it would be answering for a server it has not been told about. The document itself
 * stays as valid offline as the grammar says it is, which is also why the offline corpus keeps
 * exercising write targets this release will not install. The write-free validate verb plans the
 * batch through the same path, so an author asking the server whether a document is good is told
 * before they apply it -- what they do not get is a local file check answering for a server it has
 * not been told about.
 *
 * <p><strong>Judged only for a connector this release officially supports.</strong> Anything else is
 * passed, the same deferral {@link CapabilityRules} already gives a connector absent from the
 * catalog, and for a stronger reason: the registration path accepts the official set plus whatever a
 * deployment explicitly named for itself, so a connector outside that set is one no shipped
 * deployment can install. Refusing it here would be a verdict on documents only a deployment that
 * widened its own accepted set can write, about a connector this rule knows nothing about.
 *
 * <p><strong>What is judged, and what is only resolved.</strong> The two sets are separate and have
 * to be. Judgement is passed on the resources actually submitted: an edit elsewhere in the store must
 * not be refused because of an artifact an earlier release filed, and on the typed write path the
 * resource set reaching this rule is a reference closure that pulls in stored referrers and their
 * dependencies. Resolution goes the other way and reads everything the deployment holds. A write
 * target is an ordinary connection document, filed once and referred to afterwards, and it reaches
 * this rule unresolved in two ordinary ways: the typed face submits one document and leaves the rest
 * of the wiring stored, and a {@code kind: serve} definition's sinks are existence-checked by nothing,
 * so a batch carrying one need not carry the target. Looking in the submitted set alone passes both.
 *
 * <p>A sync element is reached through the pipeline that installs it as well as through the
 * definition that declares it, so neither form can be applied without being judged. Both report the
 * document the element is written in, which is the one to edit: a {@code kind: serve} definition
 * spells its elements at {@code sync[i]}, with no {@code serve:} key above them.
 *
 * <p>{@code serve.push} is untouched. It is event-stream egress with no table model, its targets are
 * message systems rather than databases, and none of them is in the supported set — so the same rule
 * applied there would be all deferral and no judgement, and would read as a restriction that exists.
 */
public final class TargetConnectorRules {

    /** The database kind a sync element may write through. */
    public static final String SUPPORTED_TARGET_KIND = "mongodb";

    /** Every supported connector id of that kind, in the order a message naming them should read. */
    public static final List<String> SUPPORTED_TARGETS = Objects.requireNonNull(
            OfficialConnectors.IDS_BY_DATABASE_KIND.get(SUPPORTED_TARGET_KIND),
            "the supported write-target kind must be a declared database kind");

    private TargetConnectorRules() {
    }

    /**
     * Refuses a sync element of {@code submitted} that writes through a supported connector outside
     * the write-target kind. Ids are resolved against {@code known}, which is everything the
     * deployment holds once the batch is applied: the stored artifacts plus the submitted ones that
     * replace them.
     */
    public static void validate(Collection<Resource> submitted, Collection<Resource> known) {
        Map<String, Resource> byId = new LinkedHashMap<>();
        for (Resource r : known) {
            byId.put(r.id(), r);
        }
        for (Resource r : submitted) {
            if (r instanceof PipelineResource p) {
                checkPipeline(p, byId);
            } else if (r instanceof ServeResource serve) {
                checkSyncElements(serve.sync(), serve.id(), "sync", byId);
            }
        }
    }

    private static void checkPipeline(PipelineResource p, Map<String, Resource> byId) {
        switch (p.serve()) {
            case null -> { }
            case ServeBlock.Inline inline -> checkSyncElements(inline.sync(), p.id(), "serve.sync", byId);
            // A use-reference serve carries its elements in the definition it names, which may be a
            // document this batch never touched. Applying the pipeline is what installs those writes,
            // so they are judged here too, and reported against the definition that spells them.
            case ServeBlock.Use use -> {
                if (byId.get(use.use()) instanceof ServeResource definition) {
                    checkSyncElements(definition.sync(), definition.id(), "sync", byId);
                }
            }
        }
    }

    private static void checkSyncElements(
            List<SyncElement> sync, String owner, String prefix, Map<String, Resource> byId) {
        if (sync == null) {
            return;
        }
        for (int i = 0; i < sync.size(); i++) {
            SyncElement element = sync.get(i);
            // A target that resolves nowhere has no connector to judge, and nothing can be written
            // through a connection that does not exist.
            if (!(byId.get(element.source()) instanceof SourceResource target)) {
                continue;
            }
            String connector = target.connector();
            if (SUPPORTED_TARGETS.contains(connector) || !OfficialConnectors.isOfficial(connector)) {
                continue;
            }
            String path = prefix + "[" + i + "].source";
            throw new DslException(DslError.UNSUPPORTED_TARGET_CONNECTOR, path, 0, 0, null,
                    Map.of("connector", connector, "source", element.source(), "resource", owner,
                            "supported", String.join(", ", SUPPORTED_TARGETS), "path", path));
        }
    }
}
