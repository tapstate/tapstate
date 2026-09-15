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

/**
 * Which connector a {@code serve.sync} element may write through: this release supports one, and a
 * sync naming any other supported connector is refused before anything runs.
 *
 * <p>The restriction is on the write side alone. A source and a write target are the same
 * {@code kind: source} resource (X18), so nothing in the document distinguishes them — the role
 * does, and only the sync element knows which role it is asking for. Reading through every
 * supported connector stays exactly as it was.
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
 * <p>{@code serve.push} is untouched. It is event-stream egress with no table model, its targets are
 * message systems rather than databases, and none of them is in the supported set — so the same rule
 * applied there would be all deferral and no judgement, and would read as a restriction that exists.
 */
public final class TargetConnectorRules {

    /** The one connector a sync element may write through. */
    public static final String SUPPORTED_TARGET = "mongodb";

    private TargetConnectorRules() {
    }

    public static void validate(Collection<Resource> batch) {
        Map<String, Resource> byId = new LinkedHashMap<>();
        for (Resource r : batch) {
            byId.put(r.id(), r);
        }
        for (Resource r : batch) {
            if (r instanceof PipelineResource p) {
                checkPipeline(p, byId);
            } else if (r instanceof ServeResource serve) {
                checkSyncElements(serve.sync(), byId);
            }
        }
    }

    private static void checkPipeline(PipelineResource p, Map<String, Resource> byId) {
        // A use-reference serve carries its elements in the kind: serve definition, which is judged
        // on its own above. Judging it again through every pipeline that names it would report one
        // document once per reference.
        if (p.serve() instanceof ServeBlock.Inline inline) {
            checkSyncElements(inline.sync(), byId);
        }
    }

    private static void checkSyncElements(List<SyncElement> sync, Map<String, Resource> byId) {
        if (sync == null) {
            return;
        }
        for (int i = 0; i < sync.size(); i++) {
            SyncElement element = sync.get(i);
            if (!(byId.get(element.source()) instanceof SourceResource target)) {
                continue;   // existence already proved by reference closure
            }
            String connector = target.connector();
            if (SUPPORTED_TARGET.equals(connector) || !OfficialConnectors.isOfficial(connector)) {
                continue;
            }
            String path = "serve.sync[" + i + "].source";
            throw new DslException(DslError.UNSUPPORTED_TARGET_CONNECTOR, path, 0, 0, null,
                    Map.of("connector", connector, "source", element.source(), "path", path));
        }
    }
}
