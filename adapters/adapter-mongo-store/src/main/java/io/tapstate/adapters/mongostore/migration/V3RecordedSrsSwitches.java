package io.tapstate.adapters.mongostore.migration;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.ChangeSet;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import org.bson.Document;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Records, on every stored pipeline, the srs switch it reads each of its sources through.
 *
 * <p>A pipeline stored before a reference could carry that switch names its sources bare. Apply writes
 * one on every reference it stores, so the side that runs a pipeline treats a bare reference as an
 * invariant violation and crashes naming both halves rather than picking a value -- deliberately, since
 * guessing reads as a working pipeline running the other way. That leaves a store written by an older
 * build unable to run its own pipelines, with nothing an operator can do about it from the outside: the
 * pipeline was never wrong, and re-applying it by hand is the thing this framework exists to avoid.
 *
 * <p>So the switch is materialised here, once, by the same rule apply uses for a reference it has not
 * seen before: the source's own switch is taken. Nothing else can supply it -- the pipeline recorded
 * nothing and the author wrote nothing, which is exactly the case apply resolves this way.
 *
 * <p>A pipeline naming a source the store does not hold stops the changeset rather than being skipped.
 * The reference closure makes that unreachable through apply, so a store containing one is a store this
 * cannot reason about, and a value chosen for it would be the guess the capture side refuses to make.
 */
public final class V3RecordedSrsSwitches implements ChangeSet {

    private static final CanonicalWriter WRITER = new CanonicalWriter();
    private static final DslParser PARSER = new DslParser();

    @Override
    public int version() {
        return 3;
    }

    @Override
    public void up(MongoDatabase database) {
        MongoCollection<Document> artifacts = SystemCollections.ARTIFACTS.on(database);
        Map<String, Boolean> switchesBySource = ownSwitches(artifacts);

        Map<String, Document> recorded = new LinkedHashMap<>();
        List<String> dangling = new ArrayList<>();
        for (Map.Entry<String, PipelineResource> entry : bareReferrers(artifacts).entrySet()) {
            PipelineResource pipeline = entry.getValue();
            List<SourceRef> refs = new ArrayList<>(pipeline.sources().size());
            boolean answered = true;
            for (SourceRef ref : pipeline.sources()) {
                if (ref instanceof SourceRef.Spec) {
                    refs.add(ref);
                    continue;
                }
                Boolean own = switchesBySource.get(ref.id());
                if (own == null) {
                    dangling.add(entry.getKey() + " -> " + ref.id());
                    answered = false;
                    continue;
                }
                refs.add(new SourceRef.Spec(ref.id(), own));
            }
            if (!answered) {
                // Left unbuilt rather than built short: a pipeline missing the reference it could not
                // answer is refused by its own record, and that refusal would be reported here in place
                // of the one naming the source. The run stops below either way.
                continue;
            }
            Resource written = new PipelineResource(pipeline.id(), pipeline.metadata(), refs,
                    pipeline.transforms(), pipeline.view(), pipeline.serve(), pipeline.settings(),
                    pipeline.experimental());
            recorded.put(entry.getKey(), new Document("body", new Document(WRITER.tree(written)))
                    .append("contentHash", CanonicalHash.of(written)));
        }
        if (!dangling.isEmpty()) {
            // Thrown bare: the runner turns whatever a changeset throws into the coded failure naming it.
            throw new IllegalStateException("cannot record an srs switch for " + dangling.size()
                    + " reference(s) naming a source this store does not hold: " + dangling);
        }

        recorded.forEach((id, update) ->
                artifacts.updateOne(new Document("_id", id), new Document("$set", update)));
    }

    /** Every stored source's own switch, read the way the control side reads it. */
    private static Map<String, Boolean> ownSwitches(MongoCollection<Document> artifacts) {
        Map<String, Boolean> switches = new LinkedHashMap<>();
        try (MongoCursor<Document> cursor = artifacts.find(new Document("kind", "source")).iterator()) {
            while (cursor.hasNext()) {
                Document document = cursor.next();
                if (bind(document) instanceof SourceResource source) {
                    switches.put(source.id(), source.srsEnabled());
                }
            }
        }
        return switches;
    }

    /** The stored pipelines that name at least one source bare, bound out of their structured bodies. */
    private static Map<String, PipelineResource> bareReferrers(MongoCollection<Document> artifacts) {
        Map<String, PipelineResource> pipelines = new LinkedHashMap<>();
        try (MongoCursor<Document> cursor = artifacts.find(new Document("kind", "pipeline")).iterator()) {
            while (cursor.hasNext()) {
                Document document = cursor.next();
                if (bind(document) instanceof PipelineResource pipeline
                        && pipeline.sources().stream().anyMatch(ref -> !(ref instanceof SourceRef.Spec))) {
                    pipelines.put(String.valueOf(document.get("_id")), pipeline);
                }
            }
        }
        return pipelines;
    }

    /**
     * The resource a stored document holds, or nothing when this build cannot bind it. A body this
     * build does not understand is left exactly as it is: it is not this changeset's to repair, and the
     * store's own read path already reports it where somebody can act on it.
     */
    private static Resource bind(Document document) {
        if (!(document.get("body") instanceof Document body)) {
            return null;
        }
        try {
            return PARSER.fromTree(body);
        } catch (RuntimeException unbindable) {
            return null;
        }
    }

    @Override
    public String dryRunSummary(MongoDatabase database) {
        int pending = bareReferrers(SystemCollections.ARTIFACTS.on(database)).size();
        return pending == 0
                ? "every stored pipeline already records the srs switch it reads each source through"
                : "records the srs switch on " + pending + " pipeline(s) that name a source bare, taking "
                        + "each from the source's own switch -- their content hashes change with it";
    }
}
