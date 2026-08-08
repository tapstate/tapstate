package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.nest.NestBinding;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.runtime.engine.nest.NestTable;
import io.tapstate.runtime.engine.nest.NestTopology;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import io.tapstate.spi.transform.TransformPort;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A limit is chosen where the job is assembled and enforced on the member running the vertex it is about,
 * and between those two points there is a seam that nothing else covers. Every other test of the limit
 * hands it to a vertex directly; this one hands it to the job and asks the job.
 *
 * <p>What fails here if the seam is not connected is nothing at all: the nest keeps the default limit,
 * runs, and assembles correct documents, while the configuration that was meant to bound it reads as set
 * everywhere it is looked at. That is why the assertion is a job that stops rather than a value read back.
 *
 * <p>How wide one document may grow is the one limit left that fails a run. What a level holds and how
 * many documents there are grow past memory into the layer behind it instead, so neither has a number to
 * carry here; a document does, because it is rendered whole and nothing behind the map assembles half of
 * one.
 *
 * <p>The document is driven to one element past its limit and no further, so what stops the job can only
 * be its width. The vertex is pinned to a single instance because the state under test is per-instance
 * while it is kept on a heap.
 */
class TheLimitOnADocumentReachesTheJobThatEnforcesItTest {

    /** What the sink was handed. Static because the writer is built on the member. */
    private static final List<Envelope> WRITTEN = Collections.synchronizedList(new ArrayList<>());

    private static final String PIPELINE = "p";
    private static final String NODE = "customer_doc";

    /** Two policies fit in the document; the third is what the job must stop on. */
    private static final long LIMIT = 2L;

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        WRITTEN.clear();
        Config config = new Config();
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        member = Hazelcast.newHazelcastInstance(config);
    }

    @AfterEach
    void stopMember() {
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    void aDocumentDrivenPastTheLimitItWasGivenStopsTheJobSayingSo() {
        DAG dag = customersWithPoliciesAndClaims(LIMIT + 1);

        assertThatThrownBy(() -> member.getJet().newJob(dag).join())
                .hasStackTraceContaining("nest.root-fanout-limit-exceeded")
                .hasStackTraceContaining("elements=" + (LIMIT + 1))
                .hasStackTraceContaining("limit=" + LIMIT);
    }

    @Test
    void aDocumentInsideTheLimitItWasGivenRunsToCompletion() {
        DAG dag = customersWithPoliciesAndClaims(LIMIT);

        member.getJet().newJob(dag).join();
    }

    // ---- the pipeline under test ------------------------------------------------------

    /** customers, with policies embedded beneath and claims beneath those, so policies is a resolver. */
    private static DAG customersWithPoliciesAndClaims(long policies) {
        Embed claim = new Embed("claim", Map.of("policy_id", "policy_id"), EmbedAs.ARRAY, "claims",
                List.of("claim_id"), null, null, null);
        Embed policy = new Embed("policy", Map.of("customer_id", "customer_id"), EmbedAs.ARRAY, "policies",
                List.of("policy_no"), null, null, List.of(claim));
        TransformBody.Nest body = new TransformBody.Nest(null, null,
                new NestRoot("customer", List.of("customer_id"), null, null, List.of(policy)));

        Map<String, FromRef> aliases = new LinkedHashMap<>();
        aliases.put("customer", FromRef.literal("customers"));
        aliases.put("policy", FromRef.literal("policies"));
        aliases.put("claim", FromRef.literal("claims"));
        Step step = Step.inline(NODE, FromClause.aliases(aliases), body, null, null);

        PipelineResource pipeline = new PipelineResource(PIPELINE, null,
                List.of("customers", "policies", "claims"),
                List.of(step), null,
                new ServeBlock.Inline("serve", FromRef.literal(NODE),
                        List.of(new SyncElement("sync_1", "dest", null, null, null, null)), null, null),
                null, null);

        List<Map<String, Object>> policyRows = new ArrayList<>();
        for (int i = 0; i < policies; i++) {
            policyRows.add(row("policy_id", "pid" + i, "customer_id", 1, "policy_no", "no" + i));
        }

        Map<String, ProcessorMetaSupplier> sources = new LinkedHashMap<>();
        sources.put("customers", rowsSource("customers", List.of(row("customer_id", 1, "name", "n"))));
        sources.put("policies", rowsSource("policies", policyRows));
        sources.put("claims", rowsSource("claims", List.of()));

        Map<String, NestTable> tables = new LinkedHashMap<>();
        tables.put("customer", new NestTable("customers", List.of("customer_id")));
        tables.put("policy", new NestTable("policies", List.of("policy_id")));
        tables.put("claim", new NestTable("claims", List.of("claim_id")));

        DagBindings bindings = new DagBindings(
                sources::get,
                s -> (SupplierEx<TransformPort>) () -> event -> List.of(event),
                syncElement -> (SupplierEx<SinkWriter>) CollectingSinkWriter::new,
                ref -> List.of(((FromRef.Literal) ref).ref()),
                new NestBinding(tables::get, NestBinding.onHeap(), element -> { },
                        NestSettings.defaults().withElementLimit(documentNamespace(body), LIMIT)));

        DAG dag = PipelineDagBuilder.build(pipeline, bindings);
        dag.getVertex("nest:" + NODE).localParallelism(1);
        return dag;
    }

    /** The name the compiler gives the document level, asked of the compiler rather than spelled out. */
    private static String documentNamespace(TransformBody.Nest body) {
        Map<String, NestTable> tables = new LinkedHashMap<>();
        tables.put("customer", new NestTable("customers", List.of("customer_id")));
        tables.put("policy", new NestTable("policies", List.of("policy_id")));
        tables.put("claim", new NestTable("claims", List.of("claim_id")));
        return NestTopology.compile(PIPELINE, NODE, body, tables::get)
                .assembler()
                .mapName();
    }

    // ---- doubles ----------------------------------------------------------------------

    private static Map<String, Object> row(Object... fields) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 0; i < fields.length; i += 2) {
            row.put((String) fields[i], fields[i + 1]);
        }
        return row;
    }

    private static ProcessorMetaSupplier rowsSource(String src, List<Map<String, Object>> rows) {
        return ProcessorMetaSupplier.forceTotalParallelismOne(
                ProcessorSupplier.of((SupplierEx<Processor>) () -> new RowsSource(src, rows)));
    }

    private static final class RowsSource extends AbstractProcessor {

        private final String src;
        private final List<Map<String, Object>> rows;
        private int next;

        RowsSource(String src, List<Map<String, Object>> rows) {
            this.src = src;
            this.rows = rows;
        }

        @Override
        public boolean complete() {
            while (next < rows.size()) {
                Envelope event = Envelope.insert(next + 1L, src, rows.get(next), null)
                        .withOrder(new SourceOrder(1, next));
                if (!tryEmit(event)) {
                    return false;
                }
                next++;
            }
            return true;
        }
    }

    private static final class CollectingSinkWriter implements SinkWriter {

        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            WRITTEN.addAll(records);
            return CompletableFuture.completedFuture(new WriteResult(records.size()));
        }

        @Override
        public void close() {
        }
    }
}
