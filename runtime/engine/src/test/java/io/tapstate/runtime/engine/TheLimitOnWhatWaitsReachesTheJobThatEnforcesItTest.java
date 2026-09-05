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
import io.tapstate.runtime.engine.nest.HeapNestStores;
import io.tapstate.runtime.engine.nest.NestBinding;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.runtime.engine.nest.NestTable;
import io.tapstate.runtime.engine.nest.NestTopology;
import io.tapstate.runtime.engine.nest.NestVertex;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import io.tapstate.spi.transform.TransformPort;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The same seam as the limit on a document's width, for the other limit that fails a run - and for the
 * level the other one never reaches. How much may wait is enforced on every level that holds anything,
 * including the ones between the root and the leaves, and those are supplied by a different line of the
 * graph builder than the level that assembles documents.
 *
 * <p>What fails here if that line is not connected is nothing at all. The level keeps the default limit,
 * holds whatever arrives before its parents, assembles perfectly correct documents out of everything that
 * did resolve, and reports nothing anywhere - while the configuration meant to bound it reads as set from
 * every side. That shape has caught this repository out before, which is why the assertion is a job that
 * stops rather than a value read back.
 *
 * <p>The claims here name a policy that is never read, so what stops the job can only be the queue they
 * wait in: nothing is ever assembled from them, no document grows, and every other limit is left at its
 * default. The vertex is pinned to a single instance because the state under test is per-instance while it
 * is kept on a heap.
 */
class TheLimitOnWhatWaitsReachesTheJobThatEnforcesItTest {

    private static final String PIPELINE = "p";
    private static final String NODE = "customer_doc";

    /** One orphan claim may wait under a policy; the second is what the job must stop on. */
    private static final long LIMIT = 1L;

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
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
    void aLevelHoldingPastTheLimitItWasGivenStopsTheJobSayingSo() {
        DAG dag = customersWithPoliciesAndClaims(LIMIT + 1);

        assertThatThrownBy(() -> member.getJet().newJob(dag).join())
                .hasStackTraceContaining("nest.pending-limit-exceeded")
                .hasStackTraceContaining("pending=" + (LIMIT + 1))
                .hasStackTraceContaining("limit=" + LIMIT);
    }

    @Test
    void aLevelHoldingInsideTheLimitItWasGivenRunsToCompletion() {
        DAG dag = customersWithPoliciesAndClaims(LIMIT);

        member.getJet().newJob(dag).join();
    }

    // ---- the pipeline under test ------------------------------------------------------

    /**
     * customers, with policies embedded beneath and claims beneath those, so policies is a resolver - and
     * {@code orphans} claims naming a policy no read ever produces, which is what that resolver holds.
     */
    private static DAG customersWithPoliciesAndClaims(long orphans) {
        TransformBody.Nest body = tree();

        Map<String, FromRef> aliases = new LinkedHashMap<>();
        aliases.put("customer", FromRef.literal("customers"));
        aliases.put("policy", FromRef.literal("policies"));
        aliases.put("claim", FromRef.literal("claims"));
        Step step = Step.inline(NODE, FromClause.aliases(aliases), body, null);

        PipelineResource pipeline = new PipelineResource(PIPELINE, null,
                List.of("customers", "policies", "claims"),
                List.of(step), null,
                new ServeBlock.Inline("serve", FromRef.literal(NODE),
                        List.of(new SyncElement("sync_1", "dest", null, null, null)), null, null),
                null, null);

        List<Map<String, Object>> claimRows = new ArrayList<>();
        for (int i = 0; i < orphans; i++) {
            claimRows.add(row("claim_id", "cid" + i, "policy_id", "never_read"));
        }

        Map<String, ProcessorMetaSupplier> sources = new LinkedHashMap<>();
        sources.put("customers", rowsSource("customers", List.of(row("customer_id", 1, "name", "n"))));
        sources.put("policies", rowsSource("policies", List.of()));
        sources.put("claims", rowsSource("claims", claimRows));

        DagBindings bindings = new DagBindings(
                sources::get,
                s -> (SupplierEx<TransformPort>) () -> event -> List.of(event),
                syncElement -> (SupplierEx<SinkWriter>) CollectingSinkWriter::new,
                ref -> List.of(((FromRef.Literal) ref).ref()),
                new NestBinding(tables()::get, HeapNestStores.onHeap(), (from, released) -> { },
                        NestSettings.defaults().withPendingLimit(policyLevel(body).mapName(), LIMIT)));

        DAG dag = PipelineDagBuilder.build(pipeline, bindings);
        dag.getVertex(policyLevel(body).name()).localParallelism(1);
        return dag;
    }

    private static TransformBody.Nest tree() {
        Embed claim = new Embed("claim", Map.of("policy_id", "policy_id"), EmbedAs.ARRAY, "claims",
                List.of("claim_id"), null, null, null);
        Embed policy = new Embed("policy", Map.of("customer_id", "customer_id"), EmbedAs.ARRAY, "policies",
                List.of("policy_no"), null, null, List.of(claim));
        return new TransformBody.Nest(null, null,
                new NestRoot("customer", List.of("customer_id"), null, null, List.of(policy)));
    }

    /** The level that holds claims, asked of the compiler rather than spelled out. */
    private static NestVertex policyLevel(TransformBody.Nest body) {
        return NestTopology.compile(PIPELINE, NODE, body, tables()::get).vertexAt(List.of("policies"));
    }

    private static Map<String, NestTable> tables() {
        Map<String, NestTable> tables = new LinkedHashMap<>();
        tables.put("customer", new NestTable("customers", List.of("customer_id")));
        tables.put("policy", new NestTable("policies", List.of("policy_id")));
        tables.put("claim", new NestTable("claims", List.of("claim_id")));
        return tables;
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
            return CompletableFuture.completedFuture(new WriteResult(records.size()));
        }

        @Override
        public void close() {
        }
    }
}
