package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;

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
import io.tapstate.runtime.engine.nest.NestTable;
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
 * Runs nests whose descendants arrive before the rows they hang from, which is the ordinary case rather
 * than the awkward one: separate tables are read by separate chains, and nothing sequences them.
 *
 * <p>A row that arrives with nowhere to go has to be held until its place exists. The implementation that
 * drops it instead is invisible to every other test here - they all seed roots and children together and
 * let arrival order fall where it may, so a run where the root happened to land first passes and says
 * nothing about the run where it did not.
 *
 * <p>There are two such holding places and they are not the same code. A child of the root is attached at
 * once to an assembly that has no root yet - there is always somewhere to put it - and it is the document
 * that is withheld, until a root arrives to make one. A row deeper than that never reaches the assembler
 * at all: its parent key cannot be mapped to a root until the middle row declares it, so a resolver holds
 * it. A two-level fixture drives only the first, so both are driven here.
 *
 * <p>Order is forced rather than hoped for: the ancestor sources hold off before emitting, so the
 * descendants are already through when they start. Both jobs then run to completion, and what the sink
 * was handed last is what the tree converged to.
 */
class NestHoldsDescendantsUntilAncestorsArriveTest {

    /** What the sink was handed, in arrival order. Static because the writer is built on the member. */
    private static final List<Envelope> WRITTEN = Collections.synchronizedList(new ArrayList<>());

    /** How long an ancestor source waits, so its descendants are certain to be through first. */
    private static final long ROOT_DELAY_MILLIS = 400;
    private static final long MIDDLE_DELAY_MILLIS = 200;

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
    void aChildThatArrivesBeforeItsRootIsHeldRatherThanDropped() {
        member.getJet().newJob(ordersWithLateRoot()).join();

        Map<Object, Map<String, Object>> documents = latestPerRoot("order_id");
        assertThat(documents.keySet())
                .describedAs("the root that arrived last still has a document")
                .containsExactly(1);
        assertThat(keysOf(arrayAt(documents.get(1), "items"), "item_id"))
                .describedAs("the children that arrived before there was a root were kept, not dropped")
                .containsExactlyInAnyOrder(10, 11, 12);
    }

    @Test
    void aGrandchildThatArrivesBeforeItsMiddleRowIsHeldInsideTheResolver() {
        member.getJet().newJob(customersWithLateAncestors()).join();

        Map<Object, Map<String, Object>> documents = latestPerRoot("customer_id");
        assertThat(documents.keySet())
                .describedAs("the root that arrived last of all still has a document")
                .containsExactly(1);

        List<Map<String, Object>> policies = arrayAt(documents.get(1), "policies");
        assertThat(keysOf(policies, "policy_id"))
                .describedAs("the middle row, which was itself waiting on the root when it arrived")
                .containsExactly(10);
        assertThat(keysOf(arrayAt(policies.get(0), "claims"), "claim_id"))
                .describedAs("the grandchildren, which had no resolvable root when they arrived")
                .containsExactlyInAnyOrder(100, 101);
    }

    // ---- the pipelines under test -----------------------------------------------------

    /** A root and one leaf embed, with the root source holding off: the children get there first. */
    private static DAG ordersWithLateRoot() {
        Embed item = new Embed("item", Map.of("order_id", "order_id"), EmbedAs.ARRAY, "items",
                List.of("item_id"), null, null, null);
        TransformBody.Nest body = new TransformBody.Nest(null, null,
                new NestRoot("order", List.of("order_id"), null, null, List.of(item)));

        Map<String, FromRef> aliases = new LinkedHashMap<>();
        aliases.put("order", FromRef.literal("orders"));
        aliases.put("item", FromRef.literal("order_items"));

        Map<String, ProcessorMetaSupplier> sources = new LinkedHashMap<>();
        sources.put("orders", rowsSource("orders", List.of(row("order_id", 1, "code", "A")),
                ROOT_DELAY_MILLIS));
        sources.put("order_items", rowsSource("order_items",
                List.of(row("item_id", 10, "order_id", 1),
                        row("item_id", 11, "order_id", 1),
                        row("item_id", 12, "order_id", 1)),
                0));

        Map<String, NestTable> tables = new LinkedHashMap<>();
        tables.put("order", new NestTable("orders", List.of("order_id")));
        tables.put("item", new NestTable("order_items", List.of("item_id")));

        return build("order_doc", body, aliases, sources, tables,
                List.of("orders", "order_items"));
    }

    /**
     * Three levels with both ancestors holding off, the deeper one for less time: the claims are through
     * before the policy that resolves them, and the policy before the customer it hangs from. The claims
     * therefore reach a resolver that cannot yet answer, which is the holding place a two-level tree has
     * no way to reach.
     */
    private static DAG customersWithLateAncestors() {
        Embed claims = new Embed("cl", Map.of("policy_id", "policy_id"), EmbedAs.ARRAY, "claims",
                List.of("claim_id"), null, null, null);
        Embed policies = new Embed("p", Map.of("customer_id", "customer_id"), EmbedAs.ARRAY, "policies",
                List.of("policy_id"), null, null, List.of(claims));
        TransformBody.Nest body = new TransformBody.Nest(null, null,
                new NestRoot("c", List.of("customer_id"), null, null, List.of(policies)));

        Map<String, FromRef> aliases = new LinkedHashMap<>();
        aliases.put("c", FromRef.literal("customers"));
        aliases.put("p", FromRef.literal("policies"));
        aliases.put("cl", FromRef.literal("claims"));

        Map<String, ProcessorMetaSupplier> sources = new LinkedHashMap<>();
        sources.put("customers", rowsSource("customers", List.of(row("customer_id", 1, "name", "first")),
                ROOT_DELAY_MILLIS));
        sources.put("policies", rowsSource("policies",
                List.of(row("policy_id", 10, "customer_id", 1)), MIDDLE_DELAY_MILLIS));
        sources.put("claims", rowsSource("claims",
                List.of(row("claim_id", 100, "policy_id", 10), row("claim_id", 101, "policy_id", 10)), 0));

        Map<String, NestTable> tables = new LinkedHashMap<>();
        tables.put("c", new NestTable("customers", List.of("customer_id")));
        tables.put("p", new NestTable("policies", List.of("policy_id")));
        tables.put("cl", new NestTable("claims", List.of("claim_id")));

        return build("customer_doc", body, aliases, sources, tables,
                List.of("customers", "policies", "claims"));
    }

    private static DAG build(String nodeId, TransformBody.Nest body, Map<String, FromRef> aliases,
            Map<String, ProcessorMetaSupplier> sources, Map<String, NestTable> tables, List<String> streams) {
        Step step = Step.inline(nodeId, FromClause.aliases(aliases), body, null, null);
        PipelineResource pipeline = new PipelineResource("p", null, streams, List.of(step), null,
                new ServeBlock.Inline("serve", FromRef.literal(nodeId),
                        List.of(new SyncElement("sync_1", "dest", null, null, null, null)), null, null),
                null, null);

        DagBindings bindings = new DagBindings(
                sources::get,
                s -> (SupplierEx<TransformPort>) () -> event -> List.of(event),
                syncElement -> (SupplierEx<SinkWriter>) CollectingSinkWriter::new,
                ref -> List.of(((FromRef.Literal) ref).ref()),
                new NestBinding(tables::get, NestBinding.onHeap(), element -> { }));

        return PipelineDagBuilder.build(pipeline, bindings);
    }

    // ---- reading what came out --------------------------------------------------------

    /** The last state each root reached, keyed by its root key: a document is emitted per drain. */
    private static Map<Object, Map<String, Object>> latestPerRoot(String rootKey) {
        Map<Object, Map<String, Object>> latest = new LinkedHashMap<>();
        for (Envelope written : WRITTEN) {
            Map<String, Object> document = written.after();
            if (document != null) {
                latest.put(document.get(rootKey), document);
            }
        }
        return latest;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> arrayAt(Map<String, Object> parent, String path) {
        assertThat(parent).describedAs("nothing was assembled at this level").isNotNull();
        Object embedded = parent.get(path);
        assertThat(embedded)
                .describedAs("no '%s' under %s: what was waiting never landed", path, parent)
                .isInstanceOf(List.class);
        return (List<Map<String, Object>>) embedded;
    }

    private static List<Object> keysOf(List<Map<String, Object>> elements, String key) {
        return elements.stream().map(element -> element.get(key)).toList();
    }

    // ---- doubles ----------------------------------------------------------------------

    private static Map<String, Object> row(Object... fields) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 0; i < fields.length; i += 2) {
            row.put((String) fields[i], fields[i + 1]);
        }
        return row;
    }

    /**
     * A source that turns rows into inserts on the member, each stamped with the order the engine would
     * have given it, after holding off for as long as it was told to. A stateful node crashes bare on an
     * event with no order, so a synthetic source that leaves it null tests nothing.
     */
    private static ProcessorMetaSupplier rowsSource(String src, List<Map<String, Object>> rows, long delay) {
        return ProcessorMetaSupplier.forceTotalParallelismOne(
                ProcessorSupplier.of((SupplierEx<Processor>) () -> new RowsSource(src, rows, delay)));
    }

    private static final class RowsSource extends AbstractProcessor {

        private final String src;
        private final List<Map<String, Object>> rows;
        private final long delay;
        private int next;
        private long emitAt;

        RowsSource(String src, List<Map<String, Object>> rows, long delay) {
            this.src = src;
            this.rows = rows;
            this.delay = delay;
        }

        /** A source that sleeps may not hold a cooperative thread while it does. */
        @Override
        public boolean isCooperative() {
            return delay == 0;
        }

        @Override
        public boolean complete() {
            if (delay > 0) {
                if (emitAt == 0) {
                    emitAt = System.currentTimeMillis() + delay;
                }
                long wait = emitAt - System.currentTimeMillis();
                if (wait > 0) {
                    try {
                        Thread.sleep(wait);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        return true;
                    }
                }
            }
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
