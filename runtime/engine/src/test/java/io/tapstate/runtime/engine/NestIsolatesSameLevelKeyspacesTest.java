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
 * Runs a root carrying two non-leaf embeds side by side, whose own key values collide. Each of them
 * resolves a root for its children, and the tree is only correct if those two resolutions never share a
 * keyspace: a mapping stored under the bare key 77 by one of them is a mapping the other would read.
 *
 * <p>The fixture makes that read wrong rather than harmless. Policy 77 belongs to customer 1 and order 77
 * to customer 2, and the same crossing holds at 88, so an implementation that lets one branch's mapping
 * answer the other's lookup sends every element to the other root. Both roots keep the right number of
 * elements either way - what changes is which root they are under, and nothing but reading the paths
 * sees it.
 *
 * <p>Every other nest test is safe from this by omission: none of them has two embeds at the same level,
 * so no two branches ever store a mapping at once and the collision has nowhere to happen.
 */
class NestIsolatesSameLevelKeyspacesTest {

    /** What the sink was handed, in arrival order. Static because the writer is built on the member. */
    private static final List<Envelope> WRITTEN = Collections.synchronizedList(new ArrayList<>());

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
    void oneBranchsMappingDoesNotAnswerTheOtherBranchsLookupWhenTheirKeysCollide() {
        member.getJet().newJob(customersWithPoliciesAndOrders()).join();

        Map<Object, Map<String, Object>> documents = latestPerRoot();
        assertThat(documents.keySet())
                .describedAs("both customers have a document")
                .containsExactlyInAnyOrder(1, 2);

        // Customer 1 owns policy 77 and order 88; customer 2 owns the same numbers the other way round.
        assertBranch(documents.get(1), "policies", 77, "claims", 700, "customer 1");
        assertBranch(documents.get(1), "orders", 88, "items", 8000, "customer 1");
        assertBranch(documents.get(2), "policies", 88, "claims", 800, "customer 2");
        assertBranch(documents.get(2), "orders", 77, "items", 7000, "customer 2");
    }

    /**
     * One branch of a root: the single parent element it must hold and the single child beneath that. A
     * branch answered by the other branch's mapping holds the element belonging to the other root, so
     * both the parent's key and the leaf under it are read.
     */
    private static void assertBranch(Map<String, Object> document, String branch, int parentKey,
            String childPath, int childKey, String whose) {
        List<Map<String, Object>> parents = arrayAt(document, branch, whose);
        assertThat(parents.stream().map(parent -> parent.get(keyOf(branch))).toList())
                .describedAs("the '%s' of %s, which the other branch's key of the same value must not answer",
                        branch, whose)
                .containsExactly(parentKey);
        assertThat(arrayAt(parents.get(0), childPath, whose + " " + branch + " " + parentKey).stream()
                        .map(child -> child.get(keyOf(childPath)))
                        .toList())
                .describedAs("the '%s' under '%s' %d of %s", childPath, branch, parentKey, whose)
                .containsExactly(childKey);
    }

    private static String keyOf(String path) {
        return switch (path) {
            case "policies" -> "policy_id";
            case "orders" -> "order_id";
            case "claims" -> "claim_id";
            case "items" -> "item_id";
            default -> throw new IllegalArgumentException(path);
        };
    }

    // ---- the pipeline under test ------------------------------------------------------

    /**
     * customers with two non-leaf embeds hanging off it at the same level, each carrying one child of its
     * own. Two resolver vertices are compiled, one per non-leaf embed, and the keys they resolve overlap
     * by construction.
     */
    private static DAG customersWithPoliciesAndOrders() {
        Embed claims = new Embed("cl", Map.of("policy_id", "policy_id"), EmbedAs.ARRAY, "claims",
                List.of("claim_id"), null, null, null);
        Embed policies = new Embed("p", Map.of("customer_id", "customer_id"), EmbedAs.ARRAY, "policies",
                List.of("policy_id"), null, null, List.of(claims));
        Embed items = new Embed("i", Map.of("order_id", "order_id"), EmbedAs.ARRAY, "items",
                List.of("item_id"), null, null, null);
        Embed orders = new Embed("o", Map.of("customer_id", "customer_id"), EmbedAs.ARRAY, "orders",
                List.of("order_id"), null, null, List.of(items));
        TransformBody.Nest body = new TransformBody.Nest(null, null,
                new NestRoot("c", List.of("customer_id"), null, null, List.of(policies, orders)));

        Map<String, FromRef> aliases = new LinkedHashMap<>();
        aliases.put("c", FromRef.literal("customers"));
        aliases.put("p", FromRef.literal("policies"));
        aliases.put("cl", FromRef.literal("claims"));
        aliases.put("o", FromRef.literal("orders"));
        aliases.put("i", FromRef.literal("items"));
        Step step = Step.inline("customer_doc", FromClause.aliases(aliases), body, null, null);

        PipelineResource pipeline = new PipelineResource("p", null,
                List.of("customers", "policies", "claims", "orders", "items"),
                List.of(step), null,
                new ServeBlock.Inline("serve", FromRef.literal("customer_doc"),
                        List.of(new SyncElement("sync_1", "dest", null, null, null, null)), null, null),
                null, null);

        Map<String, ProcessorMetaSupplier> sources = new LinkedHashMap<>();
        sources.put("customers", rowsSource("customers",
                List.of(row("customer_id", 1, "name", "first"), row("customer_id", 2, "name", "second"))));
        // The collision: policy 77 and order 77 both exist, and they hang from different customers.
        sources.put("policies", rowsSource("policies",
                List.of(row("policy_id", 77, "customer_id", 1), row("policy_id", 88, "customer_id", 2))));
        sources.put("orders", rowsSource("orders",
                List.of(row("order_id", 77, "customer_id", 2), row("order_id", 88, "customer_id", 1))));
        sources.put("claims", rowsSource("claims",
                List.of(row("claim_id", 700, "policy_id", 77), row("claim_id", 800, "policy_id", 88))));
        sources.put("items", rowsSource("items",
                List.of(row("item_id", 7000, "order_id", 77), row("item_id", 8000, "order_id", 88))));

        Map<String, NestTable> tables = new LinkedHashMap<>();
        tables.put("c", new NestTable("customers", List.of("customer_id")));
        tables.put("p", new NestTable("policies", List.of("policy_id")));
        tables.put("cl", new NestTable("claims", List.of("claim_id")));
        tables.put("o", new NestTable("orders", List.of("order_id")));
        tables.put("i", new NestTable("items", List.of("item_id")));

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
    private static Map<Object, Map<String, Object>> latestPerRoot() {
        Map<Object, Map<String, Object>> latest = new LinkedHashMap<>();
        for (Envelope written : WRITTEN) {
            Map<String, Object> document = written.after();
            if (document != null) {
                latest.put(document.get("customer_id"), document);
            }
        }
        return latest;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> arrayAt(Map<String, Object> parent, String path, String whose) {
        assertThat(parent).describedAs("nothing was assembled for %s", whose).isNotNull();
        Object embedded = parent.get(path);
        assertThat(embedded)
                .describedAs("no '%s' under %s: %s", path, whose, parent)
                .isInstanceOf(List.class);
        return (List<Map<String, Object>>) embedded;
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
     * have given it. A stateful node crashes bare on an event with no order, so a synthetic source that
     * leaves it null tests nothing.
     */
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
