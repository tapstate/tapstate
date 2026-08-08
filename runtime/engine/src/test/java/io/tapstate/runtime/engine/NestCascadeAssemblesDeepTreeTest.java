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
 * Runs a four-level nest inside a real Jet job, the shape that separates a cascade of resolvers from a
 * single one. The deepest table names only its immediate parent: a document row carries a claim key and
 * nothing else, so the root it belongs to exists nowhere in the row and has to be resolved a level at a
 * time on the way up. Every two-level test is satisfied by an implementation that resolves once, and no
 * such implementation can satisfy this one - a claim row cannot reach the root key from inside its own
 * partition.
 *
 * <p>What the assertions discriminate is not that the deepest rows arrive but where they arrive. An
 * implementation that hangs each descendant off whichever ancestor was at hand produces a document
 * holding exactly the elements seeded here, at the wrong paths, and every count passes. So each level is
 * walked by path and what is found there must be the elements whose foreign key names that parent, and
 * nothing else. Two customers, and a policy that owns a claim with no documents of its own, are what
 * make "wrong parent" and "everything piled on one" visible.
 *
 * <p>The sources emit concurrently and in no fixed order, so this also stands as the coarse witness that
 * a descendant arriving before its ancestor is held rather than dropped.
 */
class NestCascadeAssemblesDeepTreeTest {

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
    void theDeepestRowsLandUnderTheAncestorsThatOwnThemRatherThanAnyThatWereHandy() {
        member.getJet().newJob(customersFourDeep()).join();

        Map<Object, Map<String, Object>> documents = latestPerRoot();
        assertThat(documents.keySet())
                .describedAs("every customer that arrived has a document, and nothing invented one")
                .containsExactlyInAnyOrder(1, 2);

        List<Map<String, Object>> firstPolicies = arrayAt(documents.get(1), "policies");
        assertThat(keysOf(firstPolicies, "policy_id"))
                .describedAs("customer 1 holds its own two policies and not customer 2's")
                .containsExactlyInAnyOrder(10, 11);

        Map<String, Object> policyTen = elementWith(firstPolicies, "policy_id", 10);
        List<Map<String, Object>> claimsOfTen = arrayAt(policyTen, "claims");
        assertThat(keysOf(claimsOfTen, "claim_id"))
                .describedAs("the claim of policy 10, three levels below the root that carries it")
                .containsExactly(100);
        assertThat(keysOf(arrayAt(elementWith(claimsOfTen, "claim_id", 100), "documents"), "doc_id"))
                .describedAs("both documents of claim 100, four levels below a root their rows never name")
                .containsExactlyInAnyOrder(1000, 1001);

        Map<String, Object> policyEleven = elementWith(firstPolicies, "policy_id", 11);
        List<Map<String, Object>> claimsOfEleven = arrayAt(policyEleven, "claims");
        assertThat(keysOf(claimsOfEleven, "claim_id"))
                .describedAs("policy 11 keeps its own claim rather than policy 10 taking it")
                .containsExactly(101);
        assertThat(arrayAt(elementWith(claimsOfEleven, "claim_id", 101), "documents"))
                .describedAs("claim 101 has no documents, and an empty array is what says so")
                .isEmpty();

        List<Map<String, Object>> secondPolicies = arrayAt(documents.get(2), "policies");
        assertThat(keysOf(secondPolicies, "policy_id"))
                .describedAs("customer 2 is not a place customer 1's policies ended up")
                .containsExactly(20);
        List<Map<String, Object>> claimsOfTwenty = arrayAt(elementWith(secondPolicies, "policy_id", 20), "claims");
        assertThat(keysOf(claimsOfTwenty, "claim_id")).containsExactly(200);
        assertThat(keysOf(arrayAt(elementWith(claimsOfTwenty, "claim_id", 200), "documents"), "doc_id"))
                .describedAs("the deepest row of the second root, which a cascade resolving only one root loses")
                .containsExactly(2000);
    }

    // ---- the pipeline under test ------------------------------------------------------

    /**
     * customers with policies beneath them, claims beneath those, and documents beneath those. Each
     * level names only the level above it: no policy carries a claim key, and no document carries a
     * customer key. It compiles to one resolver vertex per non-leaf embed - policies and claims, two of
     * them - plus the assembler, so a document row travels three hops to reach the sink.
     */
    private static DAG customersFourDeep() {
        Embed documents = new Embed("d", Map.of("claim_id", "claim_id"), EmbedAs.ARRAY, "documents",
                List.of("doc_id"), null, null, null);
        Embed claims = new Embed("cl", Map.of("policy_id", "policy_id"), EmbedAs.ARRAY, "claims",
                List.of("claim_id"), null, null, List.of(documents));
        Embed policies = new Embed("p", Map.of("customer_id", "customer_id"), EmbedAs.ARRAY, "policies",
                List.of("policy_id"), null, null, List.of(claims));
        TransformBody.Nest body = new TransformBody.Nest(null, null,
                new NestRoot("c", List.of("customer_id"), null, null, List.of(policies)));

        Map<String, FromRef> aliases = new LinkedHashMap<>();
        aliases.put("c", FromRef.literal("customers"));
        aliases.put("p", FromRef.literal("policies"));
        aliases.put("cl", FromRef.literal("claims"));
        aliases.put("d", FromRef.literal("documents"));
        Step step = Step.inline("customer_doc", FromClause.aliases(aliases), body, null, null);

        PipelineResource pipeline = new PipelineResource("p", null,
                List.of("customers", "policies", "claims", "documents"),
                List.of(step), null,
                new ServeBlock.Inline("serve", FromRef.literal("customer_doc"),
                        List.of(new SyncElement("sync_1", "dest", null, null, null, null)), null, null),
                null, null);

        Map<String, ProcessorMetaSupplier> sources = new LinkedHashMap<>();
        sources.put("customers", rowsSource("customers",
                List.of(row("customer_id", 1, "name", "first"), row("customer_id", 2, "name", "second"))));
        sources.put("policies", rowsSource("policies",
                List.of(row("policy_id", 10, "customer_id", 1, "plan", "p10"),
                        row("policy_id", 11, "customer_id", 1, "plan", "p11"),
                        row("policy_id", 20, "customer_id", 2, "plan", "p20"))));
        sources.put("claims", rowsSource("claims",
                List.of(row("claim_id", 100, "policy_id", 10, "amount", 100),
                        row("claim_id", 101, "policy_id", 11, "amount", 101),
                        row("claim_id", 200, "policy_id", 20, "amount", 200))));
        // Only a claim key: the root these belong to is nowhere in the row.
        sources.put("documents", rowsSource("documents",
                List.of(row("doc_id", 1000, "claim_id", 100, "title", "d1000"),
                        row("doc_id", 1001, "claim_id", 100, "title", "d1001"),
                        row("doc_id", 2000, "claim_id", 200, "title", "d2000"))));

        Map<String, NestTable> tables = new LinkedHashMap<>();
        tables.put("c", new NestTable("customers", List.of("customer_id")));
        tables.put("p", new NestTable("policies", List.of("policy_id")));
        tables.put("cl", new NestTable("claims", List.of("claim_id")));
        tables.put("d", new NestTable("documents", List.of("doc_id")));

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

    /**
     * The array a parent carries at a path. An absent path is the failure this test is here to catch -
     * the level above assembled and nothing was ever attached beneath it - so it is reported as that
     * rather than as a null dereference.
     */
    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> arrayAt(Map<String, Object> parent, String path) {
        assertThat(parent).describedAs("nothing was assembled at this level").isNotNull();
        Object embedded = parent.get(path);
        assertThat(embedded)
                .describedAs("no '%s' under %s: the level above it reached the sink with nothing beneath", path, parent)
                .isInstanceOf(List.class);
        return (List<Map<String, Object>>) embedded;
    }

    /** The one element of an array whose own key has a value, so a path can be walked further down. */
    private static Map<String, Object> elementWith(List<Map<String, Object>> elements, String key, Object value) {
        for (Map<String, Object> element : elements) {
            if (value.equals(element.get(key))) {
                return element;
            }
        }
        throw new AssertionError("no element with " + key + "=" + value + " among " + elements);
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
