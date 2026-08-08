package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.processor.Processors;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
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
import io.tapstate.runtime.engine.nest.NestStateLedger;
import io.tapstate.runtime.engine.nest.NestTable;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import io.tapstate.spi.transform.TransformPort;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

/**
 * Where the refusal is actually reached from. The comparison itself is pinned beside the ledger; what
 * these pin is that building a nest's sub-graph consults it at all, and that it does so before any vertex
 * is drawn - a job that were built first and refused afterwards would already have named the maps it was
 * about to read the wrong entries out of.
 *
 * <p>A pipeline with no nest in it never reaches the ledger, which is the other half of the wiring: the
 * record belongs to the tree, so a pipeline that has no tree must not leave one behind.
 */
class ANestJobIsNotBuiltOntoStateOfAnotherShapeTest {

    @Test
    void aTreeCompiledIntoAJobRecordsThePathsItWillKeepItsStateUnder() {
        RecordingLedger ledger = new RecordingLedger();

        PipelineDagBuilder.build(pipelineWith(tree("orders", "lines")), bindings(ledger));

        assertThat(ledger.recorded.get("p/doc"))
                .describedAs("every path the tree addresses state by, the root's own included")
                .containsExactlyInAnyOrder("$root", "policies", "policies.claims", "orders", "orders.lines");
    }

    @Test
    void aTreeWhoseStateWasFiledUnderOtherPathsIsRefusedBeforeAnyVertexIsDrawn() {
        RecordingLedger ledger = new RecordingLedger()
                .holding("p", "doc", Set.of("$root", "policies", "policies.claims", "orders", "orders.lines"));

        assertThatThrownBy(() ->
                PipelineDagBuilder.build(pipelineWith(tree("orders", "items")), bindings(ledger)))
                .isInstanceOf(TapstateException.class)
                .extracting(thrown -> ((TapstateException) thrown).code().code())
                .isEqualTo("nest.state-paths-changed");
    }

    @Test
    void aPipelineWithNoNestInItLeavesNoRecordBehind() {
        RecordingLedger ledger = new RecordingLedger();
        Step plain = Step.inline("doc", FromClause.list(FromRef.literal("customers")),
                new TransformBody.Filter("true"), null, null);

        PipelineDagBuilder.build(pipeline(plain), bindings(ledger));

        assertThat(ledger.recorded).isEmpty();
    }

    // ---- fixtures ---------------------------------------------------------------------

    private static TransformBody.Nest tree(String orderPath, String itemPath) {
        return new TransformBody.Nest(null, null, new NestRoot("customer", List.of("customer_id"), null, null,
                List.of(
                        embed("policy", "customer_id", "policies", List.of("policy_no"),
                                embed("claim", "policy_id", "claims", List.of("claim_id"))),
                        embed("order", "customer_id", orderPath, List.of("order_id"),
                                embed("item", "order_id", itemPath, List.of("item_id"))))));
    }

    private static Embed embed(String alias, String parentField, String path, List<String> arrayKey,
            Embed... children) {
        return new Embed(alias, Map.of(parentField, parentField), EmbedAs.ARRAY, path, arrayKey, null, null,
                children.length == 0 ? null : List.of(children));
    }

    private static PipelineResource pipelineWith(TransformBody.Nest body) {
        Map<String, FromRef> aliases = new LinkedHashMap<>();
        aliases.put("customer", FromRef.literal("customers"));
        aliases.put("policy", FromRef.literal("policies"));
        aliases.put("claim", FromRef.literal("claims"));
        aliases.put("order", FromRef.literal("orders"));
        aliases.put("item", FromRef.literal("items"));
        return pipeline(Step.inline("doc", FromClause.aliases(aliases), body, null, null));
    }

    private static PipelineResource pipeline(Step step) {
        return new PipelineResource("p", null,
                List.of("customers", "policies", "claims", "orders", "items"),
                List.of(step), null,
                new ServeBlock.Inline("serve", FromRef.literal("doc"),
                        List.of(new SyncElement("sync_1", "dest", null, null, null, null)), null, null),
                null, null);
    }

    private static DagBindings bindings(NestStateLedger ledger) {
        Map<String, NestTable> tables = new LinkedHashMap<>();
        tables.put("customer", new NestTable("customers", List.of("customer_id")));
        tables.put("policy", new NestTable("policies", List.of("policy_id")));
        tables.put("claim", new NestTable("claims", List.of("claim_id")));
        tables.put("order", new NestTable("orders", List.of("order_id")));
        tables.put("item", new NestTable("items", List.of("item_id")));
        return new DagBindings(
                srcId -> ProcessorMetaSupplier.of(Processors.mapP(FunctionEx.identity())),
                step -> (SupplierEx<TransformPort>) () -> ev -> List.of(ev),
                syncElement -> (SupplierEx<SinkWriter>) NoOpSinkWriter::new,
                ref -> List.of(((FromRef.Literal) ref).ref()),
                new NestBinding(tables::get, NestBinding.onHeap(), element -> { },
                        ReplayFloorFactory.NONE, ledger));
    }

    /** A ledger that keeps what it is told in memory, so a test can see both what it read and what it wrote. */
    private static final class RecordingLedger implements NestStateLedger {

        private static final long serialVersionUID = 1L;

        private final Map<String, Set<String>> recorded = new LinkedHashMap<>();

        RecordingLedger holding(String pipelineId, String nodeId, Set<String> paths) {
            recorded.put(pipelineId + "/" + nodeId, new LinkedHashSet<>(paths));
            return this;
        }

        @Override
        public Set<String> recall(String pipelineId, String nodeId) {
            return recorded.getOrDefault(pipelineId + "/" + nodeId, Set.of());
        }

        @Override
        public void record(String pipelineId, String nodeId, Set<String> paths) {
            recorded.put(pipelineId + "/" + nodeId, new LinkedHashSet<>(paths));
        }
    }

    private static final class NoOpSinkWriter implements SinkWriter {

        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            return CompletableFuture.completedFuture(new WriteResult(records.size()));
        }

        @Override
        public void close() {
        }
    }
}
