package io.tapstate.runtime.engine.join;

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Partitioner;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.Processors;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.event.ConvertedValue;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.sql.Expr;
import io.tapstate.core.sql.JoinKind;
import io.tapstate.core.sql.JoinPlan;
import io.tapstate.core.sql.JoinTree;
import io.tapstate.core.sql.OutputField;
import io.tapstate.runtime.engine.NodeWidth;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The key a join matches and routes by is built from what a value is, not from the box it arrived in.
 *
 * <p>A value a source connector converted travels inside a carrier, together with the name that
 * source's schema gives it. Every boundary using a row value <em>as a value</em> has to expose the
 * value first, and a join key is such a boundary twice over. A carrier never equals the plain value
 * inside it, so a side that met a conversion and a side that did not simply never meet; two carriers
 * do compare by their parts, which makes it worse rather than better, because two schemas spelling
 * one column differently then stop matching as well.
 *
 * <p><b>Nothing reports any of it.</b> The job runs, the rows arrive, and the joined half is null for
 * ever. There is no error, no warning, and no empty result that looks unusual - a fact row whose
 * dimension lookup found nothing is indistinguishable from one whose dimension genuinely is not
 * there. So the property has to be asserted; it cannot be noticed.
 *
 * <p><b>One case, because it is one path.</b> A change is routed before it is matched, and both steps
 * read a key off the raw row. Routing is the step a single member cannot show: it owns every
 * partition, so a key derived two ways still meets itself, and the day it does not is the day a
 * second member joins. Asserting the match alone would therefore be satisfied by half a repair - the
 * two sides would agree on what they are matching on while the edge still sent one dimension row's
 * changes to two processors, which is one mirror entry written from two places, out of order, with
 * the job running and nothing said.
 */
class AJoinKeyIsTheValueRatherThanTheCarrierItArrivedInTest {

    private static final String STREAM = "order_state";

    /** The join vertex's name in the graph drawn below. */
    private static final String JOIN = "j";

    /** Ordinal 0 carries the driving source; each dimension source follows in the plan's order. */
    private static final int DIMENSION_EDGE = 1;

    /**
     * The mixed pairing, which is the one an example keying a document store to itself cannot produce.
     * The dimension is a collection whose key the connector converts, so it arrives carried; the fact
     * is a relational table holding the same identifier as the plain text it is.
     */
    private static final Map<String, Object> CUSTOMER =
            Map.of("id", new ConvertedValue("64f0c0de", "OBJECT_ID"), "name", "Ada");

    private static final Map<String, Object> THE_SAME_CUSTOMER_UNCONVERTED =
            Map.of("id", "64f0c0de", "name", "Ada");

    private static final Map<String, Object> ANOTHER_CUSTOMER =
            Map.of("id", "64f0c0df", "name", "Grace");

    private static final Map<String, Object> ORDER = Map.of("id", 10L, "cust_id", "64f0c0de");

    @Test
    @DisplayName("a side that met a connector conversion joins to the side that did not")
    void aConvertedValueAndThePlainValueInsideItAreOneJoinKey() {
        DAG dag = graphWithOneJoin();

        // Routed first: a change reaches the processor holding the state it is about to change, and
        // which processor that is must not depend on whether the value came in a carrier. Two
        // spellings of one identifier are one dimension row to the mirror, so they are one place.
        Object carried = routingKeyOf(dag, DIMENSION_EDGE, event(CUSTOMER));
        assertThat(carried)
                .as("the edge routes a dimension row by the value its key holds, so the carried "
                        + "spelling and the plain one address the one mirror entry they are")
                .isEqualTo(routingKeyOf(dag, DIMENSION_EDGE, event(THE_SAME_CUSTOMER_UNCONVERTED)));
        assertThat(carried)
                .as("and a different customer is still a different place -- routing everything "
                        + "together would satisfy the line above and nothing else")
                .isNotEqualTo(routingKeyOf(dag, DIMENSION_EDGE, event(ANOTHER_CUSTOMER)));

        // Then matched: the fact row points at that customer by the plain identifier it holds.
        List<Envelope> published = new ArrayList<>();
        JoinDriver driver = new JoinDriver(PLAN, List.of("id"), STREAM, new MapJoinStores());
        apply(driver, published, new SourceChange("c", event(CUSTOMER)));
        apply(driver, published, new SourceChange("o", event(ORDER)));

        assertThat(published)
                .as("the order is published: a left join emits it whether or not the customer matched")
                .singleElement()
                .extracting(change -> change.after().get("customer_name"))
                .as("the customer's name is joined in -- null here is the whole defect, and it is "
                        + "what an operator sees instead of an error")
                .isEqualTo("Ada");
    }

    /** The graph one join node draws, with a stand-in vertex feeding each source it reads. */
    private static DAG graphWithOneJoin() {
        DAG dag = new DAG();
        Map<String, Vertex> upstream = new LinkedHashMap<>();
        upstream.put("o", dag.newVertex("orders_src", Processors.noopP()));
        upstream.put("c", dag.newVertex("customers_src", Processors.noopP()));
        JoinDag.attach(dag, PLAN, "p", JOIN, List.of("id"), Map.of("c", List.of("id")),
                source -> List.of(upstream.get(source)), vertex -> 0,
                JoinStoresBinding.onTheCluster(), DimensionRowDisplacedAlert.NONE, new NodeWidth(JOIN, 4, 1, null));
        return dag;
    }

    /**
     * The key the edge into the join on {@code ordinal} routes {@code event} by.
     *
     * <p>Read out of the partitioner rather than derived a second time here: a second derivation would
     * only ever agree with itself. The partition count is arbitrary because nothing here asks where the
     * key landed, only what the key was.
     */
    private static Object routingKeyOf(DAG dag, int ordinal, Envelope event) {
        Edge edge = dag.getInboundEdges(JOIN).stream()
                .filter(candidate -> candidate.getDestOrdinal() == ordinal)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("no edge into the join on ordinal " + ordinal));
        @SuppressWarnings("unchecked")
        Partitioner<Object> partitioner = (Partitioner<Object>) edge.getPartitioner();
        assertThat(partitioner)
                .as("the edge into the join on ordinal %d is partitioned at all", ordinal)
                .isNotNull();
        AtomicReference<Object> routed = new AtomicReference<>();
        partitioner.init(key -> {
            routed.set(key);
            return 0;
        });
        partitioner.getPartition(event, 271);
        return routed.get();
    }

    /**
     * Hands {@code change} to the driver and lets it finish. A refused sink is the only reason to
     * offer again, and the sink below never refuses, so the bound is a guard against a driver that
     * stopped making progress rather than a loop this case expects to go round.
     */
    private static void apply(JoinDriver driver, List<Envelope> published, SourceChange change) {
        JoinSink sink = event -> published.add(event);
        if (driver.apply(List.of(change), sink)) {
            return;
        }
        for (int offer = 0; offer < 1000; offer++) {
            if (driver.apply(List.of(), sink)) {
                return;
            }
        }
        throw new AssertionError("the driver never finished with nothing arriving");
    }

    private static Envelope event(Map<String, Object> row) {
        return Envelope.insert(1L, "src", row, null);
    }

    private static final JoinPlan PLAN = new JoinPlan(
            List.of(new OutputField("order_id", TapstateType.INT64, false,
                            new Expr.Column(new JoinTree.ColumnRef("o", "id"))),
                    new OutputField("customer_name", TapstateType.STRING, true,
                            new Expr.Column(new JoinTree.ColumnRef("c", "name")))),
            new JoinTree.Join(
                    new JoinTree.Source("o", "orders"),
                    new JoinTree.Source("c", "customers"),
                    JoinKind.LEFT,
                    List.of(new JoinTree.KeyPair(new JoinTree.ColumnRef("o", "cust_id"),
                            new JoinTree.ColumnRef("c", "id"))),
                    false),
            Map.of("o", List.of("cust_id", "id"), "c", List.of("id", "name")));
}
