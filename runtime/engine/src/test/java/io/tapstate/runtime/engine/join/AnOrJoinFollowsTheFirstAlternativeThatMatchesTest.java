package io.tapstate.runtime.engine.join;

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Partitioner;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.Processors;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.sql.JoinPlan;
import io.tapstate.core.sql.JoinTree;
import io.tapstate.core.sql.SourceColumn;
import io.tapstate.core.sql.SourceTable;
import io.tapstate.core.sql.SqlFrontEnd;
import io.tapstate.runtime.engine.NodeWidth;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A join on {@code OR} kept under change: a wallet transaction names its location by location code or
 * by hub location id, and its published row follows whichever alternative matches first as rows of
 * either table arrive, change and leave.
 *
 * <p>Every location change is delivered once per alternative, because that is how the graph wires it:
 * the rows of the one table the SQL names feed each alternative's own edge.
 */
class AnOrJoinFollowsTheFirstAlternativeThatMatchesTest {

    private static final List<SourceTable> TABLES = List.of(
            new SourceTable("wallet_tran", List.of(
                    new SourceColumn("tran_id", TapstateType.INT64, false),
                    new SourceColumn("locn_code", TapstateType.STRING, true),
                    new SourceColumn("hub_locn_id", TapstateType.STRING, true))),
            new SourceTable("location", List.of(
                    new SourceColumn("locn_id", TapstateType.INT64, false),
                    new SourceColumn("locn_code", TapstateType.STRING, true),
                    new SourceColumn("hub_locn_id", TapstateType.STRING, true),
                    new SourceColumn("name", TapstateType.STRING, false))));

    private static final String SQL = "SELECT t.tran_id, l.name AS location "
            + "FROM wallet_tran t LEFT JOIN location l "
            + "ON t.locn_code = l.locn_code OR t.hub_locn_id = l.hub_locn_id";

    private final JoinPlan plan = SqlFrontEnd.derive(SQL, TABLES);
    private final JoinDriver driver = new JoinDriver(plan, List.of("tran_id"), "wallet_doc",
            new CountingJoinStores(ReverseIndex.DEFAULT_PAGE_SIZE));
    private final List<Envelope> taken = new ArrayList<>();
    private final JoinSink sink = change -> {
        taken.add(change);
        return true;
    };

    @Test
    @DisplayName("a transaction's location follows the first alternative that matches, through every change")
    void theFirstMatchingAlternativeWinsUnderChange() {
        location(Envelope.insert(1L, "src", loc(1, "A", null, "Atrium"), null));
        location(Envelope.insert(2L, "src", loc(2, null, "H2", "Harbor"), null));
        tran(Envelope.insert(3L, "src", tran(1, "A", null), null));
        tran(Envelope.insert(4L, "src", tran(2, null, "H2"), null));
        tran(Envelope.insert(5L, "src", tran(3, "B", "H2"), null));
        tran(Envelope.insert(6L, "src", tran(4, null, null), null));

        assertThat(locationOf(1)).as("by code").isEqualTo("Atrium");
        assertThat(locationOf(2)).as("by hub id").isEqualTo("Harbor");
        assertThat(locationOf(3)).as("no location has code B, so the hub id is the fallback").isEqualTo("Harbor");
        assertThat(lastFor(4)).as("a transaction naming neither is kept, with no location")
                .extracting(Envelope::after).isNotNull();
        assertThat(locationOf(4)).isNull();

        location(Envelope.insert(7L, "src", loc(3, "B", null, "Bayview"), null));
        assertThat(locationOf(3)).as("the first alternative matches now, and wins").isEqualTo("Bayview");

        location(Envelope.delete(8L, "src", loc(3, "B", null, "Bayview"), null));
        assertThat(locationOf(3)).as("and when it stops matching, the fallback is back").isEqualTo("Harbor");

        location(Envelope.update(9L, "src", loc(2, null, "H2", "Harbor"), loc(2, null, "H9", "Harbor"), null));
        assertThat(locationOf(2)).as("the location moved off H2: nothing names it by H2 any more").isNull();
        assertThat(locationOf(3)).isNull();

        tran(Envelope.update(10L, "src", tran(3, "B", "H2"), tran(3, "B", "H9"), null));
        assertThat(locationOf(3)).as("the transaction follows it to H9").isEqualTo("Harbor");

        location(Envelope.update(11L, "src", loc(2, null, "H9", "Harbor"), loc(2, null, "H9", "Harbour"), null));
        assertThat(locationOf(3)).as("a rename reaches the documents matched by hub id").isEqualTo("Harbour");
        assertThat(locationOf(1)).as("and leaves the ones matched by code alone").isEqualTo("Atrium");
    }

    @Test
    @DisplayName("a transaction with no hub id never matches a location with no hub id")
    void nullsMatchNothing() {
        location(Envelope.insert(1L, "src", loc(1, "A", null, "Atrium"), null));
        tran(Envelope.insert(2L, "src", tran(1, "Z", null), null));

        assertThat(locationOf(1)).isNull();
    }

    @Test
    @DisplayName("the location table feeds every alternative, each on an edge keyed by its own column")
    void eachAlternativeIsFedByTheTableTheSqlNames() {
        DAG dag = new DAG();
        Map<String, Vertex> upstream = new LinkedHashMap<>();
        upstream.put("t", dag.newVertex("wallet_tran_src", Processors.noopP()));
        upstream.put("l", dag.newVertex("location_src", Processors.noopP()));
        Map<Vertex, Integer> outbound = new HashMap<>();
        JoinDag.attach(dag, plan, "p", "j", List.of("tran_id"), Map.of("l", List.of("locn_id")),
                source -> List.of(upstream.get(source)), vertex -> outbound.merge(vertex, 1, Integer::sum) - 1,
                JoinStoresBinding.onTheCluster(), DimensionRowDisplacedAlert.NONE, new NodeWidth("j", 4, 1, null));

        var locationRouters = dag.getOutboundEdges("location_src");
        assertThat(locationRouters).hasSize(2).allSatisfy(edge ->
                assertThat(edge.getRoutingPolicy()).isEqualTo(Edge.RoutingPolicy.ISOLATED));
        List<Edge> fromLocations = locationRouters.stream()
                .flatMap(edge -> dag.getOutboundEdges(edge.getDestName()).stream())
                .filter(edge -> edge.getDestName().equals("j"))
                .sorted(Comparator.comparingInt(Edge::getDestOrdinal))
                .toList();
        assertThat(fromLocations).extracting(Edge::getDestOrdinal).containsExactly(1, 2);
        Envelope row = Envelope.insert(1L, "src", loc(2, "A", "H2", "Harbor"), null);
        assertThat(routingKeyOf(fromLocations.get(0), row))
                .as("the code alternative routes a location by its code")
                .isEqualTo(routingKeyOf(fromLocations.get(0), Envelope.insert(1L, "src", loc(9, "A", "H7", "x"), null)))
                .isNotEqualTo(routingKeyOf(fromLocations.get(0), Envelope.insert(1L, "src", loc(2, "B", "H2", "x"), null)));
        assertThat(routingKeyOf(fromLocations.get(1), row))
                .as("the hub alternative routes it by its hub id")
                .isEqualTo(routingKeyOf(fromLocations.get(1), Envelope.insert(1L, "src", loc(9, "Z", "H2", "x"), null)));
    }

    private static Object routingKeyOf(Edge edge, Envelope event) {
        @SuppressWarnings("unchecked")
        Partitioner<Object> partitioner = (Partitioner<Object>) edge.getPartitioner();
        AtomicReference<Object> routed = new AtomicReference<>();
        partitioner.init(key -> {
            routed.set(key);
            return 0;
        });
        partitioner.getPartition(event, 271);
        return routed.get();
    }

    private void location(Envelope change) {
        // the same change, on the edge of every source that reads the location table
        for (JoinTree.Source source : plan.from().sources()) {
            if (source.declared().equals("l")) {
                apply(new SourceChange(source.name(), change));
            }
        }
    }

    private void tran(Envelope change) {
        apply(new SourceChange("t", change));
    }

    private String locationOf(long tranId) {
        Envelope last = lastFor(tranId);
        return last == null || last.op() == Op.DELETE ? null : (String) last.after().get("location");
    }

    private Envelope lastFor(long tranId) {
        Envelope found = null;
        for (Envelope event : taken) {
            Map<String, Object> row = event.after() != null ? event.after() : event.before();
            if (row != null && Long.valueOf(tranId).equals(row.get("tran_id"))) {
                found = event;
            }
        }
        return found;
    }

    private void apply(SourceChange change) {
        if (driver.apply(List.of(change), sink)) {
            return;
        }
        for (int offer = 0; offer < 10_000; offer++) {
            if (driver.apply(List.of(), sink)) {
                return;
            }
        }
        throw new AssertionError("the driver never finished with nothing arriving");
    }

    private static Map<String, Object> loc(long id, String code, String hub, String name) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("locn_id", id);
        row.put("locn_code", code);
        row.put("hub_locn_id", hub);
        row.put("name", name);
        return row;
    }

    private static Map<String, Object> tran(long id, String code, String hub) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("tran_id", id);
        row.put("locn_code", code);
        row.put("hub_locn_id", hub);
        return row;
    }
}
