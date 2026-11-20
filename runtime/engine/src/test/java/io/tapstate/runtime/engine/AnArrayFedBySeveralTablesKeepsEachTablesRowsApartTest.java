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
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.nest.HeapNestStores;
import io.tapstate.runtime.engine.nest.NestBinding;
import io.tapstate.runtime.engine.nest.NestClock;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.runtime.engine.nest.NestStateLedger;
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
import java.util.regex.Pattern;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * One array in a document fed by several tables at once, inside a real Jet job.
 *
 * <p>The shape is the one a ledger-style document takes: a player's point, comp and award rows are
 * three tables with three different owners, and the document wants them as one list of movements.
 * Nothing in a nest lets one embed name three aliases, so the tables are merged before the nest - each
 * table's rows tagged with which table they came from, the tagged streams put through a {@code union},
 * and the union embedded as one array whose element identity is the tag together with the row's own id.
 * The same shape covers one table split across several physical tables (sessions sharded by a number
 * in the table name), where the merge reads them by pattern.
 *
 * <p>What the assertions have to discriminate:
 *
 * <ul>
 *   <li>The row ids deliberately collide across tables: each table has a row 1. An element identity
 *       that dropped the tag would fold three rows into one, and the array would still be non-empty -
 *       so its length is read, and each element's contents too.</li>
 *   <li>An update and a delete each land in exactly one table while a row with the same id sits in the
 *       others. A change addressed by the id alone reaches the wrong element, or every element sharing
 *       that id; the untouched rows are read back to show it reached neither.</li>
 *   <li>Two players, so an implementation that piles every element onto whichever root arrived first
 *       is caught by the second player's array.</li>
 * </ul>
 *
 * <p>The tagging step here is a stand-in for a {@code map} that writes a literal column on both of an
 * event's rows: that is the contract of the real map step, held by its own tests. What only a real job
 * shows is the part between - a merge of several chains feeding one nest level and keeping its frontier
 * moving, with each element's state filed under a key that carries the tag.
 */
class AnArrayFedBySeveralTablesKeepsEachTablesRowsApartTest {

    /** What the sink was handed, in arrival order. Static because the writer is built on the member. */
    private static final List<Envelope> WRITTEN = Collections.synchronizedList(new ArrayList<>());

    private static final String MOVEMENTS = "item_movement";
    private static final String SESSIONS = "sessions";

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
    void threeTablesWithCollidingIdsBecomeOneArrayAndEachChangeReachesOnlyItsOwnElement() {
        Map<String, List<Change>> changes = new LinkedHashMap<>();
        changes.put("players", List.of(
                insert("players", row("PlayerID", 1, "Name", "ada")),
                insert("players", row("PlayerID", 2, "Name", "grace"))));
        changes.put("points", List.of(
                insert("points", row("id", 1, "PlayerID", 1, "amount", 10)),
                insert("points", row("id", 2, "PlayerID", 2, "amount", 20)),
                update("points", row("id", 1, "PlayerID", 1, "amount", 10),
                        row("id", 1, "PlayerID", 1, "amount", 15))));
        changes.put("comps", List.of(
                insert("comps", row("id", 1, "PlayerID", 1, "amount", 5)),
                insert("comps", row("id", 2, "PlayerID", 1, "amount", 6)),
                delete("comps", row("id", 2, "PlayerID", 1, "amount", 6))));
        changes.put("awards", List.of(
                insert("awards", row("id", 1, "PlayerID", 1, "amount", 100)),
                insert("awards", row("id", 2, "PlayerID", 2, "amount", 200)),
                delete("awards", row("id", 2, "PlayerID", 2, "amount", 200))));

        member.getJet().newJob(ledger(changes)).join();

        Map<Object, Map<String, Object>> documents = latestPerRoot();
        assertThat(documents.keySet()).containsExactlyInAnyOrder(1, 2);
        assertThat(elements(documents.get(1), MOVEMENTS))
                .describedAs("player 1 holds row 1 of each table: three elements, not one per id")
                .extracting(e -> e.get("kind") + "/" + e.get("id") + "=" + e.get("amount"))
                .containsExactlyInAnyOrder("points/1=15", "comps/1=5", "awards/1=100");
        assertThat(elements(documents.get(2), MOVEMENTS))
                .describedAs("player 2 keeps its point row; its award was deleted, and the delete of "
                        + "comps row 2 under player 1 did not reach the points row 2 here")
                .extracting(e -> e.get("kind") + "/" + e.get("id") + "=" + e.get("amount"))
                .containsExactly("points/2=20");
    }

    @Test
    void shardedTablesReadByPatternBecomeOneArrayAndADeleteInOneShardLeavesTheOthers() {
        Map<String, List<Change>> changes = new LinkedHashMap<>();
        changes.put("players", List.of(insert("players", row("PlayerID", 1, "Name", "ada"))));
        // The session id restarts in every shard, which is what makes the shard part of the identity.
        changes.put("PlayerSession0", List.of(
                insert("PlayerSession0", row("SessionID", 7, "PlayerID", 1, "EndTime", 100)),
                insert("PlayerSession0", row("SessionID", 8, "PlayerID", 1, "EndTime", 110))));
        changes.put("PlayerSession3", List.of(
                insert("PlayerSession3", row("SessionID", 7, "PlayerID", 1, "EndTime", 300)),
                delete("PlayerSession3", row("SessionID", 7, "PlayerID", 1, "EndTime", 300)),
                insert("PlayerSession3", row("SessionID", 9, "PlayerID", 1, "EndTime", 390))));
        changes.put("PlayerSession9", List.of(
                insert("PlayerSession9", row("SessionID", 7, "PlayerID", 1, "EndTime", 900)),
                update("PlayerSession9", row("SessionID", 7, "PlayerID", 1, "EndTime", 900),
                        row("SessionID", 7, "PlayerID", 1, "EndTime", 950))));

        member.getJet().newJob(sessions(changes)).join();

        Map<Object, Map<String, Object>> documents = latestPerRoot();
        assertThat(elements(documents.get(1), SESSIONS))
                .describedAs("session 7 exists in three shards and is three elements; the one deleted "
                        + "from shard 3 is gone and the other two stay; the update in shard 9 lands there")
                .extracting(e -> e.get("shard") + "/" + e.get("SessionID") + "=" + e.get("EndTime"))
                .containsExactlyInAnyOrder("PlayerSession0/7=100", "PlayerSession0/8=110",
                        "PlayerSession3/9=390", "PlayerSession9/7=950");
    }

    /**
     * A row that changes which player it belongs to leaves the one it came from and joins the other,
     * with its element identity - tag and id - intact. Two rows share the id: the one that moves and one
     * in another table that stays, so a move addressed by the id alone takes the wrong one along.
     */
    @Test
    void aRowThatMovesToAnotherPlayerTakesOnlyItsOwnElementWithIt() {
        Map<String, List<Change>> changes = new LinkedHashMap<>();
        changes.put("players", List.of(
                insert("players", row("PlayerID", 1, "Name", "ada")),
                insert("players", row("PlayerID", 2, "Name", "grace"))));
        changes.put("points", List.of(
                insert("points", row("id", 1, "PlayerID", 1, "amount", 10)),
                update("points", row("id", 1, "PlayerID", 1, "amount", 10),
                        row("id", 1, "PlayerID", 2, "amount", 10))));
        changes.put("comps", List.of(insert("comps", row("id", 1, "PlayerID", 1, "amount", 5))));
        changes.put("awards", List.of());

        member.getJet().newJob(ledger(changes, true)).join();

        Map<Object, Map<String, Object>> documents = latestPerRoot();
        assertThat(elements(documents.get(1), MOVEMENTS))
                .describedAs("the point row left player 1; the comp row with the same id stayed")
                .extracting(e -> e.get("kind") + "/" + e.get("id"))
                .containsExactly("comps/1");
        assertThat(elements(documents.get(2), MOVEMENTS))
                .describedAs("the point row arrived at player 2, once")
                .extracting(e -> e.get("kind") + "/" + e.get("id"))
                .containsExactly("points/1");
    }

    // ---- the pipelines under test -----------------------------------------------------

    /**
     * players as the root; points, comps and awards each tagged with a literal {@code kind}, merged by
     * a union, and embedded as {@code item_movement} with the tag and the row's own id as its array key.
     */
    private static DAG ledger(Map<String, List<Change>> changes) {
        return ledger(changes, false);
    }

    private static DAG ledger(Map<String, List<Change>> changes, boolean trackKeyChanges) {
        List<Step> steps = new ArrayList<>();
        Map<String, String> tagOf = new LinkedHashMap<>();
        for (String table : List.of("points", "comps", "awards")) {
            String step = "tagged_" + table;
            tagOf.put(step, table);
            steps.add(Step.inline(step, FromClause.list(FromRef.literal(table)), mapPlaceholder(), null));
        }
        steps.add(Step.inline("movements", FromClause.list(FromRef.literal("tagged_points"),
                FromRef.literal("tagged_comps"), FromRef.literal("tagged_awards")), new TransformBody.Union(), null));
        Embed movement = new Embed("m", Map.of("PlayerID", "PlayerID"), EmbedAs.ARRAY, MOVEMENTS,
                null, List.of("kind", "id"), null, trackKeyChanges ? Boolean.TRUE : null, null);
        steps.add(nest(Map.of("p", "players", "m", "movements"), movement));
        return build(steps, changes, "movements", step -> {
            String tag = tagOf.get(step.id());
            return () -> event -> List.of(tagged(event, "kind", tag));
        });
    }

    /**
     * players as the root; every {@code PlayerSessionN} table read by one pattern into a step that tags
     * each row with the table it came from, embedded as {@code sessions} with that tag and the id as its array key.
     */
    private static DAG sessions(Map<String, List<Change>> changes) {
        List<Step> steps = new ArrayList<>();
        steps.add(Step.inline("all_sessions", FromClause.list(FromRef.regex("PlayerSession[0-9]")),
                mapPlaceholder(), null));
        Embed session = new Embed("s", Map.of("PlayerID", "PlayerID"), EmbedAs.ARRAY, SESSIONS,
                null, List.of("shard", "SessionID"), null, null, null);
        steps.add(nest(Map.of("p", "players", "s", "all_sessions"), session));
        return build(steps, changes, "all_sessions",
                step -> () -> event -> List.of(tagged(event, "shard", event.src())));
    }

    private static Step nest(Map<String, String> from, Embed embed) {
        Map<String, FromRef> aliases = new LinkedHashMap<>();
        from.entrySet().stream().sorted(Map.Entry.comparingByKey())
                .forEach(alias -> aliases.put(alias.getKey(), FromRef.literal(alias.getValue())));
        TransformBody.Nest body = new TransformBody.Nest(null, null,
                new NestRoot("p", List.of("PlayerID"), null, null, List.of(embed)));
        return Step.inline("player_doc", FromClause.aliases(aliases), body, null);
    }

    /** A step body for the vertex a tagging port runs in; the port itself is what the binding supplies. */
    private static TransformBody mapPlaceholder() {
        return new TransformBody.Filter("true");
    }

    private static DAG build(List<Step> steps, Map<String, List<Change>> changes, String childStep,
            java.util.function.Function<Step, SupplierEx<TransformPort>> ports) {
        List<SourceRef> sourceRefs = changes.keySet().stream().<SourceRef>map(SourceRef::bare).toList();
        PipelineResource pipeline = new PipelineResource("p", null, sourceRefs, steps, null,
                new ServeBlock.Inline("serve", FromRef.literal("player_doc"),
                        List.of(new SyncElement("sync_1", "dest", null, null, null)), null, null),
                null, null);

        Map<String, ProcessorMetaSupplier> sources = new LinkedHashMap<>();
        changes.forEach((table, events) -> sources.put(table, changesSource(events)));

        Map<String, NestTable> tables = new LinkedHashMap<>();
        tables.put("p", new NestTable("players", List.of("PlayerID")));
        // A step's output has no table key; the embed declares the identity it needs, as an author must.
        tables.put(childStep.equals("movements") ? "m" : "s", new NestTable(childStep, List.of()));

        List<String> tableNames = List.copyOf(changes.keySet());
        DagBindings bindings = new DagBindings(
                sources::get,
                ports::apply,
                syncElement -> (SupplierEx<SinkWriter>) CollectingSinkWriter::new,
                ref -> ref instanceof FromRef.Regex regex
                        ? tableNames.stream().filter(Pattern.compile(regex.pattern()).asMatchPredicate()).toList()
                        : List.of(((FromRef.Literal) ref).ref()),
                new NestBinding(tables::get, HeapNestStores.onHeap(), (from, released) -> { },
                        member -> ReplayFloor.NONE, NestStateLedger.NONE, NestSettings.defaults(),
                        NestClock.SYSTEM));
        return PipelineDagBuilder.build(pipeline, bindings);
    }

    /** The event with {@code field} set to {@code value} on both of its rows, as a literal map column is. */
    private static Envelope tagged(Envelope event, String field, Object value) {
        return new Envelope(event.op(), event.ts(), event.src(), with(event.before(), field, value),
                with(event.after(), field, value), event.schema());
    }

    private static Map<String, Object> with(Map<String, Object> row, String field, Object value) {
        if (row == null) {
            return null;
        }
        Map<String, Object> out = new LinkedHashMap<>(row);
        out.put(field, value);
        return out;
    }

    // ---- reading what came out --------------------------------------------------------

    /** The last state each root reached, keyed by its root key: a document is emitted per drain. */
    private static Map<Object, Map<String, Object>> latestPerRoot() {
        Map<Object, Map<String, Object>> latest = new LinkedHashMap<>();
        for (Envelope written : WRITTEN) {
            Map<String, Object> document = written.after();
            if (document != null) {
                latest.put(document.get("PlayerID"), document);
            }
        }
        return latest;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> elements(Map<String, Object> document, String path) {
        assertThat(document).describedAs("no document was assembled for this root").isNotNull();
        Object embedded = document.get(path);
        assertThat(embedded).describedAs("the document carries no %s array: %s", path, document).isNotNull();
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

    private static Change insert(String table, Map<String, Object> after) {
        return new Change(table, null, after);
    }

    private static Change update(String table, Map<String, Object> before, Map<String, Object> after) {
        return new Change(table, before, after);
    }

    private static Change delete(String table, Map<String, Object> before) {
        return new Change(table, before, null);
    }

    /** One change to one table, as the source will emit it; serializable because the source is shipped. */
    private record Change(String table, Map<String, Object> before, Map<String, Object> after)
            implements java.io.Serializable {

        Change {
            before = before == null ? null : new LinkedHashMap<>(before);
            after = after == null ? null : new LinkedHashMap<>(after);
        }

        Envelope at(int index) {
            long ts = index + 1L;
            Envelope event = before == null ? Envelope.insert(ts, table, after, null)
                    : after == null ? Envelope.delete(ts, table, before, null)
                    : Envelope.update(ts, table, before, after, null);
            return event.withOrder(new SourceOrder(1, index));
        }
    }

    /**
     * A source that emits its table's changes in the order given, each stamped with the order the engine
     * would have given it. A stateful node crashes bare on an event with no order.
     */
    private static ProcessorMetaSupplier changesSource(List<Change> events) {
        List<Change> copy = List.copyOf(events);
        return ProcessorMetaSupplier.forceTotalParallelismOne(ProcessorSupplier.of(
                (SupplierEx<Processor>) () -> new ChangesSource(copy)));
    }

    private static final class ChangesSource extends AbstractProcessor {

        private final List<Change> events;
        private int next;

        ChangesSource(List<Change> events) {
            this.events = events;
        }

        @Override
        public boolean complete() {
            while (next < events.size()) {
                if (!tryEmit(events.get(next).at(next))) {
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
