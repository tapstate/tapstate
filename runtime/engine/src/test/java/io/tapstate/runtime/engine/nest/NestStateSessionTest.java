package io.tapstate.runtime.engine.nest;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.GroupCommitStateStore;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.StateGroup;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * One vertex instance's state session over a store that groups: what it proves first, what one commit
 * carries, when a group is due, and what a failed commit leaves behind.
 */
class NestStateSessionTest {

    private static final NestCommitBounds BOUNDS = new NestCommitBounds(4, 1024, TimeUnit.HOURS.toNanos(1));

    @Test
    void theGroupIsProvedBeforeTheFirstReadAndCoversEveryNamespace() {
        var store = new Recording();
        var session = new NestStateSession("assembler/0", name -> store, BOUNDS);
        NestStore<String> documents = session.view("state", 1 << 20);
        NestStore<String> parking = session.view("parking", 1 << 20);
        assertThat(store.events).isEmpty();
        assertThat(documents.load("a")).isNull();
        assertThat(store.events).containsExactly("open assembler/0 [state, parking]", "load state");
        assertThatThrownBy(() -> session.view("late", 1 << 20)).isInstanceOf(IllegalStateException.class);
        assertThat(parking.load("b")).isNull();
    }

    @Test
    void oneCommitCarriesEveryNamespaceTheInstanceChanged() {
        var store = new Recording();
        var session = new NestStateSession("resolver/1", name -> store, BOUNDS);
        NestStore<String> state = session.view("state", 1 << 20);
        NestStore<String> parking = session.view("parking", 1 << 20);
        state.save("a", "one");
        parking.save("p", "two");
        state.remove("gone");
        assertThat(store.commits).isEmpty();
        assertThat(state.hasUncommitted()).isTrue();
        state.commit();
        assertThat(store.commits).hasSize(1);
        var committed = store.commits.get(0);
        assertThat(committed.keySet()).containsExactlyInAnyOrder("state", "parking");
        assertThat(committed.get("state").writes()).containsOnlyKeys(NestStateKeys.nameOf("a"));
        assertThat(committed.get("state").removals()).containsExactly(NestStateKeys.nameOf("gone"));
        assertThat(committed.get("parking").writes()).containsOnlyKeys(NestStateKeys.nameOf("p"));
        assertThat(state.hasUncommitted()).isFalse();
        // Committed values stay resident: no store read follows.
        int reads = store.reads;
        assertThat(state.load("a")).isEqualTo("one");
        assertThat(state.load("gone")).isNull();
        assertThat(store.reads).isEqualTo(reads);
    }

    @Test
    void nothingStagedCommitsNothing() {
        var store = new Recording();
        var session = new NestStateSession("lookup/0", name -> store, BOUNDS);
        NestStore<String> rows = session.view("rows", 1 << 20);
        rows.commit();
        assertThat(store.events).isEmpty();
        assertThat(store.commits).isEmpty();
    }

    @Test
    void aGroupIsDueAtItsKeyBoundOrItsByteBoundNotBefore() {
        var store = new Recording();
        var session = new NestStateSession("lookup/0", name -> store, BOUNDS);
        NestStore<String> rows = session.view("rows", 1 << 20);
        rows.save("a", "x");
        rows.save("a", "y");
        rows.save("b", "x");
        rows.save("c", "x");
        assertThat(rows.commitDue()).as("three distinct keys of four").isFalse();
        rows.save("d", "x");
        assertThat(rows.commitDue()).isTrue();
        rows.commit();
        assertThat(rows.commitDue()).isFalse();
        rows.save("big", "z".repeat(600));
        assertThat(rows.commitDue()).isFalse();
        rows.save("bigger", "z".repeat(600));
        assertThat(rows.commitDue()).as("past the byte bound with two keys").isTrue();
    }

    @Test
    void aGroupIsDueOnceItsFirstChangeHasWaitedItsDelay() throws InterruptedException {
        var store = new Recording();
        var session = new NestStateSession("lookup/0", name -> store, new NestCommitBounds(256, 1 << 20, TimeUnit.MILLISECONDS.toNanos(20)));
        NestStore<String> rows = session.view("rows", 1 << 20);
        rows.save("a", "x");
        assertThat(rows.commitDue()).isFalse();
        Thread.sleep(30);
        assertThat(rows.commitDue()).isTrue();
    }

    @Test
    void aValueOverTheByteBoundIsRefusedWithItsCode() {
        var store = new Recording();
        var session = new NestStateSession("assembler/0", name -> store, BOUNDS);
        NestStore<String> state = session.view("state", 1 << 20);
        assertThatThrownBy(() -> state.save("huge", "z".repeat(2048)))
                .isInstanceOfSatisfying(TapstateException.class, refused -> {
                    assertThat(refused.code()).isEqualTo(NestError.STATE_VALUE_TOO_LARGE);
                    assertThat(refused.args()).containsKeys("namespace", "bytes", "limit");
                });
        assertThat(state.hasUncommitted()).isFalse();
    }

    @Test
    void aFailedCommitDropsWhatWasStagedAndTheSessionFailsClosed() {
        var store = new Recording();
        var session = new NestStateSession("assembler/0", name -> store, BOUNDS);
        NestStore<String> state = session.view("state", 1 << 20);
        state.save("a", "kept");
        state.commit();
        state.save("a", "lost");
        store.refuse = true;
        assertThatThrownBy(state::commit).isInstanceOfSatisfying(TapstateException.class,
                refused -> assertThat(refused.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
        assertThat(state.hasUncommitted()).isFalse();
        assertThatThrownBy(() -> state.load("a")).isInstanceOfSatisfying(TapstateException.class,
                refused -> assertThat(refused.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
    }

    @Test
    void setsGrowAndShrinkThroughTheSession() {
        var store = new Recording();
        var session = new NestStateSession("lookup/0", name -> store, BOUNDS);
        NestStore<Set<Object>> references = session.view("references", 1 << 20);
        references.add("bucket", "r1");
        references.addAll(Map.of("bucket", new LinkedHashSet<>(List.of("r2"))));
        assertThat(references.load("bucket")).containsExactlyInAnyOrder("r1", "r2");
        references.remove("bucket", "r1");
        references.remove("bucket", "r2");
        assertThat(references.load("bucket")).isNull();
        references.commit();
        assertThat(store.commits.get(0).get("references").removals()).containsExactly(NestStateKeys.nameOf("bucket"));
    }

    /** A grouping store that records what it was asked and keeps what it committed. */
    private static final class Recording implements GroupCommitStateStore {
        final List<String> events = new ArrayList<>();
        final List<Map<String, StateGroup.Changes>> commits = new ArrayList<>();
        final Map<String, byte[]> held = new LinkedHashMap<>();
        boolean refuse;
        int reads;

        @Override
        public StateGroup openGroup(String vertexInstance, List<GroupNamespace> namespaces) {
            events.add("open " + vertexInstance + " " + namespaces.stream().map(GroupNamespace::namespace).toList());
            return changes -> {
                if (refuse) throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null);
                commits.add(Map.copyOf(new LinkedHashMap<>(changes)));
                changes.forEach((namespace, part) -> {
                    part.writes().forEach((key, value) -> held.put(namespace + "/" + key, value));
                    part.removals().forEach(key -> held.remove(namespace + "/" + key));
                });
            };
        }

        @Override public Optional<byte[]> load(String namespace, String key) {
            events.add("load " + namespace); reads++;
            return Optional.ofNullable(held.get(namespace + "/" + key));
        }
        @Override public Map<String, byte[]> loadAll(String namespace, Collection<String> keys) {
            events.add("loadAll " + namespace); reads++;
            Map<String, byte[]> out = new LinkedHashMap<>();
            for (String key : keys) if (held.containsKey(namespace + "/" + key)) out.put(key, held.get(namespace + "/" + key));
            return out;
        }
        @Override public void save(String namespace, String key, byte[] state) { throw new AssertionError("writes go through the group"); }
        @Override public void saveAll(String namespace, Map<String, byte[]> states) { throw new AssertionError("writes go through the group"); }
        @Override public Optional<byte[]> saveIfAbsent(String namespace, String key, byte[] state) { throw new AssertionError(); }
        @Override public void delete(String namespace, String key) { throw new AssertionError("writes go through the group"); }
        @Override public void dropNamespace(String namespace) { throw new AssertionError(); }
        @Override public long count(String namespace) { return held.size(); }
    }
}
