package io.tapstate.runtime.engine.nest;

import io.tapstate.core.event.ChainPosition;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;

import static io.tapstate.runtime.engine.nest.NestFixtures.at;
import static io.tapstate.runtime.engine.nest.NestFixtures.element;
import static io.tapstate.runtime.engine.nest.NestFixtures.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

class ResolverStateTest {

    /** One child element waiting on this key, carrying the position the frontier must not pass. */
    private static NestElement child(String chain, long seq, String token, Object... pairs) {
        return new NestElement(
                element(List.of("items"), "P1", pairs[1], null),
                row(pairs), at(seq),
                Map.of(chain, new ChainPosition(at(seq), token)));
    }

    @Test
    void aRowDeclaresTheParentItsChildrenResolveTo() {
        ResolverState state = new ResolverState();
        state.declare("C1", at(1));

        assertThat(state.resolve(child("items", 2, "t2", "sku", "a"))).isEqualTo(Resolution.RESOLVED);
        assertThat(state.parentKey()).isEqualTo("C1");
    }

    @Test
    void aChildThatOutrunsTheMappingIsHeldAndReleasedWhenItArrives() {
        ResolverState state = new ResolverState();
        NestElement early = child("items", 2, "t2", "sku", "a");

        assertThat(state.resolve(early)).isEqualTo(Resolution.HELD);
        assertThat(state.waiting()).containsExactly(early);

        assertThat(state.declare("C1", at(3))).containsExactly(early);
        assertThat(state.waiting()).isEmpty();
    }

    @Test
    void theHigherOrderedRowWinsAndAnOlderOneIsRefused() {
        ResolverState state = new ResolverState();
        state.declare("C1", at(5));

        assertThat(state.declare("C0", at(4))).isEmpty();
        assertThat(state.parentKey()).isEqualTo("C1");

        state.declare("C2", at(6));
        assertThat(state.parentKey()).isEqualTo("C2");
    }

    @Test
    void aTieKeepsTheMappingAlreadyThere() {
        ResolverState state = new ResolverState();
        state.declare("C1", at(5));

        // Two events that compare equal are never two versions of one row: within one epoch every
        // snapshot row shares the reserved sequence, and a snapshot yields one row per key.
        assertThat(state.declare("C2", at(5))).isEmpty();
        assertThat(state.parentKey()).isEqualTo("C1");
    }

    @Test
    void aTieDoesNotTurnALiveMappingIntoATombstone() {
        ResolverState state = new ResolverState();
        state.declare("C1", at(5));

        assertThat(state.deleteMapping(at(5))).isEmpty();
        assertThat(state.deleted()).isFalse();
        assertThat(state.parentKey()).isEqualTo("C1");
    }

    @Test
    void deletingTheRowLeavesAVersionedTombstoneNotAHole() {
        ResolverState state = new ResolverState();
        state.declare("C1", at(5));
        state.deleteMapping(at(10));

        assertThat(state.deleted()).isTrue();
        assertThat(state.parentKey()).isNull();

        // A child row replayed from beneath the tombstone must not resolve to the parent that is gone.
        state.declare("C1", at(9));
        assertThat(state.deleted()).isTrue();

        // A genuine rebuild above it revives the mapping.
        state.declare("C9", at(11));
        assertThat(state.deleted()).isFalse();
        assertThat(state.parentKey()).isEqualTo("C9");
    }

    @Test
    void deletingTheRowDrainsWhatWasWaitingImmediately() {
        ResolverState state = new ResolverState();
        NestElement orphaned = child("items", 2, "t2", "sku", "a");
        state.resolve(orphaned);

        // The parent is known deleted, so the bucket empties now rather than waiting for a timeout: those
        // children can never resolve, and holding them would pin the frontier for nothing.
        assertThat(state.deleteMapping(at(10))).containsExactly(orphaned);
        assertThat(state.waiting()).isEmpty();
    }

    @Test
    void aChildArrivingAfterTheRowIsDeletedIsNotHeld() {
        ResolverState state = new ResolverState();
        state.declare("C1", at(1));
        state.deleteMapping(at(10));

        assertThat(state.resolve(child("items", 11, "t11", "sku", "a")))
                .isEqualTo(Resolution.PARENT_ABSENT);
        assertThat(state.waiting()).isEmpty();
    }

    @Test
    void theWaitingBucketReportsThePositionsTheFrontierMustNotPass() {
        ResolverState state = new ResolverState();
        state.resolve(child("items", 7, "t7", "sku", "a"));
        state.resolve(child("items", 4, "t4", "sku", "b"));
        state.resolve(child("prices", 9, "t9", "amount", 1));

        assertThat(state.lowestHeldByChain()).containsExactly(
                Map.entry("items", new ChainPosition(at(4), "t4")),
                Map.entry("prices", new ChainPosition(at(9), "t9")));
    }

    @Test
    void aLiveMappingResolvesRatherThanHolds() {
        ResolverState state = new ResolverState();
        state.declare("C1", at(5));

        // The bucket is non-empty only while there is no mapping entry at all: a live mapping resolves on
        // the spot and a tombstone answers absent, so nothing accumulates behind either.
        assertThat(state.resolve(child("items", 6, "t6", "sku", "a"))).isEqualTo(Resolution.RESOLVED);
        assertThat(state.waiting()).isEmpty();
    }

    @Test
    void everyMutationCarriesAnOrder() {
        ResolverState state = new ResolverState();
        assertThatNullPointerException().isThrownBy(() -> state.declare("C1", null));
        assertThatNullPointerException().isThrownBy(() -> state.deleteMapping(null));
    }

    @Test
    void theStateRoundTripsWithItsTombstoneAndItsWaitingChildren() throws Exception {
        ResolverState state = new ResolverState();
        NestElement waiting = child("items", 2, "t2", "sku", "a");
        state.resolve(waiting);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(state);
        }
        ResolverState restored;
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            restored = (ResolverState) in.readObject();
        }

        assertThat(restored.waiting()).containsExactly(waiting);
        assertThat(restored.declare("C1", at(3))).containsExactly(waiting);
    }

    @Test
    void aHeldChildKeepsTheTokenItArrivedWith() {
        ResolverState state = new ResolverState();
        state.resolve(child("items", 2, "binlog.000042:1024", "sku", "a"));

        List<NestElement> released = state.declare("C1", at(3));
        assertThat(released).hasSize(1);
        assertThat(released.get(0).positions().get("items").token()).isEqualTo("binlog.000042:1024");
    }
}
