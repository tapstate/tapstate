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

    /** When a change entered the bucket. These tests are about what is held, not about how long for. */
    private static final long ARRIVED = 1_700_000_000_000L;

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

        assertThat(state.resolve(child("items", 2, "t2", "sku", "a"), ARRIVED)).isEqualTo(Resolution.RESOLVED);
        assertThat(state.parentKey()).isEqualTo("C1");
    }

    @Test
    void aChildThatOutrunsTheMappingIsHeldAndReleasedWhenItArrives() {
        ResolverState state = new ResolverState();
        NestElement early = child("items", 2, "t2", "sku", "a");

        assertThat(state.resolve(early, ARRIVED)).isEqualTo(Resolution.HELD);
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

        state.deleteMapping(at(5));
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
    void deletingTheRowRetainsExistingWaitingUntilAHigherParentDeclaration() {
        ResolverState state=new ResolverState();NestElement waiting=child("items",2,"t2","sku","a");
        state.resolve(waiting,ARRIVED);state.deleteMapping(at(10));
        assertThat(state.waiting()).containsExactly(waiting);assertThat(state.deleted()).isTrue();
        assertThat(state.declare("old",at(9))).isEmpty();assertThat(state.waiting()).containsExactly(waiting);
        assertThat(state.declare("C1",at(11))).containsExactly(waiting);assertThat(state.waiting()).isEmpty();
    }

    @Test
    void aChildArrivingAfterDeletionWaitsForAParentThatCanStillReappear() {
        ResolverState state=new ResolverState();state.declare("C1",at(1));state.deleteMapping(at(10));
        NestElement waiting=child("items",11,"t11","sku","a");
        assertThat(state.resolve(waiting,ARRIVED)).isEqualTo(Resolution.HELD);
        assertThat(state.waiting()).containsExactly(waiting);assertThat(state.declare("C2",at(12))).containsExactly(waiting);
    }

    @Test
    void repeatedParentDeletionsPreserveWaitingAcrossRestoreAndOldDeclarations() throws Exception {
        var state=new ResolverState();
        NestElement waiting=child("items",11,"opaque-child-11","sku","a");
        state.declare("C1",at(1));state.deleteMapping(at(10));
        state.resolve(waiting,ARRIVED);state.deleteMapping(at(12));
        var bytes=new ByteArrayOutputStream();try(var out=new ObjectOutputStream(bytes)){out.writeObject(state);}
        ResolverState restored;try(var in=new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))){restored=(ResolverState)in.readObject();}
        assertThat(restored.deleted()).isTrue();assertThat(restored.waiting()).containsExactly(waiting);
        assertThat(restored.lowestHeldByChain()).containsEntry("items",new ChainPosition(at(11),"opaque-child-11"));
        assertThat(restored.declare("too-old",at(11))).isEmpty();assertThat(restored.waiting()).containsExactly(waiting);
        assertThat(restored.declare("C2",at(13))).containsExactly(waiting);assertThat(restored.waiting()).isEmpty();
    }
    @Test
    void exactChildReplayAfterRestoreDoesNotSpendMorePendingCapacity() throws Exception {
        var state=new ResolverState();
        NestElement waiting=child("items",2,"opaque-2","sku","a","payload",new io.tapstate.core.event.Bytes((byte)4,new byte[]{1,2,3}));
        state.resolve(waiting,ARRIVED);
        var bytes=new ByteArrayOutputStream();try(var out=new ObjectOutputStream(bytes)){out.writeObject(state);}
        ResolverState restored;try(var in=new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))){restored=(ResolverState)in.readObject();}
        assertThat(restored.resolve(waiting,ARRIVED+90_000)).isEqualTo(Resolution.HELD);
        assertThat(restored.pending()).isEqualTo(1);assertThat(restored.declare("C1",at(3))).containsExactly(waiting);
    }
    @Test
    void theWaitingBucketReportsThePositionsTheFrontierMustNotPass() {
        ResolverState state = new ResolverState();
        state.resolve(child("items", 7, "t7", "sku", "a"), ARRIVED);
        state.resolve(child("items", 4, "t4", "sku", "b"), ARRIVED);
        state.resolve(child("prices", 9, "t9", "amount", 1), ARRIVED);

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
        assertThat(state.resolve(child("items", 6, "t6", "sku", "a"), ARRIVED)).isEqualTo(Resolution.RESOLVED);
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
        state.resolve(waiting, ARRIVED);

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
        state.resolve(child("items", 2, "binlog.000042:1024", "sku", "a"), ARRIVED);

        List<NestElement> released = state.declare("C1", at(3));
        assertThat(released).hasSize(1);
        assertThat(released.get(0).positions().get("items").token()).isEqualTo("binlog.000042:1024");
    }
}
