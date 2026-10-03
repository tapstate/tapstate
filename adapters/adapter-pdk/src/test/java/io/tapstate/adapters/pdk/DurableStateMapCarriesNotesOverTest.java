package io.tapstate.adapters.pdk;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Notes carried over from the namespaces that kept them before: key by key, on the first read that asks, and
 * never back again from where they came once they are forgotten here.
 */
class DurableStateMapCarriesNotesOverTest {

    private static final String HERE = "pdk.chain.c1";
    private static final String OPENER = "pdk.state.p1.src";
    private static final String OTHER = "pdk.state.p2.src";

    private final HeapKeyedState store = new HeapKeyedState();

    /** A key missing here is taken from the first earlier namespace holding it, and kept here from then on. */
    @Test
    void aMissingKeyIsCarriedFromTheFirstEarlierNamespaceHoldingIt() {
        earlier(OTHER).put("slot", "other-slot");
        earlier(OPENER).put("id", "opener-id");
        earlier(OTHER).put("id", "other-id");

        DurableStateMap notes = carrying();

        assertThat(notes.get("id")).as("asked in order, the opener first").isEqualTo("opener-id");
        assertThat(notes.get("slot")).isEqualTo("other-slot");
        store.dropNamespace(OPENER);
        store.dropNamespace(OTHER);
        assertThat(notes.get("slot")).as("kept here once carried").isEqualTo("other-slot");
        assertThat(notes.get("none")).isNull();
    }

    /** What is already here answers first, and a claim on a key answers with what can be carried to it. */
    @Test
    void whatIsAlreadyHereAnswersBeforeAnythingIsCarried() {
        earlier(OPENER).put("slot", "old-slot");
        new DurableStateMap(store, HERE).put("slot", "chain-slot");
        earlier(OPENER).put("id", "old-id");

        DurableStateMap notes = carrying();

        assertThat(notes.get("slot")).isEqualTo("chain-slot");
        assertThat(notes.putIfAbsent("id", "new-id")).as("the carried value holds the key").isEqualTo("old-id");
        assertThat(notes.get("id")).isEqualTo("old-id");
    }

    /**
     * A key removed here, or stored as nothing, is gone from where it was carried from as well -- otherwise the
     * next read would carry it back and hand the connector a resource it had already let go of.
     */
    @Test
    void aKeyForgottenHereDoesNotComeBackFromWhereItWasCarriedFrom() {
        earlier(OPENER).put("slot", "old-slot");
        earlier(OTHER).put("id", "old-id");

        DurableStateMap notes = carrying();

        assertThat(notes.remove("slot")).isEqualTo("old-slot");
        notes.put("id", null);

        assertThat(notes.get("slot")).isNull();
        assertThat(notes.get("id")).isNull();
    }

    /** Clearing the notes clears what they carry from, for the same reason. */
    @Test
    void clearingTheNotesClearsWhatTheyCarryFrom() {
        earlier(OPENER).put("slot", "old-slot");

        DurableStateMap notes = carrying();
        notes.put("id", "chain-id");
        notes.clear();

        assertThat(notes.get("slot")).isNull();
        assertThat(notes.get("id")).isNull();
    }

    /**
     * Forgetting a key here leaves the namespaces it could be carried from as they are: they are other pipelines'
     * own notes, which those pipelines may still read. What keeps the key from being carried back is a mark kept
     * here, and the key can be written here again.
     */
    @Test
    void forgettingAKeyHereLeavesTheNamespacesItCouldBeCarriedFromAlone() {
        earlier(OPENER).put("slot", "old-slot");
        earlier(OTHER).put("id", "old-id");
        DurableStateMap notes = carrying();

        notes.remove("slot");
        notes.put("id", null);

        assertThat(earlier(OPENER).get("slot")).as("the opening pipeline's own note").isEqualTo("old-slot");
        assertThat(earlier(OTHER).get("id")).as("another pipeline's own note").isEqualTo("old-id");
        assertThat(notes.get("slot")).as("not carried back here").isNull();
        assertThat(carrying().get("id")).as("nor once the notes are opened again").isNull();
        assertThat(notes.putIfAbsent("slot", "new-slot")).as("a key forgotten here is free to claim").isNull();
        assertThat(notes.get("slot")).isEqualTo("new-slot");
    }

    /**
     * A key forgotten here is claimed once, however many claim it at the same moment. Two connectors open these
     * notes -- the chain's stream and a load taking its seam on it -- and both find the key free: let both take
     * it, and each mints what it names, and the one whose claim is written over names a resource on the source
     * nobody comes back for. Here the second claim lands between the first one's look and its write.
     */
    @Test
    void aForgottenKeyIsClaimedOnceWhenTwoClaimItAtTheSameMoment() {
        earlier(OPENER).put("slot", "old-slot");
        carrying().remove("slot");
        java.util.concurrent.atomic.AtomicReference<Runnable> between = new java.util.concurrent.atomic.AtomicReference<>();
        io.tapstate.spi.store.KeyedStateStore racing = interleaving(HERE, "slot", between);
        DurableStateMap first = new DurableStateMap(racing, HERE, List.of(OPENER, OTHER));
        DurableStateMap second = new DurableStateMap(racing, HERE, List.of(OPENER, OTHER));
        java.util.concurrent.atomic.AtomicReference<Object> secondGot = new java.util.concurrent.atomic.AtomicReference<>();
        between.set(() -> secondGot.set(second.putIfAbsent("slot", "second-slot")));

        Object firstGot = first.putIfAbsent("slot", "first-slot");

        assertThat(secondGot.get()).as("the claim that landed in between takes the key").isNull();
        assertThat(firstGot).as("and the other claimer is told what holds it").isEqualTo("second-slot");
        assertThat(carrying().get("slot")).isEqualTo("second-slot");
    }

    /**
     * A key forgotten here while another opening of these notes is carrying it over stays forgotten: the carrier
     * read it where it was kept before, and the forgetting landed before its write here. Brought back, it would
     * hand a connector a resource it had already let go of.
     */
    @Test
    void aKeyForgottenWhileItIsBeingCarriedOverStaysForgotten() {
        earlier(OPENER).put("slot", "old-slot");
        java.util.concurrent.atomic.AtomicReference<Runnable> between = new java.util.concurrent.atomic.AtomicReference<>();
        io.tapstate.spi.store.KeyedStateStore racing = interleaving(OPENER, "slot", between);
        DurableStateMap carrier = new DurableStateMap(racing, HERE, List.of(OPENER, OTHER));
        DurableStateMap forgetter = new DurableStateMap(racing, HERE, List.of(OPENER, OTHER));
        between.set(() -> forgetter.remove("slot"));

        assertThat(carrier.get("slot")).isNull();
        assertThat(carrying().get("slot")).as("nor once the notes are opened again").isNull();
        assertThat(earlier(OPENER).get("slot")).as("the opening pipeline's own note").isEqualTo("old-slot");
    }

    /**
     * A key forgotten here is not looked for where it was carried from again. Carried over and only then found
     * forgotten, it would stand here for a moment, and a read by another opening of these notes in that moment
     * would hand its connector the resource it had let go of.
     */
    @Test
    void aKeyForgottenHereIsNotLookedForWhereItWasCarriedFrom() {
        earlier(OPENER).put("slot", "old-slot");
        carrying().remove("slot");
        java.util.concurrent.atomic.AtomicReference<Runnable> between = new java.util.concurrent.atomic.AtomicReference<>();
        java.util.concurrent.atomic.AtomicBoolean lookedBack = new java.util.concurrent.atomic.AtomicBoolean();
        between.set(() -> lookedBack.set(true));

        Object read = new DurableStateMap(interleaving(OPENER, "slot", between), HERE, List.of(OPENER, OTHER))
                .get("slot");

        assertThat(read).isNull();
        assertThat(lookedBack).as("looked for where it was carried from").isFalse();
    }

    /**
     * {@link #store}, running what {@code between} holds -- once -- just after {@code key} is read from
     * {@code namespace}, as another connector acting on the same notes would.
     */
    private io.tapstate.spi.store.KeyedStateStore interleaving(
            String namespace, String key, java.util.concurrent.atomic.AtomicReference<Runnable> between) {
        return new io.tapstate.spi.store.KeyedStateStore() {
            @Override
            public java.util.Optional<byte[]> load(String readFrom, String read) {
                java.util.Optional<byte[]> found = store.load(readFrom, read);
                if (namespace.equals(readFrom) && key.equals(read)) {
                    Runnable meanwhile = between.getAndSet(null);
                    if (meanwhile != null) {
                        meanwhile.run();
                    }
                }
                return found;
            }

            @Override
            public void save(String in, String saved, byte[] state) {
                store.save(in, saved, state);
            }

            @Override
            public java.util.Optional<byte[]> saveIfAbsent(String in, String saved, byte[] state) {
                return store.saveIfAbsent(in, saved, state);
            }

            @Override
            public void delete(String in, String deleted) {
                store.delete(in, deleted);
            }

            @Override
            public void dropNamespace(String dropped) {
                store.dropNamespace(dropped);
            }

            @Override
            public long count(String counted) {
                return store.count(counted);
            }
        };
    }

    /** Clearing the notes here leaves other pipelines' notes, and nothing is carried back afterwards. */
    @Test
    void clearingTheNotesHereLeavesTheNamespacesTheyCarriedFromAlone() {
        earlier(OPENER).put("slot", "old-slot");
        earlier(OTHER).put("unrelated", "theirs");
        DurableStateMap notes = carrying();
        notes.put("id", "chain-id");

        notes.clear();

        assertThat(earlier(OTHER).get("unrelated")).as("a note another pipeline kept for itself").isEqualTo("theirs");
        assertThat(earlier(OPENER).get("slot")).isEqualTo("old-slot");
        assertThat(notes.get("slot")).as("nothing is carried back after a clear").isNull();
        assertThat(carrying().get("slot")).as("nor once the notes are opened again").isNull();
        assertThat(notes.get("id")).isNull();
        notes.put("id", "again");
        assertThat(notes.get("id")).isEqualTo("again");
    }

    private DurableStateMap carrying() {
        return new DurableStateMap(store, HERE, List.of(OPENER, OTHER));
    }

    private DurableStateMap earlier(String namespace) {
        return new DurableStateMap(store, namespace);
    }
}
