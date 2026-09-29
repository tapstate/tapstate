package io.tapstate.runtime.engine.nest;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Each default limit is a count and what it really bounds is bytes. This holds them to that
 * multiplication: at the cost one unit was measured to have, what a default allows must still leave a
 * member able to report it.
 *
 * <p><b>A limit reached only after the member is dead is not a limit.</b> Each of these exists to fail a
 * run that has grown past what it can carry, and failing takes a member that is still running - one that
 * can build an error, render its parameters and get it out. Set past the point where the heap is gone, it
 * never fires: what happens instead is an exhausted member, which reports the wrong thing, reports it from
 * the wrong place, and takes the rest of the pipeline down with it.
 *
 * <p><b>These are ceilings, not the values themselves.</b> Asserting each default equals what it is today
 * would pass by construction and catch nothing - the constant and its copy would be edited together. What
 * is asserted is the relation the defaults have to survive, so a later change that raises one back past
 * what a member can hold fails here whatever number it picks.
 *
 * <p>The per-unit costs below are measurements, not guesses, and {@code NestStateSizeBench} is what took
 * them. They are the widest row it measured: a narrower row costs less and a limit safe at the widest is
 * safe at all of them. {@link #anElementStillCostsWhatTheseLimitsWereSetAgainst()} is what keeps them from
 * going quietly stale.
 */
class TheDefaultLimitsAreReachableBeforeAMemberDiesTest {

    private static final long KIB = 1024L;
    private static final long MIB = 1024L * KIB;

    /**
     * The member these are sized against. It used to be written here, as an assumption this file made on
     * its own; it is now a requirement the product states, so this reads it from where the product keeps
     * it rather than keeping a second copy that could drift from the published one.
     */
    private static final long REFERENCE_HEAP = NestSettings.REFERENCE_MEMBER_HEAP_BYTES;

    /**
     * What one entry may take on its own before it is refused. A sixteenth of the heap for a single
     * document or a single key is already far past anything healthy, and it still leaves the member
     * fifteen sixteenths with which to say so.
     */
    private static final long MOST_ONE_ENTRY_MAY_TAKE = REFERENCE_HEAP / 16;

    /**
     * What one namespace's resident entries may take. A quarter of the heap, because this is steady state
     * rather than a pathology - it is what the process holds while nothing at all is wrong, and the other
     * three quarters are what everything else in the process runs in.
     */
    private static final long MOST_ONE_NAMESPACE_MAY_HOLD = REFERENCE_HEAP / 4;

    // Measured, at the widest row measured. See the bench named above.
    private static final long ELEMENT_HEAP_BYTES = 7_164L;
    private static final long HELD_CHANGE_HEAP_BYTES = 7_585L;
    private static final long DELETION_RECORD_HEAP_BYTES = 620L;

    /**
     * One whole document that has absorbed a hundred elements, which is the entry size the memory budget
     * is sized against. The budget counts entries and cannot see inside one, so what it is worth depends
     * entirely on how big the entries are, and this is the assumed shape.
     */
    private static final long DOCUMENT_HEAP_BYTES = 243_137L;

    @Test
    void aDocumentFullOfElementsStillLeavesAMemberToRefuseIt() {
        long atTheLimit = NestSettings.DEFAULT_ELEMENT_LIMIT * ELEMENT_HEAP_BYTES;

        assertThat(atTheLimit)
                .as("a document at the element limit takes %d MiB, and a member with %d MiB of heap has to "
                        + "still be running to refuse it - a document is assembled whole and rendered into a "
                        + "second copy of itself, so this is paid twice at the moment it is refused",
                        atTheLimit / MIB, REFERENCE_HEAP / MIB)
                .isLessThanOrEqualTo(MOST_ONE_ENTRY_MAY_TAKE);
    }

    @Test
    void aKeyFullOfChangesWaitingStillLeavesAMemberToRefuseIt() {
        long atTheLimit = NestSettings.DEFAULT_PENDING_LIMIT * HELD_CHANGE_HEAP_BYTES;

        assertThat(atTheLimit)
                .as("one key holding the pending limit takes %d MiB. This is the only backstop left for a "
                        + "foreign key that points at a row which never arrives, since such a key is now held "
                        + "for as long as the job runs rather than timed out", atTheLimit / MIB)
                .isLessThanOrEqualTo(MOST_ONE_ENTRY_MAY_TAKE);
    }

    @Test
    void aDocumentFullOfRecordsOfDeletionStillLeavesAMemberToRefuseIt() {
        long atTheLimit = NestSettings.DEFAULT_TOMBSTONE_LIMIT * DELETION_RECORD_HEAP_BYTES;

        assertThat(atTheLimit)
                .as("a document at the limit on records of deletion takes %d MiB. A record keeps no row, so "
                        + "this one does not follow how wide the rows are", atTheLimit / MIB)
                .isLessThanOrEqualTo(MOST_ONE_ENTRY_MAY_TAKE);
    }

    @Test
    void aNamespaceAtItsBudgetStillFitsInAMember() {
        long resident = NestSettings.DEFAULT_ENTRIES_HELD_IN_MEMORY * DOCUMENT_HEAP_BYTES;

        assertThat(resident)
                .as("a namespace holding its default budget of documents keeps %d MiB resident, on a member "
                        + "with %d MiB of heap - and this is per namespace, so a cascade pays it once per "
                        + "level, while the process still has a pipeline to run in what is left",
                        resident / MIB, REFERENCE_HEAP / MIB)
                .isLessThanOrEqualTo(MOST_ONE_NAMESPACE_MAY_HOLD);
    }

    /**
     * The costs above were measured once, and the limits were chosen from them. Should an element get much
     * more expensive the limits would go on saying what they say while meaning something else, with nothing
     * anywhere to notice.
     *
     * <p>Serialized length rather than heap: it is exact and the same on every machine, where a heap
     * reading is neither. It is not the quantity the limits are about, but it moves with it - what makes an
     * element cost more in memory is a field added to it, and that shows up here.
     *
     * <p>What this catches is a change of shape, not a change of a few bytes. A band rather than a number,
     * because the point is to notice a new collection hanging off an element, not to re-pin the file every
     * time a comment moves.
     *
     * <p><b>Marginal, for the same reason the costs above are.</b> Writing one element out on its own
     * measures close to a kilobyte of class descriptors along with it - the names of every type it reaches,
     * written once however many elements follow. That is a real cost of the first element and of no other,
     * so counting it here would compare a number against one it was never taken the same way as.
     */
    @Test
    void anElementStillCostsWhatTheseLimitsWereSetAgainst() {
        long marginal = (serializedBytesOf(documentHolding(400)) - serializedBytesOf(documentHolding(200))) / 200;

        assertThat(marginal)
                .as("one more element of the shape the limits were calibrated against added 589 bytes when "
                        + "they were chosen; it adds %d now. A material change here means the defaults were "
                        + "sized against a cost that no longer exists and have to be taken again", marginal)
                .isBetween(500L, 680L);
    }

    /**
     * A document holding {@code elements} elements and nothing else, built the way the bench built the one
     * the costs were measured on - same row width, same value length, and what was absorbed on the way in
     * released before it is weighed.
     */
    private static RootAssembly documentHolding(int elements) {
        RootAssembly assembly = new RootAssembly();
        assembly.applyRoot(row(0), order(0), positions(0));
        for (int i = 0; i < elements; i++) {
            assembly.applyElement(
                    new ElementRef(List.of("items"), null, List.of("k-" + i), "id-" + i),
                    row(i), order(i + 1), positions(i + 1));
        }
        assembly.documentSent();
        return assembly;
    }

    /** Twelve fields of twenty-four characters each, which is the row every cost above was taken against. */
    private static Map<String, Object> row(int i) {
        Map<String, Object> fields = new LinkedHashMap<>();
        for (int f = 0; f < 12; f++) {
            StringBuilder value = new StringBuilder(24);
            value.append(i).append(':').append(f).append(':');
            while (value.length() < 24) {
                value.append('x');
            }
            fields.put("field_" + f, value.substring(0, 24));
        }
        return fields;
    }

    private static SourceOrder order(long seq) {
        return new SourceOrder(1L, seq);
    }

    private static Map<String, ChainPosition> positions(long seq) {
        return Map.of("chain-0", new ChainPosition(order(seq), "token-" + seq));
    }

    private static long serializedBytesOf(Object value) {
        Counting counter = new Counting();
        try (ObjectOutputStream out = new ObjectOutputStream(counter)) {
            out.writeObject(value);
        } catch (IOException failed) {
            throw new UncheckedIOException(failed);
        }
        return counter.written;
    }

    private static final class Counting extends OutputStream {

        private long written;

        @Override
        public void write(int b) {
            written++;
        }

        @Override
        public void write(byte[] bytes, int off, int len) {
            written += len;
        }
    }
}
