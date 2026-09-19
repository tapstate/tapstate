package io.tapstate.cli;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * What a {@code logs --follow} has already shown, so that attaching again carries the tail on rather
 * than starting it over.
 *
 * <p>A follow is a re-tail. The server keeps the "what have I sent this follower" bookkeeping inside the
 * connection, so a fresh attach opens by sending the whole window it holds: the same lines again after a
 * drop, and a different member's window after a move. Remembering what was printed is what turns both of
 * those into carrying on from where the stream stopped.
 *
 * <p>The overlap, and not a timestamp: lines share a millisecond routinely, so a watermark would have to
 * drop real lines in order not to repeat one. The longest suffix of what was printed that is also a
 * prefix of what arrived is the part already seen, and everything after it is new. With no overlap at
 * all -- a different member's window, or a burst that evicted everything -- all of it is new, which is
 * the honest answer over a bounded window: a follower may see a line twice, never lose one.
 *
 * <p>Only an attach's opening frame is measured this way. Every frame after it is a delta the server
 * computed against what it had already sent, so measuring those too would swallow a line that genuinely
 * repeats -- and a repeated line is the one a person following logs is most likely to be counting.
 *
 * <p>Synchronized because the frames arrive on the transport's thread while the loop that opens each
 * attach runs on the one the user is typing on.
 */
final class PrintedLogTail {

    /**
     * How many lines to remember. One node-local window is enough to recognise a window re-sent whole;
     * remembering fewer reprints lines rather than losing them, so this is a comfort bound and not a
     * contract with the server.
     */
    private static final int REMEMBERED = 200;

    private final Deque<RemoteLogLine> printed = new ArrayDeque<>();
    private boolean openingFrame = true;

    /** Says that a new attach is starting, so its opening window is measured against what was printed. */
    synchronized void attaching() {
        openingFrame = true;
    }

    /** The part of {@code arrived} nobody has been shown yet, oldest first; remembers what it returns. */
    synchronized List<RemoteLogLine> notYetPrinted(List<RemoteLogLine> arrived) {
        List<RemoteLogLine> fresh = openingFrame
                ? arrived.subList(overlap(new ArrayList<>(printed), arrived), arrived.size())
                : arrived;
        openingFrame = false;
        for (RemoteLogLine line : fresh) {
            printed.addLast(line);
            if (printed.size() > REMEMBERED) {
                printed.removeFirst();
            }
        }
        return List.copyOf(fresh);
    }

    /** The length of the longest suffix of {@code shown} that is a prefix of {@code arrived}. */
    private static int overlap(List<RemoteLogLine> shown, List<RemoteLogLine> arrived) {
        int longest = Math.min(shown.size(), arrived.size());
        for (int length = longest; length > 0; length--) {
            if (shown.subList(shown.size() - length, shown.size()).equals(arrived.subList(0, length))) {
                return length;
            }
        }
        return 0;
    }
}
