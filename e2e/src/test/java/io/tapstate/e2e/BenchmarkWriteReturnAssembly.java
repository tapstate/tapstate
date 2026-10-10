package io.tapstate.e2e;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Single-use bounded receipt assembly. Successful closure checks ledger consistency only;
 * the caller must separately prove its actual owned runtime, source coverage and timing contract.
 */
final class BenchmarkWriteReturnAssembly {
    private static final int MAX_PAGE_BYTES = 64 * 1024;
    private static final int MAX_RAW_BYTES = 2 * 1024 * 1024;
    private static final int MAX_FRAMES = 512;
    private static final int MAX_RECORDS = MAX_FRAMES * 512;
    private static final int MAX_WRITERS = 128;
    private static final String RECORDED = "RECORDED_SCOPE_UNQUALIFIED";

    record FullCall(long sequence, int writer, int totalRows, long beganNanos, long lastCallbackExitNanos, long observedNanos,
                    boolean returnedNormally, int callbackCount, long inserted, long modified, long removed,
                    int errors, List<BenchmarkWriteReturnLedger.ErrorDetail> errorDetails, String failureType,
                    String scope, String writerIdentity, String stream, String target, List<String> keyFields,
                    List<BenchmarkWriteReturnLedger.Row> rows) {
        FullCall {
            errorDetails = List.copyOf(errorDetails);
            keyFields = List.copyOf(keyFields);
            rows = List.copyOf(rows);
        }
    }

    private record Piece(long sequence, int index) { }
    private final String window;
    private final Set<Piece> pieces = new HashSet<>();
    private final Set<Long> sequences = new HashSet<>();
    private final Map<Integer, String> writers = new HashMap<>();
    private final List<FullCall> calls = new ArrayList<>();
    private boolean seenPage;
    private boolean failed;
    private boolean finished;
    private boolean unknownState;
    private long epoch;
    private int nextCursor;
    private int totalFrames;
    private int rawBytes;
    private int retainedRows;
    private long reportedRecords;
    private BenchmarkWriteReturnLedger.Call pending;
    private List<BenchmarkWriteReturnLedger.Row> pendingRows;
    private int nextPart;

    BenchmarkWriteReturnAssembly(String expectedWindow) {
        require(expectedWindow != null && !expectedWindow.isBlank() && expectedWindow.length() <= 512
                && expectedWindow.getBytes(StandardCharsets.UTF_8).length <= 512, "expected window is invalid");
        window = expectedWindow;
    }

    void add(byte[] bytes) {
        requireOpen();
        try {
            require(bytes != null && bytes.length <= MAX_PAGE_BYTES, "page exceeds its byte bound");
            int nextRawBytes = Math.addExact(rawBytes, bytes.length);
            require(nextRawBytes <= MAX_RAW_BYTES, "raw ledger exceeds its byte bound");
            var page = BenchmarkWriteReturnLedger.decode(bytes);
            require(page.epoch() > 0 && window.equals(page.window()), "page has another capture window");
            require(page.cursor() == nextCursor, "completion cursor has a gap or replay");
            require(!seenPage || page.epoch() == epoch, "capture epoch changed");
            require(!seenPage || page.totalFrames() >= totalFrames, "terminal frame total moved backward");
            require(page.totalFrames() <= MAX_FRAMES, "frame roster exceeds its bound");
            if (!RECORDED.equals(page.state())) { unknownState = true; }
            for (var part : page.calls()) { addPart(part); }
            seenPage = true;
            epoch = page.epoch();
            nextCursor = page.nextCursor();
            totalFrames = page.totalFrames();
            rawBytes = nextRawBytes;
        } catch (AssertionError | RuntimeException invalid) {
            failed = true;
            throw refusal(invalid);
        }
    }

    List<FullCall> finish(long expectedCompleteCalls, long expectedReportedRecords) {
        requireOpen();
        try {
            require(expectedCompleteCalls >= 0 && expectedCompleteCalls <= MAX_FRAMES
                    && expectedReportedRecords >= 0 && expectedReportedRecords <= MAX_RECORDS,
                    "expected terminal counters exceed their bounds");
            require(seenPage, "no actual receipt page was supplied");
            require(nextCursor == totalFrames && pending == null, "terminal pages or call pieces are missing");
            require(!unknownState, "capture state is unknown");
            require(calls.size() == expectedCompleteCalls && reportedRecords == expectedReportedRecords,
                    "terminal call or reported-record totals disagree");
            for (FullCall call : calls) {
                require(call.returnedNormally() && call.callbackCount() > 0 && call.callbackCount() <= 512
                        && Math.subtractExact(call.lastCallbackExitNanos(), call.beganNanos()) >= 0
                        && Math.subtractExact(call.observedNanos(), call.lastCallbackExitNanos()) >= 0
                        && call.errors() == 0 && call.errorDetails().isEmpty() && call.failureType().isEmpty()
                        && count(call.inserted(), call.modified(), call.removed()) == call.totalRows(),
                        "call failed or reported partial delivery");
                requireAcknowledgedScope(call.scope());
            }
            finished = true;
            return List.copyOf(calls);
        } catch (AssertionError | RuntimeException invalid) {
            failed = true;
            throw refusal(invalid);
        }
    }

    private void addPart(BenchmarkWriteReturnLedger.Call part) {
        require(pieces.size() < MAX_FRAMES && pieces.add(new Piece(part.sequence(), part.partIndex())),
                "call piece is duplicate or exceeds its roster bound");
        if (pending == null) {
            require(part.partIndex() == 0 && sequences.add(part.sequence()), "call starts with a missing or repeated piece");
            require(part.totalRows() > 0 && part.totalRows() <= 1024, "native call exceeds its record bound");
            String identity = writers.putIfAbsent(part.writer(), part.writerIdentity());
            require(writers.size() <= MAX_WRITERS && (identity == null || identity.equals(part.writerIdentity())),
                    "writer identity changed or exceeds its roster bound");
            pending = part;
            pendingRows = new ArrayList<>(part.totalRows());
            nextPart = 0;
        }
        require(part.sequence() == pending.sequence() && part.partIndex() == nextPart,
                "call pieces have a missing, interleaved or out-of-order part");
        require(sameMetadata(pending, part), "call metadata changed across receipt pages");
        retainedRows = Math.addExact(retainedRows, part.rows().size());
        require(retainedRows <= MAX_RECORDS, "record roster exceeds its bound");
        pendingRows.addAll(part.rows());
        nextPart++;
        if (nextPart == pending.partCount()) {
            require(pendingRows.size() == pending.totalRows(), "complete call has a missing row slice");
            reportedRecords = Math.addExact(reportedRecords,
                    count(pending.inserted(), pending.modified(), pending.removed()));
            calls.add(new FullCall(pending.sequence(), pending.writer(), pending.totalRows(), pending.beganNanos(),
                    pending.lastCallbackExitNanos(), pending.observedNanos(), pending.returnedNormally(), pending.callbackCount(), pending.inserted(),
                    pending.modified(), pending.removed(), pending.errors(), pending.errorDetails(), pending.failureType(),
                    pending.scope(), pending.writerIdentity(), pending.stream(), pending.target(), pending.keyFields(), pendingRows));
            pending = null;
            pendingRows = null;
            nextPart = 0;
        }
    }

    private static boolean sameMetadata(BenchmarkWriteReturnLedger.Call first, BenchmarkWriteReturnLedger.Call next) {
        return first.writer() == next.writer() && first.totalRows() == next.totalRows()
                && first.partCount() == next.partCount() && first.beganNanos() == next.beganNanos()
                && first.lastCallbackExitNanos() == next.lastCallbackExitNanos()
                && first.observedNanos() == next.observedNanos() && first.returnedNormally() == next.returnedNormally()
                && first.callbackCount() == next.callbackCount() && first.inserted() == next.inserted()
                && first.modified() == next.modified() && first.removed() == next.removed()
                && first.errors() == next.errors() && first.errorDetails().equals(next.errorDetails())
                && first.failureType().equals(next.failureType()) && first.scope().equals(next.scope())
                && first.writerIdentity().equals(next.writerIdentity()) && first.stream().equals(next.stream())
                && first.target().equals(next.target()) && first.keyFields().equals(next.keyFields());
    }

    /** Validates the receipt's ordinary scope declaration, never its actual runtime provenance. */
    private static void requireAcknowledgedScope(String scope) {
        String prefix = "state=ORDINARY_ACKNOWLEDGED;reason=PINNED_RUNTIME_SCOPE;concern=";
        require(scope.startsWith(prefix), "call scope is unqualified");
        String[] fields = scope.substring(prefix.length()).split(",", -1);
        require(fields.length == 3 && fields[0].startsWith("w:") && fields[1].startsWith("j:")
                && fields[2].startsWith("timeoutMs:"), "call write concern declaration is malformed");
        String write = fields[0].substring(2);
        require(write.equals("DEFAULT") || write.equals("MAJORITY") || write.matches("TAG_SHA256_[0-9a-f]{64}")
                || positiveInteger(write), "call write concern is not acknowledged");
        String journal = fields[1].substring(2);
        require(journal.equals("DEFAULT") || journal.equals("true") || journal.equals("false"),
                "call journal declaration is malformed");
        String timeout = fields[2].substring("timeoutMs:".length());
        require(timeout.equals("DEFAULT") || nonnegativeInteger(timeout), "call timeout declaration is malformed");
    }

    private static boolean positiveInteger(String value) {
        return nonnegativeInteger(value) && !value.equals("0");
    }
    private static boolean nonnegativeInteger(String value) {
        if (!value.matches("0|[1-9][0-9]*")) { return false; }
        try { return Integer.parseInt(value) >= 0; }
        catch (NumberFormatException overflow) { return false; }
    }
    private static long count(long inserted, long modified, long removed) {
        return Math.addExact(Math.addExact(inserted, modified), removed);
    }
    private void requireOpen() {
        require(!finished && !failed, "capture is already closed or refused");
    }
    private static AssertionError refusal(Throwable failure) {
        if (failure instanceof AssertionError assertion) { return assertion; }
        return new AssertionError("return assembly rejected malformed counters or receipt data", failure);
    }
    private static void require(boolean condition, String reason) {
        if (!condition) { throw new AssertionError("return assembly " + reason); }
    }
}
