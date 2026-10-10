package io.tapstate.e2e;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Exact bounded diagnostic decoding; a receipt never establishes formal timing qualification. */
final class BenchmarkWriteReturnLedger {
    private static final int MAGIC = 0x57525031;
    private static final int VERSION = 3;
    private static final int MAX_PAGE_BYTES = 64 * 1024;
    private static final int MAX_CALL_BYTES = MAX_PAGE_BYTES - 2048 - Integer.BYTES;
    private static final int MAX_FRAMES = 512;
    private static final int MAX_BATCH_ROWS = 512;
    private static final int MAX_NATIVE_ROWS = 1024;
    private static final int MAX_RECORDS = MAX_FRAMES * MAX_BATCH_ROWS;
    private static final int MAX_WRITERS = 128;

    private BenchmarkWriteReturnLedger() { }

    record Page(long epoch, int cursor, int nextCursor, int totalFrames, String window, String state,
                List<Call> calls) {
        Page { calls = List.copyOf(calls); }
    }

    /** One ordered identity piece; full-call metadata and counters repeat across pieces. */
    record Call(long sequence, int writer, int totalRows, int partIndex, int partCount,
                long beganNanos, long lastCallbackExitNanos, long observedNanos, boolean returnedNormally,
                int callbackCount, long inserted, long modified, long removed, int errors,
                List<ErrorDetail> errorDetails, String failureType, String scope, String writerIdentity,
                String stream, String target, List<String> keyFields, List<Row> rows) {
        Call {
            errorDetails = List.copyOf(errorDetails);
            keyFields = List.copyOf(keyFields);
            rows = List.copyOf(rows);
        }
    }

    record ErrorDetail(int rowOrdinal, String failureType) { }
    record Row(int kind, List<Integer> keys) {
        Row { keys = List.copyOf(keys); }
    }

    static Page decode(byte[] bytes) {
        require(bytes != null && bytes.length >= 32 && bytes.length <= MAX_PAGE_BYTES,
                "page is missing or exceeds its byte bound");
        try {
            var input = new DataInputStream(new ByteArrayInputStream(bytes));
            require(input.readInt() == MAGIC, "page magic differs");
            require(input.readInt() == VERSION, "page version differs");
            long epoch = input.readLong();
            int cursor = input.readInt(), next = input.readInt(), total = input.readInt();
            require(epoch >= 0 && cursor >= 0 && cursor <= next && next <= total && total <= MAX_FRAMES,
                    "page epoch or completion cursor is invalid");
            require(cursor == total || next > cursor, "page made no completion cursor progress");
            String window = text(input, 512, "window");
            String state = text(input, 128, "state");
            require(state.equals("IDLE") || state.equals("RECORDED_SCOPE_UNQUALIFIED")
                    || state.matches("UNKNOWN:[A-Z0-9_]+"), "page state is unsupported");
            if (epoch == 0) {
                require(total == 0 && window.isEmpty() && !state.equals("RECORDED_SCOPE_UNQUALIFIED"),
                        "an initial page has capture data");
            } else {
                require(!window.isBlank() && !state.equals("IDLE"), "capture page has no window");
            }
            var calls = new ArrayList<Call>(next - cursor);
            var pieces = new HashSet<Piece>();
            var metadata = new HashMap<Long, Call>();
            Call previous = null;
            int records = 0;
            long reported = 0;
            for (int i = cursor; i < next; i++) {
                int length = input.readInt();
                require(length > 0 && length <= MAX_CALL_BYTES && length <= input.available(),
                        "call length is invalid or truncated");
                Call call = call(input.readNBytes(length), state);
                require(pieces.add(new Piece(call.sequence(), call.partIndex())), "page repeats a call part");
                Call first = metadata.putIfAbsent(call.sequence(), call);
                if (first != null) {
                    require(sameMetadata(first, call), "call parts have mismatched metadata");
                }
                if (previous != null && previous.sequence() == call.sequence()) {
                    require(call.partIndex() == previous.partIndex() + 1, "call parts are missing or out of order");
                } else {
                    require(first == null, "call parts are not contiguous");
                    require(previous == null || previous.partIndex() + 1 == previous.partCount(),
                            "an interior call part is missing");
                    require(call.partIndex() == 0 || previous == null && cursor > 0,
                            "the first call part is missing");
                }
                records = Math.addExact(records, call.rows().size());
                require(records <= MAX_RECORDS, "page exceeds its record bound");
                if (first == null) {
                    reported = Math.addExact(reported, sum(call.inserted(), call.modified(), call.removed()));
                }
                calls.add(call);
                previous = call;
            }
            require(previous == null || previous.partIndex() + 1 == previous.partCount() || next < total,
                    "the final call part is missing");
            require(input.available() == 0, "page has extra bytes");
            return new Page(epoch, cursor, next, total, window, state, calls);
        } catch (IOException truncated) {
            throw new AssertionError("return ledger page is truncated", truncated);
        } catch (ArithmeticException overflow) {
            throw new AssertionError("return ledger counter overflow", overflow);
        }
    }

    private static Call call(byte[] bytes, String pageState) throws IOException {
        var input = new DataInputStream(new ByteArrayInputStream(bytes));
        long sequence = input.readLong();
        int writer = input.readInt();
        int totalRows = input.readInt(), partIndex = input.readInt(), partCount = input.readInt();
        require(totalRows > 0 && totalRows <= MAX_NATIVE_ROWS && partCount == (totalRows + MAX_BATCH_ROWS - 1) / MAX_BATCH_ROWS
                && partIndex >= 0 && partIndex < partCount, "call part metadata is invalid");
        long began = input.readLong(), lastCallbackExit = input.readLong(), observed = input.readLong();
        int normal = input.readUnsignedByte();
        require(normal <= 1, "call return flag is not a boolean");
        int callbacks = input.readInt();
        long inserted = input.readLong(), modified = input.readLong(), removed = input.readLong();
        int errors = input.readInt(), details = input.readInt();
        require(sequence > 0 && writer > 0 && writer <= MAX_WRITERS, "call identity is invalid");
        require(Math.subtractExact(observed, began) >= 0, "call clock observation moved backward");
        require(callbacks >= 0 && inserted >= 0 && modified >= 0 && removed >= 0 && errors >= 0,
                "call has negative counters");
        require(details >= 0 && details <= MAX_BATCH_ROWS && details <= errors,
                "call error detail roster is invalid");
        var errorDetails = new ArrayList<ErrorDetail>(details);
        for (int i = 0; i < details; i++) {
            int ordinal = input.readInt();
            String failureType = text(input, 256, "error failure type");
            require(!failureType.isBlank(), "call error has no failure type");
            errorDetails.add(new ErrorDetail(ordinal, failureType));
        }
        String failure = text(input, 256, "failure type");
        require(normal == 1 ? failure.isEmpty() : !failure.isBlank(), "call return and failure type disagree");
        String scope = text(input, 512, "scope");
        scope(scope);
        String identity = text(input, 512, "writer identity");
        String stream = text(input, 512, "stream");
        String target = text(input, 512, "target");
        require(!identity.isBlank() && !stream.isBlank() && !target.isBlank(), "call has missing routing identity");
        int keyCount = input.readUnsignedByte();
        require(keyCount >= 1 && keyCount <= 2, "call key field count is unsupported");
        var fields = new ArrayList<String>(keyCount);
        for (int i = 0; i < keyCount; i++) {
            String field = text(input, 128, "key field");
            require(!field.isBlank() && !fields.contains(field), "call key field is missing or duplicate");
            fields.add(field);
        }
        int count = input.readInt();
        require(count > 0 && count <= MAX_BATCH_ROWS, "call row roster exceeds its bound");
        require(count == Math.min(MAX_BATCH_ROWS, totalRows - partIndex * MAX_BATCH_ROWS),
                "call part row count differs from its declared slice");
        for (ErrorDetail detail : errorDetails) {
            require(detail.rowOrdinal() >= -1 && detail.rowOrdinal() < totalRows, "call error row ordinal is invalid");
        }
        var rows = new ArrayList<Row>(count);
        for (int i = 0; i < count; i++) {
            int kind = input.readUnsignedByte();
            require(kind >= 1 && kind <= 3, "call row kind is unsupported");
            var keys = new ArrayList<Integer>(keyCount);
            for (int key = 0; key < keyCount; key++) { keys.add(input.readInt()); }
            rows.add(new Row(kind, keys));
        }
        require(input.available() == 0, "call has extra bytes");
        long reported = sum(inserted, modified, removed);
        require(callbacks != 0 || reported == 0 && errors == 0, "call counters have no callback");
        boolean incomplete = normal == 0 || errors != 0 || callbacks == 0
                || callbacks > MAX_BATCH_ROWS || reported != totalRows;
        require(!incomplete || pageState.startsWith("UNKNOWN:"), "incomplete call lacks an unknown page state");
        // Unknown producer receipts retain exact stamp values without supplying an ordering proof.
        // Zero and negative coordinates never indicate whether a callback exit was actually observed.
        if (!pageState.startsWith("UNKNOWN:")) {
            require(Math.subtractExact(lastCallbackExit, began) >= 0
                    && Math.subtractExact(observed, lastCallbackExit) >= 0,
                    "call callback exit clock order is invalid");
        }
        return new Call(sequence, writer, totalRows, partIndex, partCount, began, lastCallbackExit, observed, normal == 1,
                callbacks, inserted, modified, removed,
                errors, errorDetails, failure, scope, identity, stream, target, fields, rows);
    }

    private record Piece(long sequence, int partIndex) { }

    private static boolean sameMetadata(Call first, Call next) {
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

    private static long sum(long inserted, long modified, long removed) {
        return Math.addExact(Math.addExact(inserted, modified), removed);
    }

    private static void scope(String value) {
        if (value.equals("UNKNOWN")) { return; }
        require(value.chars().allMatch(character -> character >= 32 && character <= 126),
                "call scope is not bounded ASCII evidence");
        String[] fields = value.split(";", -1);
        require(fields.length == 3, "call scope field roster differs");
        var names = new HashSet<String>();
        for (String field : fields) {
            int equals = field.indexOf('=');
            require(equals > 0 && equals < field.length() - 1 && names.add(field.substring(0, equals)),
                    "call scope field is missing or duplicate");
        }
        require(names.equals(Set.of("state", "reason", "concern")), "call scope field roster differs");
    }

    private static String text(DataInputStream input, int maximum, String field) throws IOException {
        int length = input.readUnsignedShort();
        require(length <= maximum && length <= input.available(), "text length is invalid for " + field);
        byte[] bytes = input.readNBytes(length);
        try {
            return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException invalid) {
            throw new AssertionError("return ledger text is not valid UTF-8 for " + field, invalid);
        }
    }

    private static void require(boolean condition, String reason) {
        if (!condition) { throw new AssertionError("return ledger " + reason); }
    }
}
