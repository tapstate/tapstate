package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationCheckEvent;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationObservation;
import io.tapstate.core.validation.ValidationNestArrayMembers;
import io.tapstate.core.validation.ValidationRelationRows;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/** Thread-confined conservative owned-work accounting for one original root, shared by its descendants. */
final class ValidationNestBudget {
    record Limits(long bytes, long operations, int queries, int rows, int depth) {
        Limits {
            if (bytes < 1 || bytes > 32L * 1024 * 1024 || operations < 1 || operations > 64L * 1024 * 1024
                    || queries < 1 || queries > 256 || rows < 1 || rows > 65536 || depth < 1 || depth > 32)
                throw new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", "nestBudget.limits"), null);
        }
    }
    private final ValidationObservation root;
    private final Limits limits;
    private final String prefix;
    private final Set<String> invocations = new LinkedHashSet<>();
    private final Map<String, ValidationCheckEvent.ReadBoundary> reads = new LinkedHashMap<>();
    private long bytes;
    private long operations;
    private int queryCount;
    private int rowCount;
    private Throwable failure;

    ValidationNestBudget(ValidationObservation root, String originalReadId, Limits limits, String prefix) {
        this.root = Objects.requireNonNull(root, "root"); this.limits = Objects.requireNonNull(limits, "limits");
        this.prefix = Objects.requireNonNull(prefix, "prefix");
        if (!root.root().ancestors().isEmpty() || originalReadId == null || originalReadId.isBlank() || originalReadId.length() > 1024
                || prefix.isBlank() || prefix.length() > 128) throw invalid("root");
        invocations.add(originalReadId);
    }

    void requireScope(ValidationObservation actual) {
        ensureActive(); Objects.requireNonNull(actual, "actual");
        if (!root.run().equals(actual.run()) || root.revision() != actual.revision()
                || !root.root().mappingId().equals(actual.root().mappingId()) || !root.root().outputId().equals(actual.root().outputId())
                || !root.root().rootKey().equals(actual.root().rootKey())) throw fail(invalid("scope"));
    }
    void requireParentRead(String id) {
        ensureActive();
        if (!invocations.contains(id) && !reads.containsKey(id)) throw fail(invalid("parentRead"));
    }
    ValidationRootReader.Query prepare(ValidationObservation parent, Supplier<ValidationRootReader.Query> factory) {
        requireScope(parent);
        if (queryCount == limits.queries()) throw fail(limit("queries"));
        queryCount++;
        try {
            var query = Objects.requireNonNull(factory.get(), "relation query");
            if (!invocations.add(query.readId()) || reads.containsKey(query.readId())) throw invalid("reusedReadId");
            bytes(256L + 2L * query.readId().length() + ValidationRelationRows.initialReservationBytes(parent, query.readId()));
            return query;
        } catch (RuntimeException | Error error) { remember(error); throw error; }
    }
    void row(ValidationObservation parent) {
        requireScope(parent);
        if (rowCount == limits.rows()) throw fail(limit("rows"));
        rowCount++;
    }
    void depth(int depth) {
        ensureActive();
        if (depth < 1 || depth > limits.depth()) throw fail(limit("depth"));
    }
    void bytes(long amount) {
        ensureActive();
        if (amount < 0 || amount > limits.bytes() - bytes) throw fail(limit("bytes"));
        bytes += amount;
    }
    /** Conservative linear frame/index bookkeeping work is bounded along with its retained bytes. */
    void reserve(long amount) { bytes(amount); operations(amount); }

    ValidationNestArrayMembers targetMembers(ValidationNestArrayMembers.Source source) {
        requireScope(source.observation());
        if (!source.read().equals(reads.get(source.read().readId()))) throw fail(invalid("sourceReadIncomplete"));
        try { return ValidationNestArrayMembers.forTarget(source, this::reserve, this::abort); }
        catch (RuntimeException | Error error) { abort(error); throw error; }
    }

    void operations(long amount) {
        ensureActive();
        if (amount < 0 || amount > limits.operations() - operations) throw fail(limit("operations"));
        operations += amount;
    }
    void read(ValidationCheckEvent.ReadBoundary read) {
        ensureActive();
        var previous = reads.get(read.readId());
        if (previous != null && !previous.equals(read)) throw fail(invalid("readIdentity"));
        if (previous == null) {
            if (reads.size() == 256) throw fail(limit("reads"));
            bytes(256L + 2L * (read.readId().length() + read.contractVersion().length()));
            reads.put(read.readId(), read);
        }
    }
    List<ValidationCheckEvent.ReadBoundary> reads() { ensureActive(); return new ArrayList<>(reads.values()); }
    long accountedBytes() { ensureActive(); return bytes; }

    /** A component-local failure invalidates the whole root even when its caller catches the error. */
    void abort(Throwable cause) { remember(Objects.requireNonNull(cause, "cause")); }

    private void ensureActive() {
        if (failure instanceof Error error) throw error;
        if (failure instanceof RuntimeException error) throw error;
    }
    private void remember(Throwable error) { if (failure == null || error instanceof Error && !(failure instanceof Error)) failure = error; }
    private TapstateException fail(TapstateException error) { remember(error); return error; }
    private TapstateException invalid(String field) { return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", prefix + "." + field), null); }
    private TapstateException limit(String field) { return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", prefix + "." + field), null); }
}
