package io.tapstate.e2e;

import io.tapstate.core.lifecycle.PipelineState;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;


/**
 * Runs a specification against one tier binding.
 *
 * <p>Waiting is condition-driven and bounded: {@code await} polls a matcher until it holds or the
 * bound expires, and an expired bound reports how long it actually waited alongside what it expected
 * and what it last read. There is no fixed-duration sleep anywhere in a run - a sleep long enough to
 * be reliable is a sleep that wastes that long on every green run, and it is never quite reliable
 * anyway.
 */
public final class E2eExecutor {

    /**
     * Consecutive identical readings before a stalled await redelivers the last changed table. A real
     * change stream positions itself some time after it is asked for, and a change written into that
     * window is never delivered; nothing observable announces readiness, so the executor watches for
     * its opposite - a reading that has stopped moving - and re-asserts the change. Redelivery is
     * idempotent under existing keys, so reading a slow delivery as a lost one costs a duplicate the
     * target absorbs, while the reverse misreading would be a timeout.
     */
    private static final int STALLED_POLLS = 15;

    private final TierBinding binding;
    private final PipelineLoader pipelineLoader;
    private final Duration timeout;
    private final Duration pollInterval;

    /** The table the last cdc step changed, until an await confirms the change arrived. */
    private TableAlias lastChanged;

    public E2eExecutor(
            TierBinding binding, PipelineLoader pipelineLoader, Duration timeout, Duration pollInterval) {
        this.binding = binding;
        this.pipelineLoader = pipelineLoader;
        this.timeout = timeout;
        this.pollInterval = pollInterval;
    }

    public void execute(Envelope envelope) {
        lastChanged = null;
        String pipelineId = pipelineLoader.resolvePipelineId(envelope.pipeline());
        provision(envelope.setup());
        for (Seed seed : envelope.seed()) {
            binding.seed(seed.table(), seed.rows());
        }
        // Discovery trails the seed: a source model is read out of what the source holds, and the seed is
        // what puts it there.
        envelope.setup().discover().forEach(binding::discoverSchema);
        for (Step step : envelope.steps()) {
            execute(step, pipelineId);
        }
    }

    /**
     * Strict order: a resource may not be applied before the connector it names is registered. The
     * resources themselves go in one batch, because that is the closure the product resolves references
     * within.
     *
     * <p>The third bootstrap verb, {@code discover}, is deliberately not here. Its dependency is not on the
     * apply alone but on the data: a model is discovered from what the source holds, and the harness's seed
     * is what materializes the table - it drops and rewrites it, so before a seed there is nothing to
     * discover. Running discovery first reads an absent table, returns an empty model, and leaves the sink
     * with no target and no key to upsert on - quietly, because an empty model is what an empty source
     * honestly looks like. So the executor keeps the envelope's three-key ordering and runs the discovery
     * once the data it describes exists.
     */
    private void provision(Setup setup) {
        setup.connectors().forEach(binding::registerConnector);
        if (!setup.apply().isEmpty()) {
            // A specification that applies nothing gets no apply: an empty batch would be a round trip
            // that asks the product for nothing.
            binding.applyResources(setup.apply());
        }
    }

    private void execute(Step step, String pipelineId) {
        switch (step) {
            case Step.Lifecycle lifecycle -> binding.drive(pipelineId, lifecycle.verb());
            case Step.Cdc cdc -> {
                binding.cdc(cdc.table(), cdc.op(), cdc.rows());
                lastChanged = cdc.table();
            }
            case Step.Assertion assertion -> check(assertion.matcher(), pipelineId);
            case Step.Await await -> {
                await(await.matcher(), pipelineId);
                // The await held, so the change it was waiting on arrived; a later stall is not this
                // change's loss, and redelivering it there would be noise against a different problem.
                lastChanged = null;
            }
        }
    }

    private void check(Matcher matcher, String pipelineId) {
        mismatch(matcher, pipelineId)
                .ifPresent(
                        mismatch -> {
                            throw new AssertionError(mismatch);
                        });
    }

    private void await(Matcher matcher, String pipelineId) {
        long start = System.nanoTime();
        long deadline = start + timeout.toNanos();
        String previousMismatch = null;
        int identicalReadings = 0;
        while (true) {
            Optional<String> mismatch = mismatch(matcher, pipelineId);
            if (mismatch.isEmpty()) {
                return;
            }
            if (System.nanoTime() - deadline >= 0) {
                throw new AssertionError(
                        "timed out after "
                                + Duration.ofNanos(System.nanoTime() - start)
                                + " (bound "
                                + timeout
                                + "); "
                                + mismatch.get());
            }
            // The mismatch text carries the reading, so identical text is a reading that has stopped
            // moving. Stalled long enough with an undelivered change on record, the change is
            // re-asserted; the counter restarts so the redelivery gets its own full window to arrive.
            identicalReadings = mismatch.get().equals(previousMismatch) ? identicalReadings + 1 : 1;
            previousMismatch = mismatch.get();
            if (identicalReadings >= STALLED_POLLS && lastChanged != null) {
                binding.redeliver(lastChanged);
                identicalReadings = 0;
            }
            sleep(pollInterval);
        }
    }

    /** The reading that falsifies the matcher, or empty when it holds. */
    private Optional<String> mismatch(Matcher matcher, String pipelineId) {
        return switch (matcher) {
            case Matcher.Count count -> countMismatch(count.expected());
            case Matcher.Doc doc -> docMismatch(doc);
            case Matcher.State state -> stateMismatch(state.expected(), pipelineId);
            case Matcher.ErrorCount errorCount -> errorCountMismatch(errorCount.expected(), pipelineId);
        };
    }

    /**
     * Holds one document to the matcher's expectations, all of them, so a failure names every path
     * that disagrees rather than the first. An absent document is its own mismatch - the ordinary
     * reading while an await sits out a crossing - and never conflated with a present document that
     * disagrees, which sends an author to a different place.
     */
    private Optional<String> docMismatch(Matcher.Doc doc) {
        Optional<Map<String, Object>> fetched = binding.fetch(doc.table(), doc.where());
        if (fetched.isEmpty()) {
            return Optional.of(doc.table() + " holds no document where " + doc.where());
        }
        Map<String, Object> document = fetched.get();
        List<String> mismatches = new ArrayList<>();
        doc.expect().forEach((path, expected) -> {
            Optional<Object> actual = valueAt(document, path);
            if (actual.isEmpty()) {
                mismatches.add(doc.table() + " has nothing at " + path);
            } else if (!scalarsAgree(expected, actual.get())) {
                mismatches.add(doc.table() + " at " + path + " expected " + expected + ", found " + actual.get());
            }
        });
        doc.size().forEach((path, expected) -> {
            Optional<Object> actual = valueAt(document, path);
            if (actual.isEmpty()) {
                mismatches.add(doc.table() + " has nothing at " + path);
            } else if (!(actual.get() instanceof List<?> list)) {
                mismatches.add(doc.table() + " at " + path + " is not a list, so it has no size: " + actual.get());
            } else if (list.size() != expected) {
                mismatches.add(doc.table() + " at " + path + " expected " + expected + " elements, found " + list.size());
            }
        });
        return mismatches.isEmpty() ? Optional.empty() : Optional.of(String.join("; ", mismatches));
    }

    /**
     * Walks a path of the shape {@code a.b} and {@code items[0].sku} into nested mappings and lists.
     * Empty when anything along the way is absent, out of range, or not the shape the next segment
     * needs - all of which read as "nothing there", which is what a polling await must see while a
     * document is still being assembled.
     */
    private static Optional<Object> valueAt(Map<String, Object> document, String path) {
        Object current = document;
        for (String segment : path.split("\\.")) {
            int bracket = segment.indexOf('[');
            String field = bracket < 0 ? segment : segment.substring(0, bracket);
            if (!(current instanceof Map<?, ?> mapping) || !mapping.containsKey(field)) {
                return Optional.empty();
            }
            current = mapping.get(field);
            while (bracket >= 0) {
                int close = segment.indexOf(']', bracket);
                int index = Integer.parseInt(segment.substring(bracket + 1, close));
                if (!(current instanceof List<?> list) || index >= list.size()) {
                    return Optional.empty();
                }
                current = list.get(index);
                bracket = segment.indexOf('[', close);
            }
        }
        return Optional.ofNullable(current);
    }

    /**
     * Whole numbers agree across their representations - a store answering Int32 for a value written
     * as a long is the same value - and everything else agrees the ordinary way.
     */
    private static boolean scalarsAgree(Object expected, Object actual) {
        if (expected instanceof Number left && actual instanceof Number right) {
            return left.longValue() == right.longValue();
        }
        return expected.equals(actual);
    }

    private Optional<String> countMismatch(Map<TableAlias, Long> expected) {
        List<String> mismatches = new ArrayList<>();
        expected.forEach(
                (table, rows) -> {
                    long actual = binding.count(table);
                    if (actual != rows) {
                        mismatches.add(table + " expected " + rows + ", found " + actual);
                    }
                });
        return mismatches.isEmpty() ? Optional.empty() : Optional.of(String.join("; ", mismatches));
    }

    /**
     * A pipeline that has published no observation yet reads as a mismatch, never as a failure: that is
     * the window between recording an intent and the first convergence pass, and an {@code await} exists
     * to sit through exactly it. The unpublished read is named in the mismatch rather than folded into
     * the states, because "nothing was ever published" and "the wrong state was published" fail for
     * different reasons and send an author looking in different places.
     */
    private Optional<String> stateMismatch(PipelineState expected, String pipelineId) {
        Optional<PipelineState> actual = binding.state(pipelineId);
        if (actual.filter(published -> published == expected).isPresent()) {
            return Optional.empty();
        }
        return Optional.of(
                pipelineId
                        + " expected "
                        + expected
                        + ", found "
                        + actual.map(Object::toString).orElse("no published observation"));
    }

    /**
     * Reads the same unobserved window as {@link #stateMismatch} the same way: a pipeline that has published
     * no observation reads as a mismatch, never as a failure, so an {@code await} sits through the window
     * between a start intent and the first convergence pass. "nothing published" is named apart from a wrong
     * count because the two fail for different reasons.
     */
    private Optional<String> errorCountMismatch(long expected, String pipelineId) {
        Optional<Long> actual = binding.errorCount(pipelineId);
        if (actual.filter(published -> published == expected).isPresent()) {
            return Optional.empty();
        }
        return Optional.of(
                pipelineId
                        + " expected error count "
                        + expected
                        + ", found "
                        + actual.map(Object::toString).orElse("no published observation"));
    }

    private static void sleep(Duration interval) {
        try {
            Thread.sleep(interval.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for a condition", e);
        }
    }
}
