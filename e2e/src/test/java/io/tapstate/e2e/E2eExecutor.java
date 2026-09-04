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

    /**
     * The streams this run is holding, so it can let them go when the steps run out.
     *
     * <p>A specification that holds a stream and never releases it is not an error - the interesting
     * assertions often sit while a stream is held, and requiring a release afterwards would be
     * ceremony that changes nothing about what was proven. So the run releases what it held, rather
     * than leaving that to a teardown some binding may not have: whoever held it is the one party
     * that certainly knows it was held.
     */
    private final java.util.Set<String> heldStreams = new java.util.LinkedHashSet<>();

    public E2eExecutor(
            TierBinding binding, PipelineLoader pipelineLoader, Duration timeout, Duration pollInterval) {
        this.binding = binding;
        this.pipelineLoader = pipelineLoader;
        this.timeout = timeout;
        this.pollInterval = pollInterval;
    }

    public void execute(Envelope envelope) {
        lastChanged = null;
        heldStreams.clear();
        String pipelineId = pipelineLoader.resolvePipelineId(envelope.pipeline());
        provision(envelope.setup());
        for (Seed seed : envelope.seed()) {
            binding.seed(seed.table(), seed.rows());
        }
        // Discovery trails the seed: a source model is read out of what the source holds, and the seed is
        // what puts it there.
        envelope.setup().discover().forEach(binding::discoverSchema);
        applyResources(envelope.setup(), envelope.pipeline());
        try {
            for (Step step : envelope.steps()) {
                execute(step, pipelineId);
            }
        } finally {
            releaseHeldStreams();
        }
    }

    /**
     * Lets go of every stream still held, including after a failing step - a run that fails while
     * holding must not leave the hold behind it, or the next thing to touch that store waits on a gate
     * nobody remembers closing.
     */
    private void releaseHeldStreams() {
        // Every one of them, even after one refuses. Stopping at the first failure would leave the rest
        // gated, and a gate outliving the run that made it is the failure this method exists to prevent
        // - so the refusals are collected and raised once the last stream has had its turn.
        RuntimeException firstRefusal = null;
        for (String sourceId : heldStreams) {
            try {
                binding.driveStream(sourceId, StreamVerb.RESUME);
            } catch (RuntimeException refused) {
                if (firstRefusal == null) {
                    firstRefusal = refused;
                } else {
                    firstRefusal.addSuppressed(refused);
                }
            }
        }
        heldStreams.clear();
        if (firstRefusal != null) {
            throw firstRefusal;
        }
    }

    /**
     * Strict order: register, read, seed, discover, apply. Each step is where it is because the one
     * before it is what makes it answerable.
     *
     * <p>A resource may not be applied before the connector it names is registered. The resources
     * themselves go in one batch, because that is the closure the product resolves references within.
     *
     * <p>Discovery sits between the seed and the apply, pinned from both sides. It cannot precede the
     * seed: a model is discovered from what the source holds, and the harness's seed is what
     * materializes the table - it drops and rewrites it, so before a seed there is nothing to discover.
     * Discovering an absent table returns an empty model and leaves the sink with no target and no key
     * to upsert on, quietly, because an empty model is what an empty source honestly looks like. And it
     * cannot follow the apply: a pipeline whose expression reads a row field is refused unless the
     * sources feeding it were discovered first, so an apply that ran before the discovery would be
     * refused rather than applied.
     *
     * <p>Which is why reading the resources is its own step. The seed dials the source's own address and
     * the discovery names its connector and settings - both stated only in the resource files, and both
     * needed before the product has been told anything at all.
     */
    private void provision(Setup setup) {
        setup.connectors().forEach(binding::registerConnector);
        if (!setup.apply().isEmpty()) {
            // Read, do not apply. What the resources declare is needed before the product is told
            // anything: the seed dials the source's own address, and a discovery is asked for with the
            // connector and settings the source states.
            binding.readResources(setup.apply());
        }
    }

    /**
     * The batch is the setup's resources plus the pipeline the envelope names. The pipeline is not
     * optional to apply: every specification declares one, and every specification drives it - so a run
     * that applied only what {@code setup.apply} listed would leave the steps addressing a pipeline the
     * product was never told about, and the specification would fail at its first verb over a resource
     * that reads perfectly correct.
     *
     * <p>Listing it under {@code setup.apply} anyway is allowed and is what the checked-in examples do,
     * so it is added only when absent: the batch is a closure the product resolves ids within, and one
     * id submitted twice is not a closure. Which spelling an author picks cannot change what is sent.
     *
     * <p>One batch, not two. The pipeline names its source and target by id, and the product resolves
     * references within the set submitted together - so applying it after its endpoints, in a round trip
     * of its own, is a pipeline referencing ids that are not in its own batch.
     *
     * <p>The pipeline is applied but never read in {@link #provision}: the read is there to learn an
     * endpoint's own address and settings before the product has been told anything, and a pipeline
     * states neither.
     */
    private void applyResources(Setup setup, String pipeline) {
        List<String> resources = new ArrayList<>(setup.apply());
        if (!resources.contains(pipeline)) {
            resources.add(pipeline);
        }
        binding.applyResources(resources);
    }

    private void execute(Step step, String pipelineId) {
        switch (step) {
            case Step.Lifecycle lifecycle -> binding.drive(pipelineId, lifecycle.verb());
            case Step.StreamLifecycle stream -> {
                binding.driveStream(stream.sourceId(), stream.verb());
                switch (stream.verb()) {
                    case PAUSE -> heldStreams.add(stream.sourceId());
                    case RESUME -> heldStreams.remove(stream.sourceId());
                }
            }
            case Step.Composed composed -> binding.restart(pipelineId, composed.verb().rereadsEverything());
            case Step.Cdc cdc -> {
                switch (cdc.change()) {
                    case Step.Change.Generated generated ->
                            binding.cdc(cdc.table(), generated.op(), generated.rows());
                    case Step.Change.Update update ->
                            binding.update(cdc.table(), update.where(), update.set());
                    case Step.Change.Delete delete -> binding.delete(cdc.table(), delete.where());
                    case Step.Change.Insert insert -> binding.insert(cdc.table(), insert.values());
                }
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
        // Redelivery answers one suspicion only - that a change never crossed - so it belongs to a wait
        // that reads what crossed. A wait on a lifecycle state or an error count reads the product's own
        // observation, whose reading holds still for the ordinary reason that the product has not
        // converged yet; rewriting the source there would not move it, and would mutate the very fixture
        // a specification about failure is asserting on.
        boolean readsDeliveredData = matcher instanceof Matcher.Count || matcher instanceof Matcher.Doc;
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
            if (readsDeliveredData && identicalReadings >= STALLED_POLLS && lastChanged != null) {
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
            case Matcher.FailureCode failureCode -> failureCodeMismatch(failureCode.expected(), pipelineId);
            case Matcher.DeadLettered discarded -> deadLetteredMismatch(discarded.expected(), pipelineId);
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
                // The parser holds every path an author writes to this shape, so reaching here with a
                // malformed one is the harness disagreeing with itself; it is named rather than left to
                // surface as a substring or number fault with no path in it.
                if (close < 0) {
                    throw new EnvelopeException("the path " + path + " leaves an index unclosed");
                }
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

    /**
     * Reads the same unobserved window the same way as the matchers above. The reading itself differs from
     * theirs in one respect worth knowing: a pipeline that discarded nothing publishes no such metric at all,
     * so an observed zero and an observed nothing are the same answer here - which is why asserting zero is
     * a real assertion and not a tautology, since a pipeline that discarded rows publishes a number instead.
     */
    private Optional<String> deadLetteredMismatch(long expected, String pipelineId) {
        Optional<Long> actual = binding.deadLettered(pipelineId);
        if (actual.filter(published -> published == expected).isPresent()) {
            return Optional.empty();
        }
        return Optional.of(
                pipelineId
                        + " expected "
                        + expected
                        + " changes that could not be placed in a document, found "
                        + actual.map(Object::toString).orElse("no published observation"));
    }

    /**
     * Reads the same unobserved window as the two matchers above the same way. "no published failure" is
     * named apart from the wrong code because they fail for different reasons: the pipeline is healthy or
     * was never observed, versus it died of something else than the specification expects.
     */
    private Optional<String> failureCodeMismatch(String expected, String pipelineId) {
        Optional<String> actual = binding.failureCode(pipelineId);
        if (actual.filter(expected::equals).isPresent()) {
            return Optional.empty();
        }
        return Optional.of(
                pipelineId
                        + " expected failure code "
                        + expected
                        + ", found "
                        + actual.orElse("no published failure"));
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
