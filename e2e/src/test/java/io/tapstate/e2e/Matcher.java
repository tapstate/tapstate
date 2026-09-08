package io.tapstate.e2e;

import io.tapstate.core.lifecycle.PipelineState;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A condition over observable product state. One vocabulary serves both timings: {@code assert}
 * checks a matcher once, {@code await} polls the same matcher until it holds or the bound expires.
 *
 * <p>Every word has a real source today: {@code count} reads the target database, {@code state} reads
 * the published observation, {@code error_count} reads the observation's metrics map. A word whose
 * source the runtime does not yet populate would poll an empty map until timeout, so it is not admitted
 * until something fills it.
 */
public sealed interface Matcher {

    /**
     * Row counts per table, read from the target endpoint itself. Declaration order is preserved,
     * so the endpoints are read in the order written and a failure reads the same way twice.
     */
    record Count(Map<TableAlias, Long> expected) implements Matcher {
        public Count {
            expected = Collections.unmodifiableMap(new LinkedHashMap<>(expected));
        }
    }

    /**
     * The lifecycle state of the pipeline this specification names. A specification references
     * exactly one pipeline, and the executor already resolves its id through the product's parser,
     * so naming it again here would only be an id to copy by hand and get wrong.
     */
    record State(PipelineState expected) implements Matcher {}

    /**
     * The count of observable errors the pipeline this specification names has published, read from its
     * metrics face. A specification references exactly one pipeline - the same one {@link State} names -
     * so the count is written on its own rather than keyed by an id an author would copy by hand.
     */
    record ErrorCount(long expected) implements Matcher {}

    /**
     * The canonical code of the failure the pipeline this specification names has published, read from its
     * status face. The state says a run died and the error count says it was counted; only the code says
     * what killed it, so a regression that swaps one reason for another is visible here and nowhere else.
     */
    record FailureCode(String expected) implements Matcher {}

    /**
     * How many changes the pipeline this specification names could never place in a document, added up over
     * every namespace of it, read from its metrics face. Written as one number rather than keyed by
     * namespace: a namespace name is derived from the pipeline and an embed path inside it, so naming one
     * here would be an internal name for an author to copy by hand and to rewrite whenever a step is renamed.
     */
    record DeadLettered(long expected) implements Matcher {}

    /**
     * One document at an endpoint: located by the equality settings in {@code where}, held to scalar
     * values by path in {@code expect}, to list lengths by path in {@code size}, and to paths that
     * must not be there at all in {@code absent}. Paths read {@code a.b} for a field of a field and
     * {@code items[0].sku} for a field of a list element. Identity is spelled {@code id} whatever the
     * store calls it; the driver owns that spelling.
     *
     * <p><b>{@code absent} is the only one of the three that a wider document can fail.</b> Every
     * value and length an author writes down is satisfied by a document that carries extra fields
     * beside them, so a target built one column too wide passes every other expectation there is.
     */
    record Doc(
            TableAlias table,
            Map<String, Object> where,
            Map<String, Object> expect,
            Map<String, Long> size,
            List<String> absent)
            implements Matcher {
        public Doc {
            where = Collections.unmodifiableMap(new LinkedHashMap<>(where));
            expect = Collections.unmodifiableMap(new LinkedHashMap<>(expect));
            size = Collections.unmodifiableMap(new LinkedHashMap<>(size));
            absent = List.copyOf(absent);
        }
    }

    static Matcher count(TableAlias table, long rows) {
        return new Count(Map.of(table, rows));
    }

    static Matcher state(PipelineState expected) {
        return new State(expected);
    }

    static Matcher errorCount(long expected) {
        return new ErrorCount(expected);
    }

    static Matcher failureCode(String expected) {
        return new FailureCode(expected);
    }

    static Matcher deadLettered(long expected) {
        return new DeadLettered(expected);
    }
}
