package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;

import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Every word an author may write, derived from whatever owns it.
 *
 * <p>Nothing here is a list someone typed. The lifecycle steps are the product's verbs, the states
 * are the product's states, the keys are the envelope record's own components, and the matcher and
 * step keywords are enums the parser switches over exhaustively. The point is not tidiness: this is
 * what the specification schema and the vocabulary listing are generated from, and a listing
 * generated from a second copy of the truth would drift from the parser exactly the way a
 * hand-written guide does - only harder to notice, because it would look derived.
 *
 * <p>The reflection here runs in a test JVM at build time. This module ships nothing, so there is no
 * runtime path for it to be on.
 */
final class Vocabulary {

    /** Steps written on their own. The product's verb set defines them; this only spells them. */
    static final SortedSet<String> LIFECYCLE_STEPS =
            sorted(LifecycleVerb.values(), LifecycleVerb::id);

    /**
     * Steps written on their own that the product spells with more than one verb. Kept apart from
     * {@link #LIFECYCLE_STEPS} because that set is the product's verb enum and has to stay it: a
     * word here is one a person types, not one the control plane exposes.
     */
    static final SortedSet<String> COMPOSED_STEPS = sorted(ComposedVerb.values(), ComposedVerb::word);

    /** Steps that carry a body. */
    static final SortedSet<String> BODIED_STEPS = sorted(StepKeyword.values(), StepKeyword::word);

    /**
     * Lifecycle steps that may instead carry one source id, meaning that stream alone. A subset of
     * {@link #LIFECYCLE_STEPS} and spelled from the same product verbs - what the harness decides is
     * which of them a source may be named under, not what any of them is called.
     */
    static final SortedSet<String> STREAM_SCOPED_STEPS = sorted(StreamVerb.values(), StreamVerb::word);

    /** Matcher words, shared by {@code await} and {@code assert} - two timings, one vocabulary. */
    static final SortedSet<String> MATCHERS = sorted(MatcherWord.values(), MatcherWord::word);

    /** Changes a cdc step can produce. */
    static final SortedSet<String> CDC_OPERATIONS = sorted(CdcOp.values(), Vocabulary::lowerName);

    /** States the {@code state} matcher can expect, as the product publishes them. */
    static final SortedSet<String> PIPELINE_STATES = sorted(PipelineState.values(), Enum::name);

    /** The envelope's keys, in the order the record declares them. */
    static final List<String> TOP_LEVEL_KEYS = componentsOf(Envelope.class);

    /** The provisioning facet's keys, in dependency order - which is the order they are declared. */
    static final List<String> SETUP_KEYS = componentsOf(Setup.class);

    /** Store kinds the harness can provide, as a specification spells them. */
    static final SortedSet<String> DATABASE_KINDS = sorted(DatabaseKind.values(), DatabaseKind::word);

    /** The one key a store request carries. */
    static final Set<String> DATABASE_KEYS = Set.of("kind");

    /** The keys a seed entry may carry - a generated count, or the rows themselves. */
    static final Set<String> SEED_KEYS = Set.of("rows", "values");

    /** The keys a doc matcher body carries: how to find the document, and what to hold it to. */
    static final Set<String> DOC_KEYS = Set.of("where", "expect", "size");

    /**
     * The keys a valued cdc change carries, per operation. Both locate a row the same way the doc
     * matcher locates a document - one {@code where} spelling across the surface, not one per word -
     * and only an update also says what to write.
     *
     * <p>An insert names rows rather than locating one, so it carries {@code values} and no
     * {@code where} - the same spelling a seed uses, so an author who can seed a table can add to it.
     */
    static Set<String> valuedChangeKeys(CdcOp op) {
        return switch (op) {
            case UPDATE -> Set.of("where", "set");
            case DELETE -> Set.of("where");
            case INSERT -> Set.of("values");
        };
    }

    static String lowerName(CdcOp op) {
        return lowerName((Enum<?>) op);
    }

    private Vocabulary() {
    }

    private static <T> SortedSet<String> sorted(T[] values, Function<T, String> word) {
        return Collections.unmodifiableSortedSet(
                Arrays.stream(values).map(word).collect(Collectors.toCollection(TreeSet::new)));
    }

    private static List<String> componentsOf(Class<?> record) {
        return Arrays.stream(record.getRecordComponents())
                .map(RecordComponent::getName)
                .toList();
    }

    private static String lowerName(Enum<?> value) {
        return value.name().toLowerCase(Locale.ROOT);
    }
}
