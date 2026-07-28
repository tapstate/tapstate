package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The envelope is a frozen authoring surface: once published it is a long-term commitment, and it
 * grows by adding facets. Dropping a key breaks every specification that already writes it, so it
 * happens only as a deliberate decision and never as a side effect of tidying up. These tests are
 * what holds that line, so they read the surface exactly as an author writes it.
 */
class EnvelopeParserTest {

    private static final String FIRST_EXAMPLE =
            """
            name: mongo-to-mongo-first-sync
            setup:
              connectors: [mongodb]
              apply: [src_mongo.tap.yml, tgt_mongo.tap.yml]
              discover: [src_mongo]
            pipeline: mongo2mongo.tap.yml
            seed:
              src_mongo.orders: { rows: 100 }
            steps:
              - start
              - await: { count: { tgt_mongo.orders: 100 } }
              - cdc: { src_mongo.orders: insert 10 }
              - await: { count: { tgt_mongo.orders: 110 } }
              - stop
              - start
              - await: { count: { tgt_mongo.orders: 110 } }
            """;

    @Test
    void parsesTheFrozenFirstExample() {
        Envelope envelope = EnvelopeParser.parse(FIRST_EXAMPLE);

        assertThat(envelope.name()).isEqualTo("mongo-to-mongo-first-sync");
        assertThat(envelope.pipeline()).isEqualTo("mongo2mongo.tap.yml");
        assertThat(envelope.setup().connectors()).containsExactly("mongodb");
        assertThat(envelope.setup().apply()).containsExactly("src_mongo.tap.yml", "tgt_mongo.tap.yml");
        assertThat(envelope.setup().discover()).containsExactly("src_mongo");
        assertThat(envelope.seed()).containsExactly(new Seed(new TableAlias("src_mongo", "orders"), 100L));
    }

    /**
     * A specification can ask for the stores it needs, so a real-database case is data like any other.
     *
     * <p>Without this a case can only name endpoints that already exist, which is why every published
     * example runs on a synthetic connector and every real-database witness is hand-written Java. The
     * name is the case's handle: {@code src} is what its resources interpolate an address out of.
     */
    @Test
    void parsesTheStoresASpecificationAsksFor() {
        Envelope envelope = EnvelopeParser.parse("""
                name: a-case-that-needs-real-stores
                setup:
                  databases:
                    src: { kind: mysql }
                    tgt: { kind: mongo }
                  connectors: [mysql, mongodb]
                  apply: [src_mysql.tap.yml, tgt_mongo.tap.yml, pipeline.tap.yml]
                  discover: [src_mysql]
                pipeline: pipeline.tap.yml
                steps:
                  - start
                """);

        assertThat(envelope.setup().databases())
                .containsExactly(
                        entry("src", new DatabaseRequest(DatabaseKind.MYSQL)),
                        entry("tgt", new DatabaseRequest(DatabaseKind.MONGO)));
    }

    /** A store kind nobody can provide is an authoring mistake, and it is cheapest to say so here. */
    @Test
    void refusesAStoreKindTheHarnessCannotProvide() {
        assertThatThrownBy(() -> EnvelopeParser.parse("""
                name: a-case-asking-for-something-unprovidable
                setup:
                  databases:
                    src: { kind: cassandra }
                pipeline: pipeline.tap.yml
                steps:
                  - start
                """))
                .isInstanceOf(EnvelopeException.class)
                .hasMessageContaining("cassandra")
                .hasMessageContaining("mongo");
    }

    @Test
    void parsesTheStepSequenceInOrder() {
        Envelope envelope = EnvelopeParser.parse(FIRST_EXAMPLE);

        assertThat(envelope.steps())
                .containsExactly(
                        new Step.Lifecycle(LifecycleVerb.START),
                        new Step.Await(Matcher.count(new TableAlias("tgt_mongo", "orders"), 100L)),
                        new Step.Cdc(new TableAlias("src_mongo", "orders"), CdcOp.INSERT, 10L),
                        new Step.Await(Matcher.count(new TableAlias("tgt_mongo", "orders"), 110L)),
                        new Step.Lifecycle(LifecycleVerb.STOP),
                        new Step.Lifecycle(LifecycleVerb.START),
                        new Step.Await(Matcher.count(new TableAlias("tgt_mongo", "orders"), 110L)));
    }

    @Test
    void spellsLifecycleStepsExactlyAsTheProductSpellsItsVerbs() {
        Envelope envelope =
                EnvelopeParser.parse(minimal("steps:\n  - start\n  - pause\n  - resume\n  - stop\n"));

        assertThat(envelope.steps())
                .extracting(step -> ((Step.Lifecycle) step).verb())
                .containsExactly(
                        LifecycleVerb.START, LifecycleVerb.PAUSE, LifecycleVerb.RESUME, LifecycleVerb.STOP);
    }

    @Test
    void rejectsRunBecauseTheProductReservesItForApplyThenStart() {
        assertThatThrownBy(() -> EnvelopeParser.parse(minimal("steps:\n  - run\n")))
                .isInstanceOf(EnvelopeException.class)
                .hasMessageContaining("run");
    }

    @Test
    void parsesTheStateMatcherAgainstTheProductLifecycleEnum() {
        Envelope envelope = EnvelopeParser.parse(minimal("steps:\n  - assert: { state: RUNNING }\n"));

        assertThat(envelope.steps())
                .containsExactly(new Step.Assertion(Matcher.state(PipelineState.RUNNING)));
    }

    @Test
    void parsesTheErrorCountMatcherAsAWholeNumberWrittenOnItsOwn() {
        Envelope envelope = EnvelopeParser.parse(minimal("steps:\n  - assert: { error_count: 1 }\n"));

        assertThat(envelope.steps())
                .containsExactly(new Step.Assertion(Matcher.errorCount(1L)));
    }

    @Test
    void rejectsANegativeErrorCount() {
        assertThatThrownBy(() -> EnvelopeParser.parse(minimal("steps:\n  - assert: { error_count: -1 }\n")))
                .isInstanceOf(EnvelopeException.class)
                .hasMessageContaining("error_count");
    }

    @Test
    void acceptsAssertAndAwaitCarryingTheSameMatcherVocabulary() {
        Envelope awaited = EnvelopeParser.parse(minimal("steps:\n  - await: { count: { t.orders: 1 } }\n"));
        Envelope asserted = EnvelopeParser.parse(minimal("steps:\n  - assert: { count: { t.orders: 1 } }\n"));

        Matcher same = Matcher.count(new TableAlias("t", "orders"), 1L);
        assertThat(awaited.steps()).containsExactly(new Step.Await(same));
        assertThat(asserted.steps()).containsExactly(new Step.Assertion(same));
    }

    @Test
    void rejectsAnUnknownTopLevelKey() {
        assertThatThrownBy(() -> EnvelopeParser.parse(minimal("expect: { count: { t.orders: 1 } }\n")))
                .isInstanceOf(EnvelopeException.class)
                .hasMessageContaining("expect");
    }

    @Test
    void rejectsAnUnknownStepVerb() {
        assertThatThrownBy(() -> EnvelopeParser.parse(minimal("steps:\n  - restart\n")))
                .isInstanceOf(EnvelopeException.class)
                .hasMessageContaining("restart");
    }

    @Test
    void rejectsAnUnknownMatcherWord() {
        assertThatThrownBy(() -> EnvelopeParser.parse(minimal("steps:\n  - await: { synced: true }\n")))
                .isInstanceOf(EnvelopeException.class)
                .hasMessageContaining("synced");
    }

    @Test
    void rejectsAnAliasThatIsNotResourceIdDotTable() {
        assertThatThrownBy(() -> EnvelopeParser.parse(minimal("steps:\n  - await: { count: { orders: 1 } }\n")))
                .isInstanceOf(EnvelopeException.class)
                .hasMessageContaining("orders");
    }

    @Test
    void splitsAnAliasAtTheFirstDotSoATableNameMayContainDots() {
        Envelope envelope =
                EnvelopeParser.parse(minimal("steps:\n  - await: { count: { src.orders.2026: 1 } }\n"));

        assertThat(envelope.steps())
                .containsExactly(new Step.Await(Matcher.count(new TableAlias("src", "orders.2026"), 1L)));
    }

    /**
     * A lane word is not a harmless leftover. Nothing in the run selects a lane from the envelope, so
     * an author writing one would be describing a choice that is not taken - the surface says so out
     * loud rather than reading the word and discarding it.
     */
    @Test
    void rejectsALaneWordRatherThanReadingOneNothingSelects() {
        assertThatThrownBy(
                        () -> EnvelopeParser.parse("name: n\ntier: smoke\npipeline: p.tap.yml\nsteps:\n  - start\n"))
                .isInstanceOf(EnvelopeException.class)
                .hasMessageContaining("tier");
    }

    @Test
    void rejectsAnEnvelopeWithoutAName() {
        assertThatThrownBy(() -> EnvelopeParser.parse("pipeline: p.tap.yml\nsteps:\n  - start\n"))
                .isInstanceOf(EnvelopeException.class)
                .hasMessageContaining("name");
    }

    @Test
    void rejectsAnEnvelopeWithoutAPipeline() {
        assertThatThrownBy(() -> EnvelopeParser.parse("name: n\nsteps:\n  - start\n"))
                .isInstanceOf(EnvelopeException.class)
                .hasMessageContaining("pipeline");
    }

    @Test
    void rejectsAnUnknownCdcOperation() {
        assertThatThrownBy(() -> EnvelopeParser.parse(minimal("steps:\n  - cdc: { t.orders: truncate 1 }\n")))
                .isInstanceOf(EnvelopeException.class)
                .hasMessageContaining("truncate");
    }

    @Test
    void treatsSetupAsOptionalSoAPipelineOnlyCaseStillParses() {
        Envelope envelope = EnvelopeParser.parse(minimal("steps:\n  - start\n"));

        assertThat(envelope.setup().connectors()).isEmpty();
        assertThat(envelope.setup().apply()).isEmpty();
        assertThat(envelope.setup().discover()).isEmpty();
        assertThat(envelope.seed()).isEmpty();
    }

    @Test
    void keepsSeedEntriesInDeclarationOrder() {
        Envelope envelope =
                EnvelopeParser.parse(minimal("seed:\n  a.t1: { rows: 1 }\n  b.t2: { rows: 2 }\nsteps:\n  - start\n"));

        assertThat(envelope.seed())
                .containsExactly(new Seed(new TableAlias("a", "t1"), 1L), new Seed(new TableAlias("b", "t2"), 2L));
    }

    @Test
    void keepsCountEntriesInDeclarationOrderSoEndpointsAreReadAsWritten() {
        Envelope envelope =
                EnvelopeParser.parse(
                        minimal("steps:\n  - assert: { count: { c.t: 3, a.t: 1, b.t: 2 } }\n"));

        Matcher.Count count = (Matcher.Count) ((Step.Assertion) envelope.steps().get(0)).matcher();
        assertThat(count.expected().keySet())
                .containsExactly(new TableAlias("c", "t"), new TableAlias("a", "t"), new TableAlias("b", "t"));
    }

    @Test
    void rejectsMalformedYaml() {
        assertThatThrownBy(() -> EnvelopeParser.parse("name: [unclosed\n")).isInstanceOf(EnvelopeException.class);
    }

    @Test
    void rejectsADuplicateKeyRatherThanSilentlyKeepingTheLast() {
        assertThatThrownBy(
                        () -> EnvelopeParser.parse(minimal("steps:\n  - await: { count: { t.orders: 100, t.orders: 110 } }\n")))
                .isInstanceOf(EnvelopeException.class);
    }

    @Test
    void rejectsADuplicateTopLevelKey() {
        assertThatThrownBy(() -> EnvelopeParser.parse("name: first\nname: second\npipeline: p.tap.yml\nsteps:\n  - start\n"))
                .isInstanceOf(EnvelopeException.class);
    }

    @Test
    void rejectsAnEmptyCountThatWouldAssertNothing() {
        assertThatThrownBy(() -> EnvelopeParser.parse(minimal("steps:\n  - assert: { count: {} }\n")))
                .isInstanceOf(EnvelopeException.class)
                .hasMessageContaining("count");
    }

    @Test
    void rejectsAnEnvelopeWithNoStepsThatWouldTestNothing() {
        assertThatThrownBy(() -> EnvelopeParser.parse("name: n\npipeline: p.tap.yml\n"))
                .isInstanceOf(EnvelopeException.class)
                .hasMessageContaining("steps");
        assertThatThrownBy(() -> EnvelopeParser.parse(minimal("steps: []\n")))
                .isInstanceOf(EnvelopeException.class)
                .hasMessageContaining("steps");
    }

    @Test
    void rejectsAFractionalRowCountRatherThanTruncatingIt() {
        assertThatThrownBy(() -> EnvelopeParser.parse(minimal("seed:\n  t.o: { rows: 3.9 }\nsteps:\n  - start\n")))
                .isInstanceOf(EnvelopeException.class)
                .hasMessageContaining("3.9");
    }

    @Test
    void rejectsNegativeRowCounts() {
        assertThatThrownBy(() -> EnvelopeParser.parse(minimal("seed:\n  t.o: { rows: -5 }\nsteps:\n  - start\n")))
                .isInstanceOf(EnvelopeException.class)
                .hasMessageContaining("-5");
        assertThatThrownBy(() -> EnvelopeParser.parse(minimal("steps:\n  - cdc: { t.o: insert -5 }\n")))
                .isInstanceOf(EnvelopeException.class)
                .hasMessageContaining("-5");
        assertThatThrownBy(() -> EnvelopeParser.parse(minimal("steps:\n  - assert: { count: { t.o: -5 } }\n")))
                .isInstanceOf(EnvelopeException.class)
                .hasMessageContaining("-5");
    }

    @Test
    void readsACdcRowCountBeyondIntRange() {
        Envelope envelope = EnvelopeParser.parse(minimal("steps:\n  - cdc: { t.o: insert 5000000000 }\n"));

        assertThat(envelope.steps())
                .containsExactly(new Step.Cdc(new TableAlias("t", "o"), CdcOp.INSERT, 5_000_000_000L));
    }

    @Test
    void namesAKeyThatYamlDidNotResolveToAString() {
        assertThatThrownBy(() -> EnvelopeParser.parse(minimal("steps:\n  - await: { count: { t.o: 1 } }\non: 1\n")))
                .isInstanceOf(EnvelopeException.class)
                .hasMessageContaining("true");
    }

    @Test
    void rejectsAStepCarryingMoreThanOneVerb() {
        assertThatThrownBy(
                        () -> EnvelopeParser.parse(minimal("steps:\n  - { await: { count: { t.o: 1 } }, assert: { count: { t.o: 1 } } }\n")))
                .isInstanceOf(EnvelopeException.class);
    }

    @Test
    void holdsTheStepsListImmutable() {
        Envelope envelope = EnvelopeParser.parse(FIRST_EXAMPLE);

        assertThat(envelope.steps()).isUnmodifiable();
        assertThat(envelope.seed()).isUnmodifiable();
    }

    private static String minimal(String body) {
        return "name: n\npipeline: p.tap.yml\n" + body;
    }
}
