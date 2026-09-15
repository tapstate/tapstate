package io.tapstate.core.dsl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.tapstate.core.model.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * This release writes through one database kind. A source and a write target are the same
 * {@code kind: source} resource, so the document alone never says which role a connection is being
 * asked for -- the sync element naming it does, and that is the only place this judges.
 *
 * <p>The two halves of the claim are separate cases on purpose. A rule that refused every mysql
 * connection would satisfy "a mysql target is refused" just as well while making the product
 * unusable, so reading through the refused connector is asserted in the same batch that refuses
 * writing to it.
 *
 * <p>The rule is handed two sets — what to judge, and what to resolve ids against — and the cases
 * keep them apart: a target the batch did not resubmit is still resolved, and a stored document the
 * batch did not submit is still not judged. Passing one set for both would satisfy every case where
 * they happen to coincide and fail exactly where a deployment lives.
 *
 * <p>The gate runs on the apply path rather than in {@link Workspace}, so these cases call it
 * directly; the apply-path wiring has its own case in control-core.
 */
class TargetConnectorRulesTest {

    private static final String READ_SOURCE = """
            version: tapstate/v1
            kind: source
            id: src_my
            connector: mysql
            mode: cdc
            tables: [ orders ]
            """;

    private static final String DEFINITION_USING = """
            version: tapstate/v1
            kind: pipeline
            id: p
            source: src_my
            transforms:
              - { id: keep, from: [orders], type: filter, expr: "op != 'd'" }
            serve: out
            """;

    private static String target(String id, String connector) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                """.formatted(id, connector);
    }

    private static String pipelineWritingTo(String targetId) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: p
                source: src_my
                serve:
                  from: orders
                  sync: [ { id: s, source: %s } ]
                """.formatted(targetId);
    }

    // A definition body carries no `from:` -- the assembly that uses it supplies the wiring (X19).
    private static String definitionWritingTo(String targetId) {
        return """
                version: tapstate/v1
                kind: serve
                id: out
                sync: [ { id: s, source: %s } ]
                """.formatted(targetId);
    }

    private static List<Resource> parse(String... yamls) {
        DslParser parser = new DslParser();
        return Stream.of(yamls).map(parser::parse).toList();
    }

    /** The ordinary case: everything in hand was submitted, so the two sets coincide. */
    private static void validate(String... yamls) {
        List<Resource> batch = parse(yamls);
        TargetConnectorRules.validate(batch, batch);
    }

    /** A deployment: {@code stored} was filed by an earlier batch and is resolvable but not judged. */
    private static void validate(List<String> submitted, List<String> stored) {
        List<Resource> batch = parse(submitted.toArray(String[]::new));
        List<Resource> known = new ArrayList<>(parse(stored.toArray(String[]::new)));
        known.addAll(batch);
        TargetConnectorRules.validate(batch, known);
    }

    @Test
    void refusesASyncWritingThroughAnotherSupportedConnector() {
        Throwable t = catchThrowable(() -> validate(
                READ_SOURCE, target("tgt_pg", "postgres"), pipelineWritingTo("tgt_pg")));

        assertThat(t).isInstanceOf(DslException.class);
        DslException ex = (DslException) t;
        assertThat(ex.code()).isEqualTo(DslError.UNSUPPORTED_TARGET_CONNECTOR);
        assertThat(ex.path()).isEqualTo("serve.sync[0].source");
        assertThat(ex.args())
                .containsEntry("connector", "postgres")
                .containsEntry("source", "tgt_pg")
                // the document to edit, which a batch carrying several pipelines does not otherwise say
                .containsEntry("resource", "p")
                .containsEntry("supported", "mongodb, mongodb-atlas, aliyun-db-mongodb, tencent-db-mongodb");
    }

    static Stream<String> supportedTargets() {
        return TargetConnectorRules.SUPPORTED_TARGETS.stream();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("supportedTargets")
    void acceptsASyncWritingThroughAnyConnectorOfTheSupportedKind(String connector) {
        // Support is certified per database kind, and the managed variants of the kind are the same
        // database underneath. A deployment on one registers that variant's id and nothing else, so a
        // rule keyed to the bare id would leave it unable to install any sync at all.
        assertThatCode(() -> validate(
                READ_SOURCE, target("tgt_mg", connector), pipelineWritingTo("tgt_mg")))
                .doesNotThrowAnyException();
    }

    @Test
    void namesEverySupportedIdOfTheKindAndNothingElse() {
        assertThat(TargetConnectorRules.SUPPORTED_TARGETS)
                .containsExactly("mongodb", "mongodb-atlas", "aliyun-db-mongodb", "tencent-db-mongodb");
    }

    @Test
    void leavesTheReadSideAlone() {
        // The same batch that refuses writing to mysql above reads from mysql here, and the only
        // difference is which side of the pipeline the connection is on.
        assertThatCode(() -> validate(
                READ_SOURCE, target("tgt_mg", "mongodb"), pipelineWritingTo("tgt_mg")))
                .doesNotThrowAnyException();

        Throwable t = catchThrowable(() -> validate(
                READ_SOURCE, target("tgt_my", "mysql"), pipelineWritingTo("tgt_my")));
        assertThat(t).isInstanceOf(DslException.class);
        assertThat(((DslException) t).args()).containsEntry("connector", "mysql");
    }

    @Test
    void defersOnAConnectorThisReleaseDoesNotSupportAtAll() {
        // Not a verdict of "allowed": a connector outside the supported set is one no shipped
        // deployment can register, so only a deployment that widened its own accepted set can write
        // this document -- and this rule knows nothing about that deployment.
        assertThatCode(() -> validate(
                READ_SOURCE, target("tgt_es", "elasticsearch"), pipelineWritingTo("tgt_es")))
                .doesNotThrowAnyException();
    }

    // ---- what is judged, and what is only resolved ---------------------------------------

    @Test
    void judgesASyncWhoseTargetTheBatchDidNotResubmit() {
        // A connection document is filed once and referred to by every later batch, so a target
        // resolved only within the batch is a target normally not found -- and a rule that passes
        // what it cannot resolve would then pass the ordinary case. What reaches apply with the
        // target still stored has its own cases in control-core; this one is the rule alone.
        Throwable t = catchThrowable(() -> validate(
                List.of(READ_SOURCE, pipelineWritingTo("tgt_pg")),
                List.of(target("tgt_pg", "postgres"))));

        assertThat(t).isInstanceOf(DslException.class);
        assertThat(((DslException) t).code()).isEqualTo(DslError.UNSUPPORTED_TARGET_CONNECTOR);
    }

    @Test
    void leavesAStoredPipelineThisBatchDidNotSubmitAlone() {
        // The same stored pipeline the case above refuses when it is applied. Here the batch only
        // rotates the connection it reads from, and nothing it submitted installs a write.
        String rotated = """
                version: tapstate/v1
                kind: source
                id: src_my
                connector: mysql
                config: { password: rotated }
                mode: cdc
                tables: [ orders ]
                """;

        assertThatCode(() -> validate(
                List.of(rotated),
                List.of(READ_SOURCE, target("tgt_pg", "postgres"), pipelineWritingTo("tgt_pg"))))
                .doesNotThrowAnyException();
    }

    @Test
    void judgesASyncCarriedByAServeDefinition() {
        Throwable t = catchThrowable(() -> validate(
                READ_SOURCE, target("tgt_pg", "postgres"), definitionWritingTo("tgt_pg"), DEFINITION_USING));

        assertThat(t).isInstanceOf(DslException.class);
        DslException ex = (DslException) t;
        assertThat(ex.code()).isEqualTo(DslError.UNSUPPORTED_TARGET_CONNECTOR);
        // The element is written in the definition, where there is no `serve:` key above it, so a
        // path carrying one would not resolve in the document the author has to edit.
        assertThat(ex.path()).isEqualTo("sync[0].source");
        assertThat(ex.args()).containsEntry("resource", "out");
    }

    @Test
    void judgesADefinitionReachedOnlyThroughTheSubmittedPipeline() {
        // Nothing existence-checks a standalone definition's sinks, and the pipeline that uses one is
        // what installs its writes -- so applying the pipeline has to reach them wherever they live.
        Throwable t = catchThrowable(() -> validate(
                List.of(DEFINITION_USING),
                List.of(READ_SOURCE, target("tgt_pg", "postgres"), definitionWritingTo("tgt_pg"))));

        assertThat(t).isInstanceOf(DslException.class);
        assertThat(((DslException) t).args()).containsEntry("resource", "out");
    }

    @Test
    void acceptsADefinitionOnTheSupportedKind() {
        assertThatCode(() -> validate(
                READ_SOURCE, target("tgt_mg", "mongodb-atlas"), definitionWritingTo("tgt_mg"), DEFINITION_USING))
                .doesNotThrowAnyException();
    }

    @Test
    void leavesPushEgressAlone() {
        // The target is a connector this release supports and is not the write kind, so the same rule
        // applied to push would refuse this batch: what keeps it green is that push is not judged,
        // not that nothing here could be judged.
        String pipeline = """
                version: tapstate/v1
                kind: pipeline
                id: p
                source: src_my
                serve:
                  from: orders
                  push: [ { id: e, source: tgt_pg, topic: orders } ]
                """;

        assertThatCode(() -> validate(READ_SOURCE, target("tgt_pg", "postgres"), pipeline))
                .doesNotThrowAnyException();
    }
}
