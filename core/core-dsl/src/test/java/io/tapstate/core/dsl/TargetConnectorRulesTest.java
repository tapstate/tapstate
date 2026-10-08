package io.tapstate.core.dsl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.model.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Target roles follow catalog capabilities on-prem and MongoDB/Atlas scope in cloud. */
class TargetConnectorRulesTest {

    private static final TapstateCatalog CATALOG = TapstateCatalog.load();

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

    private static void validate(boolean cloud, String... yamls) {
        List<Resource> batch = parse(yamls);
        TargetConnectorRules.validate(batch, batch, CATALOG, cloud);
    }

    private static void validate(boolean cloud, List<String> submitted, List<String> stored) {
        List<Resource> batch = parse(submitted.toArray(String[]::new));
        List<Resource> known = new ArrayList<>(parse(stored.toArray(String[]::new)));
        known.addAll(batch);
        TargetConnectorRules.validate(batch, known, CATALOG, cloud);
    }

    @Test
    void onPremAcceptsEverySinkCapableConnector() {
        assertThatCode(() -> validate(false, READ_SOURCE, target("tgt_pg", "postgres"),
                pipelineWritingTo("tgt_pg"))).doesNotThrowAnyException();
        assertThatCode(() -> validate(false, READ_SOURCE, target("tgt_my", "mysql"),
                pipelineWritingTo("tgt_my"))).doesNotThrowAnyException();
    }

    @Test
    void onPremLeavesPrivateConnectorsForTheOperatorToValidate() {
        assertThatCode(() -> validate(false, READ_SOURCE, target("private-sink", "acme-warehouse"),
                pipelineWritingTo("private-sink"))).doesNotThrowAnyException();
    }

    @Test
    void cloudRefusesConnectorsOutsideTheMongoDbAndAtlasCatalogEntries() {
        Throwable thrown = catchThrowable(() -> validate(true, READ_SOURCE,
                target("private-sink", "acme-warehouse"), pipelineWritingTo("private-sink")));
        assertThat(thrown).isInstanceOf(DslException.class);
        assertThat(((DslException) thrown).args()).containsEntry("connector", "acme-warehouse");
    }

    static Stream<String> sinkConnectors() {
        return CATALOG.all().stream().filter(entry -> entry.sink().capable())
                .map(entry -> entry.id()).filter(id -> !"aws-rds-mysql".equals(id));
    }

    @ParameterizedTest(name = "on-prem permits sink connector {0}")
    @MethodSource("sinkConnectors")
    void allowsAnyCatalogSinkConnector(String connector) {
        assertThatCode(() -> validate(false, READ_SOURCE, target("tgt", connector),
                pipelineWritingTo("tgt"))).doesNotThrowAnyException();
    }

    @Test
    void refusesAConnectorWithoutSinkCapability() {
        Throwable thrown = catchThrowable(() -> validate(false, READ_SOURCE, target("tgt", "ai-chat"),
                pipelineWritingTo("tgt")));
        assertThat(thrown).isInstanceOf(DslException.class);
        DslException error = (DslException) thrown;
        assertThat(error.code()).isEqualTo(DslError.UNSUPPORTED_TARGET_CONNECTOR);
        assertThat(error.path()).isEqualTo("serve.sync[0].source");
        assertThat(error.args()).containsEntry("connector", "ai-chat").containsEntry("source", "tgt");
    }

    @Test
    void cloudAcceptsMongoDbAndAtlasAndRefusesOtherSinkConnectors() {
        assertThatCode(() -> validate(true, READ_SOURCE, target("mongo", "mongodb"),
                pipelineWritingTo("mongo"))).doesNotThrowAnyException();
        assertThatCode(() -> validate(true, READ_SOURCE, target("atlas", "mongodb-atlas"),
                pipelineWritingTo("atlas"))).doesNotThrowAnyException();
        Throwable thrown = catchThrowable(() -> validate(true, READ_SOURCE, target("pg", "postgres"),
                pipelineWritingTo("pg")));
        assertThat(thrown).isInstanceOf(DslException.class);
        assertThat(((DslException) thrown).args()).containsEntry("connector", "postgres");
    }

    @Test
    void judgesTargetsResolvedFromStoredResources() {
        Throwable thrown = catchThrowable(() -> validate(false,
                List.of(READ_SOURCE, pipelineWritingTo("tgt")), List.of(target("tgt", "ai-chat"))));
        assertThat(thrown).isInstanceOf(DslException.class);
        assertThat(((DslException) thrown).code()).isEqualTo(DslError.UNSUPPORTED_TARGET_CONNECTOR);
    }

    @Test
    void doesNotRejudgeAStoredPipelineWhenItsReadSourceChanges() {
        String rotatedSource = READ_SOURCE.replace("mode: cdc", "config: { password: rotated }\nmode: cdc");
        assertThatCode(() -> validate(false,
                List.of(rotatedSource),
                List.of(READ_SOURCE, target("tgt", "ai-chat"), pipelineWritingTo("tgt"))))
                .doesNotThrowAnyException();
    }

    @Test
    void leavesTheReadSideAlone() {
        // The same batch that refuses writing to mysql above reads from mysql here, and the only
        // difference is which side of the pipeline the connection is on.
        assertThatCode(() -> validate(true,
                READ_SOURCE, target("tgt_mg", "mongodb"), pipelineWritingTo("tgt_mg")))
                .doesNotThrowAnyException();

        Throwable t = catchThrowable(() -> validate(true,
                READ_SOURCE, target("tgt_my", "mysql"), pipelineWritingTo("tgt_my")));
        assertThat(t).isInstanceOf(DslException.class);
        assertThat(((DslException) t).args()).containsEntry("connector", "mysql");
    }

    @ParameterizedTest(name = "RDS MySQL remains source-only in cloud={0}")
    @ValueSource(booleans = {false, true})
    void awsRdsMysqlMayBeReadButNotUsedAsASyncTarget(boolean cloud) {
        String awsSource = READ_SOURCE.replace("connector: mysql", "connector: aws-rds-mysql");
        assertThatCode(() -> validate(cloud,
                awsSource, target("tgt_mg", "mongodb-atlas"), pipelineWritingTo("tgt_mg")))
                .doesNotThrowAnyException();

        Throwable failure = catchThrowable(() -> validate(cloud,
                awsSource, target("tgt_aws", "aws-rds-mysql"), pipelineWritingTo("tgt_aws")));
        assertThat(failure).isInstanceOf(DslException.class);
        DslException refusal = (DslException) failure;
        assertThat(refusal.code()).isEqualTo(DslError.UNSUPPORTED_TARGET_CONNECTOR);
        assertThat(refusal.path()).isEqualTo("serve.sync[0].source");
        assertThat(refusal.args()).containsEntry("connector", "aws-rds-mysql");
    }

    @Test
    void onPremAcceptsAnElasticsearchSinkFromTheCatalog() {
        assertThatCode(() -> validate(false,
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
        Throwable t = catchThrowable(() -> validate(true,
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

        assertThatCode(() -> validate(true,
                List.of(rotated),
                List.of(READ_SOURCE, target("tgt_pg", "postgres"), pipelineWritingTo("tgt_pg"))))
                .doesNotThrowAnyException();
    }

    @Test
    void judgesASyncCarriedByAServeDefinition() {
        Throwable t = catchThrowable(() -> validate(true,
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
        Throwable t = catchThrowable(() -> validate(true,
                List.of(DEFINITION_USING),
                List.of(READ_SOURCE, target("tgt_pg", "postgres"), definitionWritingTo("tgt_pg"))));

        assertThat(t).isInstanceOf(DslException.class);
        assertThat(((DslException) t).args()).containsEntry("resource", "out");
    }

    @Test
    void acceptsADefinitionOnTheSupportedKind() {
        assertThatCode(() -> validate(true,
                READ_SOURCE, target("tgt_mg", "mongodb-atlas"), definitionWritingTo("tgt_mg"), DEFINITION_USING))
                .doesNotThrowAnyException();
    }

    @Test
    void judgesServeDefinitionsReachedThroughPipelines() {
        Throwable thrown = catchThrowable(() -> validate(false, READ_SOURCE, target("tgt", "ai-chat"),
                definitionWritingTo("tgt"), DEFINITION_USING));
        assertThat(thrown).isInstanceOf(DslException.class);
        DslException error = (DslException) thrown;
        assertThat(error.path()).isEqualTo("sync[0].source");
        assertThat(error.args()).containsEntry("resource", "out");
    }

    @Test
    void leavesPushEgressAlone() {
        String pipeline = """
                version: tapstate/v1
                kind: pipeline
                id: p
                source: src_my
                serve:
                  from: orders
                  push: [ { id: e, source: tgt, topic: orders } ]
                """;
        assertThatCode(() -> validate(false, READ_SOURCE, target("tgt", "ai-chat"), pipeline))
                .doesNotThrowAnyException();
    }
}
