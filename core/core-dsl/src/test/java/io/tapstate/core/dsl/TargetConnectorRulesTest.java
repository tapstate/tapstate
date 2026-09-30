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

/** The target role is authorized by connector sink capability, narrowed to Atlas in cloud. */
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
    void cloudRefusesConnectorsOutsideTheAtlasCatalogEntry() {
        Throwable thrown = catchThrowable(() -> validate(true, READ_SOURCE,
                target("private-sink", "acme-warehouse"), pipelineWritingTo("private-sink")));
        assertThat(thrown).isInstanceOf(DslException.class);
        assertThat(((DslException) thrown).args()).containsEntry("connector", "acme-warehouse");
    }

    static Stream<String> sinkConnectors() {
        return CATALOG.all().stream().filter(entry -> entry.sink().capable()).map(entry -> entry.id());
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
    void cloudAcceptsAtlasAndRefusesOtherSinkConnectors() {
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
