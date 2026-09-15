package io.tapstate.core.dsl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.tapstate.core.model.Resource;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * This release writes through one connector. A source and a write target are the same
 * {@code kind: source} resource, so the document alone never says which role a connection is being
 * asked for -- the sync element naming it does, and that is the only place this judges.
 *
 * <p>The two halves of the claim are separate cases on purpose. A rule that refused every mysql
 * connection would satisfy "a mysql target is refused" just as well while making the product
 * unusable, so reading through the refused connector is asserted in the same batch that refuses
 * writing to it.
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

    private static void validate(String... yamls) {
        DslParser parser = new DslParser();
        List<Resource> batch = Stream.of(yamls).map(parser::parse).toList();
        TargetConnectorRules.validate(batch);
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
                .containsEntry("source", "tgt_pg");
    }

    @Test
    void acceptsASyncWritingThroughTheSupportedConnector() {
        assertThatCode(() -> validate(
                READ_SOURCE, target("tgt_mg", "mongodb"), pipelineWritingTo("tgt_mg")))
                .doesNotThrowAnyException();
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

    @Test
    void judgesASyncCarriedByAServeDefinition() {
        // A definition body carries no `from:` -- the assembly that uses it supplies the wiring (X19).
        String definition = """
                version: tapstate/v1
                kind: serve
                id: out
                sync: [ { id: s, source: tgt_pg } ]
                """;
        String pipeline = """
                version: tapstate/v1
                kind: pipeline
                id: p
                source: src_my
                transforms:
                  - { id: keep, from: [orders], type: filter, expr: "op != 'd'" }
                serve: out
                """;

        Throwable t = catchThrowable(
                () -> validate(READ_SOURCE, target("tgt_pg", "postgres"), definition, pipeline));

        assertThat(t).isInstanceOf(DslException.class);
        assertThat(((DslException) t).code()).isEqualTo(DslError.UNSUPPORTED_TARGET_CONNECTOR);
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
                  push: [ { id: e, source: tgt_kfk, topic: orders } ]
                """;

        assertThatCode(() -> validate(READ_SOURCE, target("tgt_kfk", "kafka"), pipeline))
                .doesNotThrowAnyException();
    }
}
