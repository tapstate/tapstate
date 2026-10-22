package io.tapstate.core.catalog;

import io.tapstate.core.dsl.DslError;
import io.tapstate.core.dsl.DslException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.dsl.TargetConnectorRules;
import io.tapstate.core.model.Resource;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Missing catalog metadata must not turn an official source-only connector into a private sink. */
class SourceOnlyTargetWithoutCatalogTest {

    @ParameterizedTest
    @ValueSource(strings = {"db2", "db2i", "aws-rds-mysql"})
    void sourceOnlyPolicySurvivesAMissingCatalogRow(String connector) {
        TapstateCatalog empty = TapstateCatalog.build("{\"entries\":[]}", id -> {
            throw new AssertionError("an empty catalog must not resolve entries");
        });
        List<Resource> resources = batch(connector);
        for (boolean cloud : new boolean[] {false, true}) {
            Throwable failure = catchThrowable(() -> TargetConnectorRules.validate(resources, resources, empty, cloud));
            assertThat(failure).as("%s in cloud=%s", connector, cloud).isInstanceOf(DslException.class);
            DslException refusal = (DslException) failure;
            assertThat(refusal.code()).isEqualTo(DslError.UNSUPPORTED_TARGET_CONNECTOR);
            assertThat(refusal.args()).containsEntry("connector", connector).containsEntry("source", "target");
        }
        List<Resource> privateSink = batch("acme-warehouse");
        assertThatCode(() -> TargetConnectorRules.validate(privateSink, privateSink, empty, false))
                .doesNotThrowAnyException();
    }

    private static List<Resource> batch(String connector) {
        DslParser parser = new DslParser();
        return List.of(parser.parse("""
                version: tapstate/v1
                kind: source
                id: target
                connector: %s
                """.formatted(connector)), parser.parse("""
                version: tapstate/v1
                kind: pipeline
                id: copy
                source: input
                serve:
                  from: orders
                  sync: [ { id: out, source: target } ]
                """));
    }
}
