package io.tapstate.core.dsl;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.model.Resource;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A field that still parses but no longer decides anything is reported when the batch is applied, and so
 * is a source asking for a parallel read it will not get. Both apply and run; the report is the only place
 * an author learns that what they wrote reads as a choice and is not one.
 */
class WhatNoLongerDecidesHowANodeRunsIsReportedTest {

    private final DslParser parser = new DslParser();

    private List<Advisory> review(String... documents) {
        return ExecutionRules.review(java.util.Arrays.stream(documents)
                .map(parser::parse)
                .map(Resource.class::cast)
                .toList());
    }

    private static String pipeline(String settings) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: p
                source: src_a
                serve:
                  from: orders
                  sync: [ { id: out, source: tgt } ]
                """ + settings;
    }

    private static String source(String execution) {
        return """
                version: tapstate/v1
                kind: source
                id: src_a
                connector: mysql
                config: { host: h }
                mode: cdc
                tables: [ orders ]
                """ + execution;
    }

    @Test
    void aPipelineLevelParallelismAndBatchSizeAreEachReported() {
        List<Advisory> findings = review(pipeline("""
                settings: { parallelism: 4, batch_size: 500 }
                """));

        assertThat(findings).extracting(Advisory::code)
                .containsExactly(ExecutionAdvisoryError.SETTING_HAS_NO_EFFECT,
                        ExecutionAdvisoryError.SETTING_HAS_NO_EFFECT);
        assertThat(findings.get(0).params()).isEqualTo(Map.of(
                "pipeline", "p", "setting", "settings.parallelism", "instead", "execution.parallelism"));
        assertThat(findings.get(1).params()).isEqualTo(Map.of(
                "pipeline", "p", "setting", "settings.batch_size", "instead", "execution.batch.max_records"));
    }

    @Test
    void aSourceAskingForAParallelReadIsReportedWithTheNumberItAskedFor() {
        List<Advisory> findings = review(source("execution: { parallelism: 4 }\n"));

        assertThat(findings).singleElement().satisfies(finding -> {
            assertThat(finding.code()).isEqualTo(ExecutionAdvisoryError.SOURCE_READ_NOT_SPLIT);
            assertThat(finding.params()).isEqualTo(Map.of("source", "src_a", "requested", 4));
        });
    }

    @Test
    void whatDecidesNothingOnlyIsReportedAndNothingElse() {
        // A pipeline with other settings, a source asking for exactly one processor or only for a batch,
        // and a pipeline with no settings at all: every one of them runs as written.
        assertThat(review(
                pipeline("settings: { read_mode: cdc_only }\n"),
                pipeline(""),
                source("execution: { parallelism: 1 }\n"),
                source("execution: { batch: { max_records: 512 } }\n"),
                source(""))).isEmpty();
    }
}
