package io.tapstate.core.dsl;

import io.tapstate.core.model.Resource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * A {@code kind: source} has two roles (X18) and its {@code mode:} follows the role rather than the
 * resource: a source a pipeline reads declares one, a source only named as a write target does not.
 * The rule was decided when the grammar was, written into the model's javadoc as a validate-layer
 * obligation, and then never implemented — so until now the field was simply optional everywhere.
 *
 * <p>What that cost is not one missing refusal but two rules reading a field nobody had to write.
 * Both are exercised below: {@code schedule} on an unbounded read was let through, and
 * {@code start_from} on a genuine tail was refused with a reason ({@code (bounded)}) that was not
 * true of any source in the document.
 */
class AReadSourceDeclaresItsModeTest {

    private final DslParser parser = new DslParser();

    private static final String WRITE_TARGET = """
            version: tapstate/v1
            kind: source
            id: tgt
            connector: mysql
            config: { host: 10.0.0.2 }
            """;

    private Workspace load(String... documents) {
        List<Resource> parsed = new java.util.ArrayList<>();
        for (String document : documents) {
            parsed.add(parser.parse(document));
        }
        return Workspace.of(List.copyOf(parsed));
    }

    private static String source(String modeLine) {
        return """
                version: tapstate/v1
                kind: source
                id: src_a
                connector: mysql
                config: { host: 10.0.0.1 }
                """ + modeLine + "tables: [ t1 ]\n";
    }

    private static String pipeline(String settings) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: p
                source: src_a
                """ + settings + "serve: { from: t1, sync: [ { source: tgt } ] }\n";
    }

    @Test
    @DisplayName("a source a pipeline reads must declare its mode")
    void aSourceReadByAPipelineMustDeclareItsMode() {
        Throwable thrown = catchThrowable(() -> load(source(""), WRITE_TARGET, pipeline("")));

        assertThat(thrown).isInstanceOf(DslException.class);
        DslException ex = (DslException) thrown;
        assertThat(ex.code()).isEqualTo(DslError.MODE_REQUIRED_FOR_READ);
        // Naming the pipeline is the point: with several sources and several pipelines, the source's
        // own name does not say which of them turned it into a read.
        assertThat(ex.args()).containsEntry("source", "src_a").containsEntry("pipeline", "p");
        assertThat(ex.path()).isEqualTo("mode");
    }

    @Test
    @DisplayName("a source named only as a write target must not declare one")
    void aPureConnectionSupplierNeedsNoMode() {
        // The other half of X18, and the reason this is conditional rather than a plain required
        // field: the write target in every valid scenario carries no mode, and making mode required
        // outright would refuse the whole corpus.
        assertThatCode(() -> load(source("mode: cdc\n"), WRITE_TARGET, pipeline("")))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("a source nothing reads is not judged")
    void anUnreadSourceIsNotJudged() {
        // A batch is not always a running system: a source declared ahead of the pipeline that will
        // read it is an ordinary intermediate state, and refusing it would make the rule fire on
        // documents that are merely incomplete rather than wrong.
        assertThatCode(() -> load(source(""), WRITE_TARGET)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("omitting the mode no longer waves schedule past the rule that forbids it")
    void scheduleOverAChangeFeedIsRefusedWithOrWithoutTheModeWritten() {
        // The silent half, and the one worth a case: schedule: re-runs a pipeline and a cdc read never
        // ends, so the two are incompatible - but the incompatibility is read off mode, and an absent
        // mode answered "neither stream nor cdc", which is to say bounded. The same document was
        // refused with the line and accepted without it; now both are refused, and the pair is the
        // assertion. Written, it is still the mode rule that answers; omitted, X18 gets there first.
        assertThat(((DslException) catchThrowable(() -> load(
                source("mode: cdc\n"), WRITE_TARGET, pipeline("settings: { schedule: '0 * * * *' }\n"))))
                .code()).isEqualTo(DslError.MODE_MISMATCH);

        assertThat(((DslException) catchThrowable(() -> load(
                source(""), WRITE_TARGET, pipeline("settings: { schedule: '0 * * * *' }\n"))))
                .code()).isEqualTo(DslError.MODE_REQUIRED_FOR_READ);
    }

    @Test
    @DisplayName("start_from is no longer refused for a boundedness no source declared")
    void startFromIsJudgedAgainstADeclaredModeOrNotAtAll() {
        // The loud half, failing the other way. With the mode written the read has a tail and this is
        // legal; with it omitted the read looked bounded and this was refused as mode=(bounded) - a
        // mode nothing in the document had said. Now the omission is named for what it is.
        assertThatCode(() -> load(
                source("mode: cdc\n"), WRITE_TARGET, pipeline("settings: { start_from: latest }\n")))
                .doesNotThrowAnyException();

        assertThat(((DslException) catchThrowable(() -> load(
                source(""), WRITE_TARGET, pipeline("settings: { start_from: latest }\n"))))
                .code()).isEqualTo(DslError.MODE_REQUIRED_FOR_READ);
    }
}
