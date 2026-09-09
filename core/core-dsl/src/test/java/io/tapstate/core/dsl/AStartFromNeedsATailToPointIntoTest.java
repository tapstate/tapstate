package io.tapstate.core.dsl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * {@code start_from} says where an incremental tail begins, so a read that has no tail has nowhere for
 * it to point. A cdc source read {@code snapshot_only} is exactly that read: the source can stream, but
 * this pipeline asked it for the current rows once and nothing after them.
 *
 * <p>Accepting the pairing is the failure this refuses, and it is quiet in both directions the setting
 * could be resolved. Dropped, the pipeline runs having silently discarded a line its author wrote.
 * Honoured, it would have to grow the tail the author said not to read -- which is the opposite of what
 * the other setting asked for. Neither reading is available, so the pairing itself has to be refused
 * where the author can still see it, and validate is the only place that is true.
 *
 * <p>Both controls matter more than the refusal here, because a rule that refused {@code start_from}
 * outright, or refused {@code snapshot_only} outright, would satisfy the first case just as well while
 * being useless: the discriminating claim is about the two together.
 */
class AStartFromNeedsATailToPointIntoTest {

    private static final String SOURCE = """
            version: tapstate/v1
            kind: source
            id: src_a
            connector: mysql
            mode: cdc
            tables: [ orders ]
            """;

    /** A pipeline over the cdc source above, carrying whatever {@code settings} the case gives it. */
    private static String pipeline(String settings) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: p
                source: src_a
                serve:
                  from: orders
                  sync: [ { id: s, source: src_a } ]
                """
                + settings;
    }

    private static void batch(String settings) {
        DslParser parser = new DslParser();
        Workspace.of(Stream.of(SOURCE, pipeline(settings)).map(parser::parse).toList());
    }

    @Test
    void aSnapshotOnlyReadRefusesAStartFrom() {
        Throwable t = catchThrowable(
                () -> batch("settings: { read_mode: snapshot_only, start_from: earliest }\n"));

        assertThat(t).isInstanceOf(DslException.class);
        DslException refused = (DslException) t;
        assertThat(refused.code()).isEqualTo(DslError.MODE_MISMATCH);
        assertThat(refused.path())
                .as("the setting that cannot be honoured is the one the author is pointed at")
                .isEqualTo("settings.start_from");
        assertThat(refused.args())
                .as("and the read_mode that removed the tail is what explains why")
                .containsEntry("mode", "snapshot_only");
    }

    @Test
    void theSameStartFromIsFineOnceTheReadHasATail() {
        assertThatCode(() -> batch("settings: { read_mode: cdc_only, start_from: earliest }\n"))
                .as("cdc_only over a cdc source is all tail, so the setting has somewhere to point")
                .doesNotThrowAnyException();
    }

    @Test
    void theSameSnapshotOnlyReadIsFineWithoutAStartFrom() {
        assertThatCode(() -> batch("settings: { read_mode: snapshot_only }\n"))
                .as("reading the current rows once is a legal read; it is the pairing that is not")
                .doesNotThrowAnyException();
    }
}
