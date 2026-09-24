package io.tapstate.core.dsl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * A join reads source tables. An alias naming another step of the pipeline is refused while the
 * workspace is validated, rather than accepted and left to fail at every start.
 *
 * <p>The shape that motivated this: a {@code js} step renaming a column, joined afterwards. It passed
 * validate and apply, and then every start threw the same column-resolution error, because the join
 * resolves its SQL against the discovered model of each input and a step has none. The pipeline never
 * left {@code new}.
 *
 * <p>The first case is the control: the same join over the source tables themselves is accepted, so a
 * rule refusing every join cannot satisfy the rest.
 */
class AJoinReadsSourceTablesRatherThanStepsTest {

    private static final String SOURCES = """
            version: tapstate/v1
            kind: source
            id: music
            connector: mysql
            mode: cdc
            tables: [ tracks, song_xwalk ]
            """;

    private static final String TARGET = """
            version: tapstate/v1
            kind: source
            id: dest
            connector: mongodb
            config: { uri: u }
            """;

    private static String pipeline(String factRef) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: track_state
                source: music
                transforms:
                  - id: t_track
                    from: [ tracks ]
                    type: js
                    script: |
                      function process(record, ctx) { return record; }
                  - id: resolve
                    type: join
                    from: { t: %s, x: song_xwalk }
                    engine: builtin
                    sql: |
                      SELECT t.track_id AS track_id, x.song_id AS song_id
                      FROM t LEFT JOIN x ON t.track_id = x.track_id
                serve:
                  from: resolve
                  sync: [ { id: s, source: dest } ]
                """.formatted(factRef);
    }

    private static void batch(String... yamls) {
        DslParser parser = new DslParser();
        Workspace.of(Stream.of(yamls).map(parser::parse).toList());
    }

    @Test
    void aJoinOverSourceTablesIsAccepted() {
        assertThatCode(() -> batch(SOURCES, TARGET, pipeline("tracks"))).doesNotThrowAnyException();
    }

    @Test
    void aJoinWhoseInputIsAJsStepIsRefusedWhileValidating() {
        Throwable thrown = catchThrowable(() -> batch(SOURCES, TARGET, pipeline("t_track")));

        assertThat(thrown).isInstanceOf(DslException.class);
        DslException ex = (DslException) thrown;
        assertThat(ex.code().code()).isEqualTo("dsl.join-input-not-a-table");
        assertThat(ex.path()).isEqualTo("transforms[1].from.t");
        assertThat(ex.args())
                .containsEntry("step", "resolve")
                .containsEntry("alias", "t")
                .containsEntry("ref", "t_track");
    }

    @Test
    void aJoinWhoseInputIsAnotherJoinIsRefusedTheSameWay() {
        String joinOverJoin = """
                version: tapstate/v1
                kind: pipeline
                id: track_state
                source: music
                transforms:
                  - id: first
                    type: join
                    from: { t: tracks, x: song_xwalk }
                    engine: builtin
                    sql: |
                      SELECT t.track_id AS track_id, x.song_id AS song_id
                      FROM t LEFT JOIN x ON t.track_id = x.track_id
                  - id: second
                    type: join
                    from: { f: first, x: song_xwalk }
                    engine: builtin
                    sql: |
                      SELECT f.track_id AS track_id, x.song_id AS song_id
                      FROM f LEFT JOIN x ON f.track_id = x.track_id
                serve:
                  from: second
                  sync: [ { id: s, source: dest } ]
                """;

        Throwable thrown = catchThrowable(() -> batch(SOURCES, TARGET, joinOverJoin));

        assertThat(thrown).isInstanceOf(DslException.class);
        DslException ex = (DslException) thrown;
        assertThat(ex.code().code()).isEqualTo("dsl.join-input-not-a-table");
        assertThat(ex.path()).isEqualTo("transforms[1].from.f");
        assertThat(ex.args()).containsEntry("ref", "first");
    }
}
