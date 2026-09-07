package io.tapstate.core.dsl;

import io.tapstate.core.model.JoinEngine;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.TransformResource;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The engine a join runs on is a closed set, and a name outside it is refused while the workspace is
 * still being validated rather than when the pipeline is started.
 *
 * <p>The field used to be free text: any spelling was accepted, so a workspace naming an engine that
 * does not exist passed validation and failed later at run time, where the author is no longer
 * looking and the failure is about assembly rather than about what they wrote.
 *
 * <p>What makes these assertions discriminating is that the refusal is read through its code, its
 * path and its arguments rather than through the fact that something was thrown. A parser that
 * refused every join, and a parser that refused this one for an unrelated reason, both throw; only
 * the code together with the offending value and the accepted set say that the engine name is what
 * was judged. The first case below is the control that keeps the rest honest -- without a spelling
 * that is accepted, a parser refusing all of them satisfies every other assertion here.
 */
class AJoinNamesAnEngineThatExistsTest {

    private final DslParser parser = new DslParser();

    private static String joinWithEngine(String engine) {
        return """
                version: tapstate/v1
                kind: transform
                id: cust_wide
                type: join
                engine: %s
                sql: |
                  SELECT c.id AS customer_id FROM c JOIN o ON o.customer_id = c.id
                """
                .formatted(engine);
    }

    @Test
    void theEngineThisReleaseRunsJoinsOnIsAccepted() {
        TransformResource t = (TransformResource) parser.parse(joinWithEngine("builtin"));

        assertThat(((TransformBody.Join) t.body()).engine()).isEqualTo(JoinEngine.BUILTIN);
    }

    @Test
    void anEngineNameThisReleaseDoesNotRunIsRefusedWhileValidating() {
        // A name that reads like a real engine is the interesting one: it is what an author who has
        // read about another carrier would write, and free text would have carried it all the way to
        // the run that cannot honour it.
        Throwable thrown = catchThrowable(() -> parser.parse(joinWithEngine("duckdb")));

        assertThat(thrown).isInstanceOf(DslException.class);
        DslException ex = (DslException) thrown;
        assertThat(ex.code()).isEqualTo(DslError.ILLEGAL_VALUE);
        assertThat(ex.path()).isEqualTo("engine");
        assertThat(ex.args()).containsEntry("value", "duckdb");
    }

    @Test
    void anArbitraryStringIsRefusedTheSameWayAndTheMessageNamesWhatWouldBeAccepted() {
        Throwable thrown = catchThrowable(() -> parser.parse(joinWithEngine("not-an-engine")));

        assertThat(thrown).isInstanceOf(DslException.class);
        DslException ex = (DslException) thrown;
        assertThat(ex.code()).isEqualTo(DslError.ILLEGAL_VALUE);
        assertThat(ex.args()).containsEntry("value", "not-an-engine");
        // The accepted set travels with the refusal. An author who is only told "no" has the failure
        // but not the fix, and the set is the whole of the fix for a closed field.
        assertThat(String.valueOf(ex.args().get("expected"))).contains(JoinEngine.BUILTIN.yaml());
    }

    @Test
    void anEmptyEngineIsNotQuietlyTreatedAsTheDefault() {
        // Falling back to the only member when the field is blank would make the field look required
        // while behaving as optional, and the pipeline that reaches production would be one nobody
        // wrote. This asserts the refusal, not any particular spelling of it.
        Throwable thrown = catchThrowable(() -> parser.parse(joinWithEngine("\"\"")));

        assertThat(thrown).isInstanceOf(DslException.class);
        assertThat(((DslException) thrown).code()).isEqualTo(DslError.ILLEGAL_VALUE);
    }
}
