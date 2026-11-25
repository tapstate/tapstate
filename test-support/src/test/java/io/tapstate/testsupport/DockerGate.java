package io.tapstate.testsupport;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.DockerClientFactory;

/**
 * Decides what an unavailable Docker daemon means for a test that needs real containers.
 *
 * <p>The usual annotation for this ({@code @Testcontainers(disabledWithoutDocker = true)}) answers
 * "skip" everywhere, which is wrong in exactly one place that matters: a suite whose reason to exist
 * is running against the real thing would report a green build in CI having run nothing. So the answer
 * is split by environment - a developer without Docker gets an honest abort, CI without Docker gets a
 * failure.
 *
 * <p>Usable two ways. A suite that owns its container lifecycle calls {@link #require()} from
 * {@code @BeforeAll}. A suite that lets the Testcontainers extension start static containers cannot:
 * that extension runs before any {@code @BeforeAll} method, so it would hit the missing daemon first
 * and raise its own error before this class was ever consulted. Those suites use
 * {@link RequiresDocker}, which registers this class as an execution condition - and conditions are
 * evaluated before any callback, whatever order the annotations were written in.
 *
 * <p>The skip is a disabled condition rather than an aborted one on purpose. Both keep a Docker-less
 * developer machine green, but an aborted container is reported as nothing at all: the suite vanishes
 * from the run instead of appearing in the skipped count. Trading a visible "skipped" for silence is
 * the wrong direction for a gate whose entire subject is tests that seem to have run.
 */
public class DockerGate implements ExecutionCondition {

    /** Set on every GitHub Actions runner, and by convention on other CI services. */
    private static final String CI_ENV = "CI";

    private static final String SKIP_REASON =
            "no Docker daemon: skipping a test that needs real endpoints. On Docker 29 the daemon "
                    + "rejects the API version the client negotiates by default; pass -Dapi.version=1.44 "
                    + "to run these locally.";

    private static final String FAIL_REASON =
            "no Docker daemon in CI: these tests must run here, and skipping them would report a "
                    + "green build that verified nothing";

    public enum Decision {

        /** Docker answers: run against real containers. */
        RUN,

        /** No Docker, no CI: a developer machine, where absence is ordinary. */
        SKIP,

        /** No Docker in CI: the gate is absent, and pretending otherwise is the bug. */
        FAIL
    }

    static Decision decide(boolean dockerAvailable, boolean ci) {
        if (dockerAvailable) {
            return Decision.RUN;
        }
        return ci ? Decision.FAIL : Decision.SKIP;
    }

    /** Applies the decision to the calling test: returns, aborts, or fails. */
    public static void require() {
        applyDecision(decide(dockerAvailable(), ci()));
    }

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        var decision=decide(dockerAvailable(),ci());
        if(decision==Decision.RUN)context.getTestClass().ifPresent(ContainerLabels::configure);
        return resultFor(decision);
    }

    /** The decision as a test that has already started sees it: proceed, abort, or fail. */
    static void applyDecision(Decision decision) {
        switch (decision) {
            case RUN -> {
            }
            case SKIP -> Assumptions.abort(SKIP_REASON);
            case FAIL -> throw new AssertionError(FAIL_REASON);
        }
    }

    /** The same decision as a condition, evaluated before the test starts at all. */
    static ConditionEvaluationResult resultFor(Decision decision) {
        return switch (decision) {
            case RUN -> ConditionEvaluationResult.enabled("a Docker daemon answered");
            case SKIP -> ConditionEvaluationResult.disabled(SKIP_REASON);
            case FAIL -> throw new AssertionError(FAIL_REASON);
        };
    }

    /**
     * Whether a daemon answers. Testcontainers throws several unrelated runtime exceptions when it
     * cannot reach one, and every single one of them means the same thing here, so none may escape:
     * the gate's job is to answer the question, not to become a third outcome of its own.
     */
    static boolean dockerAvailable() {
        try {
            return DockerClientFactory.instance().isDockerAvailable();
        } catch (RuntimeException e) {
            return false;
        }
    }

    static boolean isCi(String environmentValue) {
        return "true".equals(environmentValue);
    }

    private static boolean ci() {
        return isCi(System.getenv(CI_ENV));
    }
}
