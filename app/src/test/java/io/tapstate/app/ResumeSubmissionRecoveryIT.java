package io.tapstate.app;

import org.junit.jupiter.api.Test;

class ResumeSubmissionRecoveryIT {
    @Test void recoversTheSubmittedJobAfterTheControllerLosesItsReply() {
        ScopedResumeMetricsTest.exercise(ScopedResumeMetricsTest.Scenario.IMMEDIATE_RESUME,
                ScopedResumeMetricsTest.Recovery.LOST_SUBMISSION_REPLY);
    }

    @Test void aRestartedProducerPublishesItsNewWindowAfterControllerRecovery() {
        ScopedResumeMetricsTest.exercise(ScopedResumeMetricsTest.Scenario.IMMEDIATE_RESUME,
                ScopedResumeMetricsTest.Recovery.NEW_NATIVE_WINDOW);
    }
}
