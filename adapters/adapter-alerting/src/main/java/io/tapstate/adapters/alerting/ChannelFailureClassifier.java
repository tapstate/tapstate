package io.tapstate.adapters.alerting;

import io.tapstate.core.alerting.DeliveryEvidence.Outcome;
import io.tapstate.spi.alerting.NotificationChannel.*;
import java.time.Instant;
import java.util.Optional;

/** Protocol acceptance and scheduling action are deliberately separate dimensions. */
public final class ChannelFailureClassifier {
    private ChannelFailureClassifier() { }
    public static Result http(int status, Instant retryAfter) {
        if (status >= 200 && status < 300) { return new Result(Outcome.ACCEPTED, Action.STOP, null, Phase.FINAL_RESPONSE, status, "webhook"); }
        boolean retry = status == 408 || status == 429 || status >= 500 && status <= 599 && status != 501 && status != 505;
        Outcome outcome = status >= 500 || status < 200 ? Outcome.UNKNOWN : Outcome.NOT_ACCEPTED;
        return new Result(outcome, retry ? Action.RETRY : Action.STOP, retry ? retryAfter : null, Phase.FINAL_RESPONSE, status, "webhook");
    }
    /** Empty means a valid intermediate reply, never final delivery acceptance. */
    public static Optional<Result> smtp(Phase phase, int status) {
        if (phase == Phase.FINAL_RESPONSE && status >= 200 && status <= 299) {
            return Optional.of(new Result(Outcome.ACCEPTED, Action.STOP, null, phase, status, "smtp"));
        }
        if (status >= 200 && status <= 299 || phase == Phase.BODY && status == 354 || phase == Phase.AUTHENTICATION && status == 334) {
            return Optional.empty();
        }
        if (status >= 400 && status <= 599) {
            return Optional.of(new Result(Outcome.NOT_ACCEPTED, status < 500 ? Action.RETRY : Action.STOP, null, phase, status, "smtp"));
        }
        return Optional.of(new Result(phase == Phase.FINAL_RESPONSE ? Outcome.UNKNOWN : Outcome.NOT_ACCEPTED,
                Action.STOP, null, phase, status, "smtp"));
    }
}
