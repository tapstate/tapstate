package io.tapstate.spi.store;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationRootCheck;
import io.tapstate.core.validation.ValidationRunIdentity;
import io.tapstate.core.validation.ValidationRunSpec;
import io.tapstate.core.validation.ValidationCountCheck;
import io.tapstate.core.validation.ValidationMethod;
import java.util.Map;
import java.util.Objects;

/**
 * CONFIG-only report/root publication boundary. Reports and differences use the same verified CONFIG
 * client and transaction. CACHE claims/runs use their own verified client and full VALIDATION fence;
 * reading a CACHE lease never authorizes an atomic CONFIG write. No business PDK connection is used.
 * Implementations require prepared resources and never recreate a missing/expired report on publish.
 * Stored local observations remain unconfirmed unless the separate timing protocol establishes them.
 */
public interface ValidationResultStore {
    /** Immutable report identity and open epoch; this is not a renewable lease or current task lookup. */
    record Report(ValidationRunIdentity run, long openVersion) {
        public Report {
            Objects.requireNonNull(run, "run");
            if (openVersion < 1) throw invalid("report.openVersion");
        }
    }
    record RootWrite(Report report, ValidationRootCheck check) {
        public RootWrite {
            Objects.requireNonNull(report, "report"); Objects.requireNonNull(check, "check");
            if (!report.run().equals(check.observation().run())) throw invalid("report.run");
        }
    }
    /** COUNT is a bounded table summary, never a synthetic problem root or a row comparison. */
    record CountWrite(Report report, ValidationRunSpec snapshot, ValidationCountCheck check) {
        public CountWrite {
            Objects.requireNonNull(report); Objects.requireNonNull(snapshot); Objects.requireNonNull(check);
            if (!report.run().equals(check.run()) || snapshot.method() != ValidationMethod.COUNT
                    || !report.run().specHash().equals(snapshot.fingerprint())
                    || snapshot.task().mappings().stream().noneMatch(mapping -> mapping.mappingId().equals(check.mappingId())
                            && mapping.outputId().equals(check.outputId()))) throw invalid("report.countScope");
        }
    }
    enum Publication { APPLIED, DUPLICATE, STALE_OBSERVATION, CLOSED_OR_EXPIRED }
    enum Seal { SEALED, ALREADY_SEALED, CLOSED_OR_EXPIRED }

    /**
     * In ONE CONFIG transaction, compete on the exact open report and atomically replace the entire
     * root observation. Never append partial findings. Equal revision is idempotent only for the
     * identical payload; conflicting replay is a coded protocol error. Older revisions cannot replace
     * newer ones. Unknown commit is an exception, never APPLIED or permission to free quota.
     */
    Publication publish(RootWrite write);

    /** Same CONFIG open-report fence as root publication; implementations must never create a report. */
    default Publication publishCount(CountWrite write) { throw invalid("report.countPublicationUnsupported"); }

    /**
     * Irreversibly close this original report epoch in the same transaction domain as publish.
     * Caller seals local work, waits for known CONFIG closure, then records dependent terminal/retire
     * facts through the CACHE fence. A failed/unknown seal keeps that gate closed. Report terminal
     * mirroring uses only known CACHE facts and cannot reopen this epoch. No cross-domain transaction.
     * CLOSED_OR_EXPIRED does not prove closure of an unborn report: cancel a pending birth through
     * the original ValidationReportAdmission command, not an assumed successful seal of a missing row.
     */
    Seal seal(Report report);

    private static TapstateException invalid(String field) {
        return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", field), null);
    }
}
