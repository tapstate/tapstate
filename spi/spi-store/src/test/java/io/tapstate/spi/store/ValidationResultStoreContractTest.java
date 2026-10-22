package io.tapstate.spi.store;

import io.tapstate.core.validation.*;
import io.tapstate.core.common.TapstateException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

/** Minimal transaction-domain fixture. Real CONFIG adapter conformance is verified separately. */
class ValidationResultStoreContractTest {
    @Test void originalRunAndOpenEpochGateCompleteRootReplacement() {
        var report = new ValidationResultStore.Report(run("boot", "run"), 1);
        var store = new Fixture(report); var first = new ValidationResultStore.RootWrite(report, root(report.run(), 1));
        assertThat(store.publish(first)).isEqualTo(ValidationResultStore.Publication.APPLIED);
        assertThat(store.publish(first)).isEqualTo(ValidationResultStore.Publication.DUPLICATE);
        var newer = new ValidationResultStore.RootWrite(report, root(report.run(), 2));
        assertThat(store.publish(newer)).isEqualTo(ValidationResultStore.Publication.APPLIED);
        assertThat(store.publish(first)).isEqualTo(ValidationResultStore.Publication.STALE_OBSERVATION);
        assertThat(store.root).isSameAs(newer.check());
        assertThatThrownBy(() -> new ValidationResultStore.RootWrite(report, root(run("new-boot", "run"), 2)))
                .isInstanceOf(TapstateException.class);
        assertThat(store.publish(new ValidationResultStore.RootWrite(new ValidationResultStore.Report(report.run(), 2), newer.check())))
                .isEqualTo(ValidationResultStore.Publication.CLOSED_OR_EXPIRED);
    }
    @Test void reconstructedSameRevisionIsIdempotentButDifferentFactsAreRejected() {
        var report = new ValidationResultStore.Report(run("boot", "run"), 1);
        var store = new Fixture(report);
        store.publish(new ValidationResultStore.RootWrite(report, root(report.run(), 1)));
        assertThat(store.publish(new ValidationResultStore.RootWrite(report, root(report.run(), 1))))
                .isEqualTo(ValidationResultStore.Publication.DUPLICATE);
        assertThatThrownBy(() -> store.publish(new ValidationResultStore.RootWrite(report, root(report.run(), 1, true))))
                .isInstanceOf(TapstateException.class);
        assertThat(store.root.findings()).isEmpty();
    }

    @Test void cacheControlUsesTheFullIndependentFenceAndRequiresAnOriginalTerminalReport() {
        var identity = run("boot", "run");
        var control = new ValidationControlStore.Run(identity, 9);
        assertThat(control.fence()).isEqualTo(ValidationClaims.fence(identity.owner()));
        assertThat(control.fence().key().type()).isEqualTo(WorkloadClaimType.VALIDATION);
        new ValidationControlStore.Transition(control, ValidationRunState.STARTING, ValidationRunState.RUNNING);
        assertThatThrownBy(() -> new ValidationControlStore.Transition(control, ValidationRunState.COMPLETED, ValidationRunState.RUNNING))
                .isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> new ValidationControlStore.Retirement(control,
                new ValidationResultStore.Report(run("other-boot", "run"), 1), ValidationRunState.STOPPED)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> new ValidationControlStore.Retirement(control,
                new ValidationResultStore.Report(identity, 1), ValidationRunState.STOPPING)).isInstanceOf(TapstateException.class);
    }

    @Test void successorCleanupTargetsOldRunButUsesOnlyItsOwnCurrentClaimFence() {
        var old = new ValidationControlStore.Run(run("old-boot", "old-run"), 1);
        var report = new ValidationResultStore.Report(old.identity(), 1);
        var successor = new ValidationOwner("cluster", "pipeline", "node-b", "new-boot", 2, 2, 0);
        var cleanup = new ValidationControlStore.LostOwner(successor, old, report);
        assertThat(cleanup.fence()).isEqualTo(ValidationClaims.fence(successor)).isNotEqualTo(old.fence());
        assertThat(cleanup.obsolete().identity()).isEqualTo(old.identity());
        assertThatThrownBy(() -> new ValidationControlStore.LostOwner(old.identity().owner(), old, report)).isInstanceOf(TapstateException.class);
        var other = new ValidationOwner("cluster", "other-pipeline", "node-b", "new-boot", 2, 2, 0);
        assertThatThrownBy(() -> new ValidationControlStore.LostOwner(other, old, report)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> new ValidationControlStore.LostOwner(successor, old,
                new ValidationResultStore.Report(run("new-boot", "new-run"), 1))).isInstanceOf(TapstateException.class);
    }

    @Test void publicationAndSealCompeteOnOneConfigIdentityInEitherOrder() {
        var report = new ValidationResultStore.Report(run("boot", "run"), 1);
        var write = new ValidationResultStore.RootWrite(report, root(report.run(), 1));
        var sealFirst = new Fixture(report);
        assertThat(sealFirst.seal(report)).isEqualTo(ValidationResultStore.Seal.SEALED);
        assertThat(sealFirst.publish(write)).isEqualTo(ValidationResultStore.Publication.CLOSED_OR_EXPIRED);
        assertThat(sealFirst.root).isNull();
        var writeFirst = new Fixture(report);
        assertThat(writeFirst.publish(write)).isEqualTo(ValidationResultStore.Publication.APPLIED);
        assertThat(writeFirst.seal(report)).isEqualTo(ValidationResultStore.Seal.SEALED);
        assertThat(writeFirst.seal(report)).isEqualTo(ValidationResultStore.Seal.ALREADY_SEALED);
        assertThat(writeFirst.publish(new ValidationResultStore.RootWrite(report, root(report.run(), 2))))
                .isEqualTo(ValidationResultStore.Publication.CLOSED_OR_EXPIRED);
        assertThat(writeFirst.root).isSameAs(write.check());
    }
    private static final class Fixture implements ValidationResultStore {
        private final Report report; private boolean sealed; private ValidationRootCheck root;
        Fixture(Report report) { this.report = report; }
        @Override public synchronized Publication publish(RootWrite write) {
            if (sealed || !report.equals(write.report())) return Publication.CLOSED_OR_EXPIRED;
            if (root != null && root.observation().revision() > write.check().observation().revision()) return Publication.STALE_OBSERVATION;
            if (root != null && root.observation().revision() == write.check().observation().revision()) {
                if (!samePayload(root, write.check())) throw new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", "report.conflictingReplay"), null);
                return Publication.DUPLICATE;
            }
            root = write.check(); return Publication.APPLIED;
        }
        private static boolean samePayload(ValidationRootCheck left, ValidationRootCheck right) {
            try { assertThat(left).usingRecursiveComparison().isEqualTo(right); return true; }
            catch (AssertionError different) { return false; }
        }
        @Override public synchronized Seal seal(Report expected) {
            if (!report.equals(expected)) return Seal.CLOSED_OR_EXPIRED;
            if (sealed) return Seal.ALREADY_SEALED;
            sealed = true; return Seal.SEALED;
        }
    }
    private static ValidationRunIdentity run(String boot, String id) {
        return new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "node", boot, 1, 1, 0),
                "task", id, ValidationMode.FULL, "spec", 1, "rules");
    }
    private static ValidationRootCheck root(ValidationRunIdentity run, long revision) { return root(run, revision, false); }
    private static ValidationRootCheck root(ValidationRunIdentity run, long revision, boolean wrong) {
        var observation = new ValidationObservation(run, new ValidationRoot("mapping", "output",
                new ValidationKey(List.of(ValidationKey.Part.number(7))), List.of()), revision);
        try (var result = ValidationRootCheck.builder(observation, ValidationMethod.FULL_COLUMN,
                ValidationCheckEvent.Participation.SHOULD_EXIST, ValidationRootCheck.TargetPresence.PRESENT,
                new ValidationRootCheck.Limits(32768, 10, 10, 10, 0))) {
            result.read(ValidationRootCheck.Side.SOURCE, new ValidationCheckEvent.ReadBoundary("source", "v1", Instant.EPOCH, Instant.EPOCH));
            result.read(ValidationRootCheck.Side.TARGET, new ValidationCheckEvent.ReadBoundary("target", "v1", Instant.EPOCH, Instant.EPOCH));
            if (wrong) result.finding(new ValidationRootCheck.Finding(ValidationRootCheck.Kind.TARGET_VALUE,
                    new ValidationRootCheck.Location(observation.root(), List.of("amount"))));
            for (var check : ValidationRootCheck.Check.values()) result.checked(check);
            return result.finish();
        }
    }
}
