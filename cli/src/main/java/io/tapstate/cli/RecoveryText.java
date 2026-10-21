package io.tapstate.cli;

import io.tapstate.messages.MessageCatalog;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

/** Human recovery facts, keeping archived positions separate from the successor's actual request. */
final class RecoveryText {
    private RecoveryText() {}

    static void cluster(PrintWriter out, RemoteRecovery.Cluster view) {
        if (view == null) { return; }
        out.println("recovery  " + known(view.recoveryState()) + "  quorum " + known(view.quorumReady()) + "  causes " + view.causes());
        profile(out, "current profile", view.currentProfile());
        failure(out, "profile unavailable", view.profileUnavailable());
        claim(out, "recovery coordinator", view.coordinatorClaim());
        failure(out, "coordinator unavailable", view.claimUnavailable());
        capacity(out, view.capacity());
        failure(out, "queue unavailable", view.queueUnavailable());
        items(out, view.items());
    }

    static void pipeline(PrintWriter out, RemoteRecovery.Pipeline view) {
        if (view == null) { return; }
        out.println("  recovery   " + known(view.recoveryState()) + "  incarnation " + known(view.currentIncarnation()) + "  causes " + view.causes());
        failure(out, "recovery unavailable", view.unavailable());
        capacity(out, view.capacity());
        items(out, view.items());
    }

    private static void items(PrintWriter out, List<RemoteRecovery.Item> items) {
        for (var item : items) {
            out.println("  queued     " + item.pipelineId() + "  " + item.status() + "  cause " + item.cause()
                    + "  attempt " + known(item.attempt()) + "/" + known(item.maxAttempts())
                    + "  queue " + known(item.queuePosition()) + "  sequence " + known(item.enqueueSequence()));
            out.println("  resource   incarnation " + item.incarnation() + "  artifact " + known(item.currentArtifactHash())
                    + "  intent " + item.intentFingerprint());
            out.println("  original   execution " + known(item.originalExecutionGeneration()) + "  revision " + known(item.originalExecutionRevision())
                    + "  topology " + known(item.sourceTopologyRevision()) + "  legacy profile " + known(item.legacySourceProfile()));
            profile(out, "original profile", item.originalProfile());
            profile(out, "target profile", item.targetProfile());
            out.println("  target     topology " + known(item.targetTopologyRevision()) + "  frontier " + known(item.executionFrontier())
                    + "  eligible at " + known(item.nextEligibleAt()));
            positions(out, "original position", item.originalPositions());
            if (item.permit() != null) {
                var permit = item.permit();
                out.println("  permit     " + permit.reservationId() + "  deadline " + permit.deadline()
                        + "  allocated execution " + known(permit.transferredExecutionGeneration()));
                out.println("  demand     " + JsonOut.compact(RecoveryWire.tree(permit.demandByNode())));
            }
            if (item.successor() != null) {
                var successor = item.successor();
                out.println("  successor  execution " + known(successor.pipelineClaim().executionGeneration()) + "  native job " + known(successor.nativeJobId())
                        + "  members " + successor.executionNodeIds() + "  required sources " + successor.requiredSourceIds()
                        + "  requirements recorded " + known(successor.sourceRequirementsRecorded()));
                out.println("  startup    initialized " + known(successor.nativeInitializedAt()) + "  sources accepted " + known(successor.sourcesAcceptedAt())
                        + "  finite completed " + known(successor.executionCompleted()));
                positions(out, "requested position", successor.requestedPositions());
                positions(out, "accepted position", successor.acceptedPositions());
                if (successor.failureNote() != null) {
                    out.println("  failure note " + successor.failureNote().stage() + "  at " + successor.failureNote().recordedAt());
                    diagnostic(out, successor.failureNote().diagnostic());
                }
            }
            diagnostic(out, item.diagnostic());
            claim(out, "pipeline claim", item.currentPipelineClaim());
            failure(out, "pipeline claim unavailable", item.claimUnavailable());
        }
    }

    private static void profile(PrintWriter out, String label, RemoteRecovery.Profile profile) {
        if (profile != null) { out.println("  " + label + " " + known(profile.generation()) + "  hash " + known(profile.hash()) + "  " + profile.attributes()); }
    }

    private static void claim(PrintWriter out, String label, RemoteRecovery.ClaimReading reading) {
        if (reading == null) { return; }
        var claim = reading.claim();
        out.println("  " + label + " " + claim.ownerNodeId() + "  boot " + claim.ownerBootId()
                + "  claim " + known(claim.claimGeneration()) + "  execution " + known(claim.executionGeneration())
                + "  topology " + known(claim.topologyRevision()) + "  profile " + known(claim.profileGeneration())
                + "  leased " + known(reading.leased()) + "  remaining ms " + known(reading.leaseRemainingMillis())
                + "  until " + known(reading.leaseUntil()));
    }

    private static void capacity(PrintWriter out, RemoteRecovery.Capacity capacity) {
        if (capacity == null) { return; }
        out.println("  capacity   " + capacity.availability() + "  provenance " + capacity.provenance());
        if (capacity.configuredLimits() != null) { out.println("  limits     " + JsonOut.compact(RecoveryWire.tree(capacity.configuredLimits()))); }
        out.println("  occupied   " + (capacity.occupiedByNode() == null ? "unknown" : JsonOut.compact(RecoveryWire.tree(capacity.occupiedByNode()))));
        failure(out, "capacity unavailable", capacity.unavailable());
    }

    private static void diagnostic(PrintWriter out, RemoteRecovery.Diagnostic diagnostic) {
        if (diagnostic == null) { return; }
        out.println("  failure    " + diagnostic.code() + "  " + MessageCatalog.bundled().render(diagnostic.code(), diagnostic.params()).message());
        out.println("  arguments  " + JsonOut.compact(diagnostic.params()));
        out.println("  disposition " + diagnostic.disposition());
        positions(out, "failure position", diagnostic.positions());
    }

    private static void failure(PrintWriter out, String label, RemoteRecovery.ReadFailure failure) {
        if (failure != null) { out.println("  " + label + " " + failure.code() + "  " + JsonOut.compact(failure.params())); }
    }

    private static void positions(PrintWriter out, String label, Map<String, RemoteRecovery.Position> positions) {
        positions.forEach((source, position) -> out.println("  " + label + " " + source + "  " + JsonOut.compact(RecoveryWire.tree(position))));
    }

    private static String known(Object value) { return value == null ? "unknown" : value.toString(); }
}
