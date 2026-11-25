package io.tapstate.spi.store;

import io.tapstate.core.model.PipelineNode;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/** Exact artifact and necessary-note coordinates for one retained Pipeline generation. */
public record ConnectorRunBindings(PipelineIntentScope scope, long effectiveIntentRevision,
        List<Binding> effective, Handoff handoff) {
    public enum Phase { REQUESTED, PREPARED, PUBLISHED }

    public record Binding(String nodeId, PipelineNode.Purpose purpose, String connectorId,
            String version, String sha256, RequiredNoteScope notes, String captureId, String chainId) {
        public Binding {
            Objects.requireNonNull(purpose, "purpose"); Objects.requireNonNull(notes, "notes");
            if (nodeId == null || nodeId.isBlank() || connectorId == null || connectorId.isBlank()
                    || version == null || version.isBlank() || sha256 == null || !sha256.matches("[a-f0-9]{64}")
                    || (captureId == null) != (chainId == null)
                    || captureId != null && (captureId.isBlank() || chainId.isBlank() || purpose != PipelineNode.Purpose.CAPTURE))
                throw new IllegalArgumentException("invalid connector execution binding");
        }
    }

    public record Handoff(long intentRevision, Phase phase, List<Binding> candidates, List<Binding> previous) {
        public Handoff {
            Objects.requireNonNull(phase, "phase");
            candidates = checked(candidates); previous = checked(previous);
            if (intentRevision < 1 || intentRevision == Long.MAX_VALUE
                    || phase == Phase.REQUESTED && !candidates.isEmpty()
                    || phase != Phase.PUBLISHED && !previous.isEmpty())
                throw new IllegalArgumentException("invalid connector binding handoff");
        }
    }

    public ConnectorRunBindings {
        Objects.requireNonNull(scope, "scope"); effective = checked(effective);
        if (effectiveIntentRevision < 0 || effectiveIntentRevision == Long.MAX_VALUE
                || effectiveIntentRevision == 0 && !effective.isEmpty()
                || handoff != null && (handoff.phase() == Phase.PUBLISHED
                        ? effectiveIntentRevision != handoff.intentRevision() || !effective.equals(handoff.candidates())
                        : effectiveIntentRevision >= handoff.intentRevision()))
            throw new IllegalArgumentException("invalid connector bindings");
        for (var value : effective) requireScope(scope, value);
        if (handoff != null) {
            for (var value : handoff.candidates()) requireScope(scope, value);
            for (var value : handoff.previous()) requireScope(scope, value);
        }
    }

    public java.util.Optional<Binding> find(String nodeId, PipelineNode.Purpose purpose) {
        return effective.stream().filter(value -> value.nodeId().equals(nodeId) && value.purpose() == purpose).findFirst();
    }

    private static List<Binding> checked(List<Binding> bindings) {
        var values = List.copyOf(bindings);
        var keys = new HashSet<String>();
        for (var binding : values)
            if (!keys.add(binding.purpose().name() + "\n" + binding.nodeId()))
                throw new IllegalArgumentException("duplicate connector binding node purpose");
        return values;
    }

    private static void requireScope(PipelineIntentScope scope, Binding value) {
        var owner = value.notes().owner();
        if (owner.bindingVersion() != scope.bindingVersion()
                || owner.kind() == RequiredNoteOwner.Kind.PIPELINE
                        && (!owner.ownerId().equals(scope.pipelineId()) || owner.stateGeneration() != scope.stateGeneration())
                || owner.kind() == RequiredNoteOwner.Kind.GLOBAL
                || value.chainId() != null && (owner.kind() != RequiredNoteOwner.Kind.CHAIN || !owner.ownerId().equals(value.chainId()))
                || value.chainId() == null && owner.kind() != RequiredNoteOwner.Kind.PIPELINE)
            throw new IllegalArgumentException("connector notes belong to another execution scope");
    }
}
