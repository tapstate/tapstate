package io.tapstate.core.validation;

import java.util.Objects;

/** User-confirmed ordered key fields and model/codec versions; recommendations alone are not this contract. */
public record ValidationConfirmedMapping(String mappingId, String outputId, long revision, String confirmationId,
                                         String specHash, String comparisonRulesVersion,
                                         Side source, Side target) {
    public record Side(ValidationKeyBinding keys, String modelVersion, String codecBinding) {
        public Side {
            Objects.requireNonNull(keys, "keys");
            modelVersion = ValidationError.identifier(modelVersion, "mapping.model");
            codecBinding = ValidationError.identifier(codecBinding, "mapping.codec");
        }
    }
    public ValidationConfirmedMapping {
        mappingId = ValidationError.identifier(mappingId, "mapping.id");
        outputId = ValidationError.identifier(outputId, "mapping.output");
        ValidationError.positive(revision, "mapping.revision");
        confirmationId = ValidationError.identifier(confirmationId, "mapping.confirmation");
        specHash = ValidationError.identifier(specHash, "mapping.spec");
        comparisonRulesVersion = ValidationError.identifier(comparisonRulesVersion, "mapping.rules");
        Objects.requireNonNull(source, "source"); Objects.requireNonNull(target, "target");
        var left = source.keys().components(); var right = target.keys().components();
        if (left.size() != right.size()) throw ValidationError.invalid("mapping.arity");
        for (int i = 0; i < left.size(); i++) {
            if (left.get(i).domain() != right.get(i).domain()
                    || left.get(i).numericPolicy() != right.get(i).numericPolicy()) throw ValidationError.invalid("mapping.domain");
        }
    }
    public void requireObservation(ValidationObservation observation) {
        Objects.requireNonNull(observation, "observation");
        if (!mappingId.equals(observation.root().mappingId()) || !outputId.equals(observation.root().outputId())
                || revision != observation.run().keyMappingRevision() || !specHash.equals(observation.run().specHash())
                || !comparisonRulesVersion.equals(observation.run().comparisonRulesVersion())) throw ValidationError.invalid("mapping.observation");
    }
}
