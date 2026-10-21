package io.tapstate.core.alerting;

import java.util.Objects;

/** Identity supplied by a trusted host binding, never inferred from display labels. */
public record AlarmKey(Scope scope, Resource resource, String typeId, String qualifier) {
    public static final int CONTRACT_VERSION = 1;
    public AlarmKey {
        Objects.requireNonNull(scope, "scope");
        Objects.requireNonNull(resource, "resource");
        typeId = AlertValidation.text(typeId, "typeId", 128);
        AlertValidation.require(typeId.matches("[a-z][a-z0-9]*([.-][a-z0-9]+)*"), "typeId", "invalid-id");
        qualifier = AlertValidation.text(qualifier, "qualifier", 256);
    }
    /** Both values must come from the authenticated deployment binding. */
    public record Scope(String authorityId, String clusterId) {
        public Scope {
            authorityId = AlertValidation.text(authorityId, "authorityId", 256);
            clusterId = AlertValidation.text(clusterId, "clusterId", 256);
        }
    }
    /** Incarnation changes only when the configuration resource is removed and recreated. */
    public record Resource(String kind, String id, String incarnation) {
        public Resource {
            kind = AlertValidation.text(kind, "resourceKind", 64);
            id = AlertValidation.text(id, "resourceId", 256);
            incarnation = AlertValidation.text(incarnation, "resourceIncarnation", 256);
        }
    }
}
