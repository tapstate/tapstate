package io.tapstate.core.validation;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Coordinates of a current Nest row: array identities and object field prefixes stay separate. */
public record ValidationNestScope(ValidationObservation observation, List<String> objectPath) {
    public ValidationNestScope {
        Objects.requireNonNull(observation, "observation");
        if (objectPath == null || objectPath.size() > 32) throw ValidationError.invalid("nestScope.objectPath");
        for (String field : objectPath) ValidationError.identifier(field, "nestScope.objectPath");
        objectPath = List.copyOf(objectPath);
    }
    public ValidationNestScope(ValidationObservation observation) { this(observation, List.of()); }

    /** Full path within the current keyed ancestor, never a physical query column name. */
    public List<String> path(List<String> localPath) {
        if (localPath == null || localPath.size() > 32 - objectPath.size()) throw ValidationError.invalid("nestScope.path");
        for (String field : localPath) ValidationError.identifier(field, "nestScope.path");
        var path = new ArrayList<>(objectPath); path.addAll(localPath);
        return List.copyOf(path);
    }
    public ValidationRootCheck.Location location(List<String> localPath) {
        return new ValidationRootCheck.Location(observation.root(), new ArrayList<>(path(localPath)));
    }
    public ValidationNestScope object(List<String> localPath) { return new ValidationNestScope(observation, path(localPath)); }

    /** Entering an element preserves the original root key and starts a new local row coordinate. */
    public ValidationNestScope member(List<String> arrayPath, ValidationKey elementKey) {
        var parent = observation.root();
        var ancestors = new ArrayList<>(parent.ancestors());
        ancestors.add(new ValidationRoot.Ancestor(path(arrayPath), elementKey));
        return new ValidationNestScope(new ValidationObservation(observation.run(),
                new ValidationRoot(parent.mappingId(), parent.outputId(), parent.rootKey(), ancestors), observation.revision()));
    }
}
