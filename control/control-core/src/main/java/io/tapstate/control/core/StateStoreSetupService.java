package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.Metadata;
import io.tapstate.core.model.SourceResource;
import java.util.Map;
import java.util.Objects;

/** Creates the Cloud destination under a server-owned marker and stable identity. */
public final class StateStoreSetupService {
    private static final String ID = "atlas-store";
    private final ApplyService apply;
    private final ArtifactQueryService artifacts;
    private final SourceRepresentation representation;
    private final DeploymentProfile profile;
    private final ConnectionTestService connections;

    public StateStoreSetupService(ApplyService apply, ArtifactQueryService artifacts,
            SourceRepresentation representation, DeploymentProfile profile, ConnectionTestService connections) {
        this.apply = Objects.requireNonNull(apply, "apply");
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.representation = Objects.requireNonNull(representation, "representation");
        this.profile = Objects.requireNonNull(profile, "profile");
        this.connections = Objects.requireNonNull(connections, "connections");
    }

    public SourceView connect(String principal, Map<String, Object> config) {
        if (profile != DeploymentProfile.CLOUD) {
            throw new TapstateException(ControlError.MALFORMED_REQUEST,
                    Map.of("reason", "Atlas state store setup is only available in Cloud"), null);
        }
        if (config == null || config.isEmpty()) {
            throw new TapstateException(ControlError.MALFORMED_REQUEST,
                    Map.of("reason", "MongoDB Atlas connector settings are required"), null);
        }
        if (artifacts.listResources().stream().map(StoredResource::resource)
                .filter(SourceResource.class::isInstance).map(SourceResource.class::cast)
                .anyMatch(source -> source.metadata() != null
                        && "true".equals(source.metadata().labels().get("store")))) {
            throw new TapstateException(ControlError.MALFORMED_REQUEST,
                    Map.of("reason", "a state store is already configured"), null);
        }
        ConnectionTestReport tested = connections.test(ID, "mongodb-atlas", config, principal);
        if (tested.outcome() != ConnectionTestReport.Outcome.PASSED) {
            throw new TapstateException(ControlError.MALFORMED_REQUEST,
                    Map.of("reason", "MongoDB Atlas connection test failed"), null);
        }
        SourceInput input = new SourceInput(ID,
                new Metadata(Map.of("store", "true"), "MongoDB Atlas state store"),
                "mongodb-atlas", config, null, null, null, null, null, null);
        SourceResource source = representation.toModel(input, null);
        ArtifactWriteResult result = apply.create(principal, source, ControlOperations.STATE_STORE_CONNECT);
        SourceProjectionService.throwForWriteRefusal(source.id(), result.write());
        return representation.toView((SourceResource) result.artifact().resource(),
                result.artifact().contentHash());
    }
}
