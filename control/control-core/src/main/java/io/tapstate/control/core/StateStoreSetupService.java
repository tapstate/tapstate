package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.Metadata;
import io.tapstate.core.model.SourceResource;
import java.util.Map;
import java.util.Objects;

/** Creates the Cloud destination under a server-owned marker and stable identity. */
public final class StateStoreSetupService {
    private static final String ATLAS_CONNECTOR = "mongodb-atlas";
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

    /** Reads only the deployment-managed destination. A caller-supplied Source label grants no access. */
    public String connectionUri(TapstatePrincipal caller, String id, Runnable active, AuditGate audit) {
        var operation = ControlOperations.STATE_STORE_CONNECTION_URI;
        boolean cloud = profile == DeploymentProfile.CLOUD;
        var required = cloud ? Scope.WRITE : Scope.ADMIN;
        var credential = cloud ? CredentialType.CLOUD_SESSION : CredentialType.HUMAN_JWT;
        if (caller.credentialType() != credential || !caller.permits(required)) {
            throw new TapstateException(ControlError.FORBIDDEN,
                    Map.of("op", operation.id(), "required", required.name().toLowerCase(java.util.Locale.ROOT)), null);
        }
        active.run();
        return audit.dispatch(operation, new AuditContext(caller.subject(), id), () -> {
            if (artifacts.managedDefaultSourceId().filter(id::equals).isEmpty()) throw unavailable(id);
            var inventory = artifacts.resourceInventory("source", id);
            if (inventory.managedDefaultSourceId().filter(id::equals).isEmpty()) throw unavailable(id);
            var uri = inventory.resources().stream().map(StoredResource::resource)
                    .filter(SourceResource.class::isInstance).map(SourceResource.class::cast)
                    .filter(source -> id.equals(source.id())
                            && ("mongodb".equals(source.connector()) || ATLAS_CONNECTOR.equals(source.connector())))
                    .map(source -> source.config().get("uri")).filter(String.class::isInstance).map(String.class::cast)
                    .filter(value -> value.startsWith("mongodb://") || value.startsWith("mongodb+srv://"))
                    .findFirst().orElseThrow(() -> unavailable(id));
            active.run();
            return uri;
        });
    }

    private static TapstateException unavailable(String id) {
        return new TapstateException(SourceError.NOT_FOUND, Map.of("id", id), null);
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
        ConnectionTestReport tested = connections.test(ID, ATLAS_CONNECTOR, config, principal);
        if (tested.outcome() != ConnectionTestReport.Outcome.PASSED) {
            throw new TapstateException(ControlError.MALFORMED_REQUEST,
                    Map.of("reason", "MongoDB Atlas connection test failed"), null);
        }
        SourceInput input = new SourceInput(ID,
                new Metadata(Map.of("store", "true"), "MongoDB Atlas state store"),
                ATLAS_CONNECTOR, config, null, null, null, null, null, null);
        SourceResource source = representation.toModel(input, null);
        ArtifactWriteResult result = apply.create(principal, source, ControlOperations.STATE_STORE_CONNECT);
        SourceProjectionService.throwForWriteRefusal(source.id(), result.write());
        return representation.toView((SourceResource) result.artifact().resource(),
                result.artifact().contentHash());
    }
}
