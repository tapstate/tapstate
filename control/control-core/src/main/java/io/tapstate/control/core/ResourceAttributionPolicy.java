package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.Metadata;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.ServeResource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.TransformResource;
import io.tapstate.core.model.ViewResource;

import java.util.Map;
import java.util.Objects;

/**
 * Assigns the two server-managed provenance fields carried by an applied resource. A managed Cloud
 * Cluster marks only newly created resources; later writes preserve the original attribution and
 * cannot replace it. Existing resources without attribution remain unknown rather than being silently
 * assigned to whichever user happens to edit them first.
 */
public final class ResourceAttributionPolicy {

    private final boolean managedCloud;

    private ResourceAttributionPolicy(boolean managedCloud) {
        this.managedCloud = managedCloud;
    }

    /** Existing on-prem behavior: no new attribution is generated. */
    public static ResourceAttributionPolicy onPrem() {
        return new ResourceAttributionPolicy(false);
    }

    /** Managed Cloud behavior: new resources are marked and bound to the verified user id. */
    public static ResourceAttributionPolicy managedCloud() {
        return new ResourceAttributionPolicy(true);
    }

    /**
     * Returns the resource that may be persisted. {@code existing} is null for a create. The caller's
     * principal is already verified by the selected authentication implementation and is therefore the
     * stable Cloud user id in managed mode.
     */
    public Resource attribute(String principal, Resource submitted, Resource existing) {
        Objects.requireNonNull(principal, "principal");
        Objects.requireNonNull(submitted, "submitted");
        Metadata submittedMetadata = submitted.metadata();
        Metadata existingMetadata = existing == null ? null : existing.metadata();

        Boolean submittedCloud = submittedMetadata == null ? null : submittedMetadata.cloud();
        String submittedUser = submittedMetadata == null ? null : submittedMetadata.userId();
        Boolean existingCloud = existingMetadata == null ? null : existingMetadata.cloud();
        String existingUser = existingMetadata == null ? null : existingMetadata.userId();

        if (existing == null) {
            refuseCallerAttribution(submittedCloud, submittedUser);
            return managedCloud
                    ? withMetadata(submitted, attributed(submittedMetadata, principal))
                    : submitted;
        }

        if (submittedCloud != null && !Objects.equals(submittedCloud, existingCloud)
                || submittedUser != null && !Objects.equals(submittedUser, existingUser)) {
            throw reservedFields();
        }
        if (existingCloud == null && existingUser == null) {
            return withMetadata(submitted, authoredOnly(submittedMetadata));
        }
        return withMetadata(submitted, new Metadata(
                labels(submittedMetadata), description(submittedMetadata), existingCloud, existingUser));
    }

    /** Validates a dry-run without inventing attribution or changing its canonical bytes. */
    public void validate(Resource submitted, Resource existing) {
        Objects.requireNonNull(submitted, "submitted");
        Metadata metadata = submitted.metadata();
        if (existing == null) {
            refuseCallerAttribution(metadata == null ? null : metadata.cloud(),
                    metadata == null ? null : metadata.userId());
            return;
        }
        Metadata prior = existing.metadata();
        Boolean priorCloud = prior == null ? null : prior.cloud();
        String priorUser = prior == null ? null : prior.userId();
        if (metadata != null
                && (metadata.cloud() != null && !Objects.equals(metadata.cloud(), priorCloud)
                    || metadata.userId() != null && !Objects.equals(metadata.userId(), priorUser))) {
            throw reservedFields();
        }
    }

    private static Metadata attributed(Metadata metadata, String userId) {
        if (userId.isBlank()) {
            throw new IllegalArgumentException("verified Cloud user id must be non-blank");
        }
        return new Metadata(labels(metadata), description(metadata), true, userId);
    }

    private static Metadata authoredOnly(Metadata metadata) {
        return metadata == null ? null : new Metadata(metadata.labels(), metadata.description());
    }

    private static void refuseCallerAttribution(Boolean cloud, String userId) {
        if (cloud != null || userId != null) {
            throw reservedFields();
        }
    }

    private static TapstateException reservedFields() {
        return new TapstateException(ControlError.MALFORMED_REQUEST,
                Map.of("reason", "metadata.cloud and metadata.user_id are server-managed"), null);
    }

    private static Map<String, String> labels(Metadata metadata) {
        return metadata == null ? Map.of() : metadata.labels();
    }

    private static String description(Metadata metadata) {
        return metadata == null ? null : metadata.description();
    }

    private static Resource withMetadata(Resource resource, Metadata metadata) {
        return switch (resource) {
            case SourceResource source -> new SourceResource(
                    source.id(), metadata, source.connector(), source.config(), source.mode(), source.tables(),
                    source.srs(), source.execution(), source.experimental());
            case PipelineResource pipeline -> new PipelineResource(
                    pipeline.id(), metadata, pipeline.sources(), pipeline.transforms(), pipeline.view(),
                    pipeline.serve(), pipeline.settings(), pipeline.experimental());
            case TransformResource transform -> new TransformResource(
                    transform.id(), metadata, transform.body(), transform.experimental());
            case ViewResource view -> new ViewResource(
                    view.id(), metadata, view.primaryKey(), view.storage(), view.execution(), view.experimental());
            case ServeResource serve -> new ServeResource(
                    serve.id(), metadata, serve.sync(), serve.query(), serve.push(), serve.experimental());
        };
    }
}
