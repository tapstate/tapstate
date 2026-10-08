package io.tapstate.core.dsl;

import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.ServeResource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.SyncElement;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/** Deployment-aware validation for connections used by {@code serve.sync}. */
public final class TargetConnectorRules {

    private TargetConnectorRules() {
    }

    /**
     * Validates write targets against the live sink catalog and deployment profile, with nothing stored
     * before: every submitted source is new.
     */
    public static void validate(
            Collection<Resource> submitted,
            Collection<Resource> known,
            TapstateCatalog catalog,
            boolean cloudDeployment) {
        validate(submitted, known, List.of(), catalog, cloudDeployment);
    }

    /**
     * Validates write targets against the live sink catalog and deployment profile.
     *
     * <p>A submitted pipeline or serve definition is judged on every element it syncs. A submitted source
     * whose connector is new - a source not {@code stored} before, or one whose connector the edit changes -
     * is judged as the write target it may already be: every known pipeline or serve definition that syncs
     * to it is judged on those elements, so a connection cannot become a target on a connector the release
     * refuses by being edited after the pipeline writing to it was accepted. Only those elements are
     * judged, and an edit that keeps a source's connector is not judged as a target here, so a stored
     * referrer's other targets, or a connection an earlier release filed, never refuse an unrelated edit.
     */
    public static void validate(
            Collection<Resource> submitted,
            Collection<Resource> known,
            Collection<Resource> stored,
            TapstateCatalog catalog,
            boolean cloudDeployment) {
        Map<String, Resource> byId = new LinkedHashMap<>();
        for (Resource resource : known) byId.put(resource.id(), resource);
        for (Resource resource : submitted) {
            judge(resource, any -> true, byId, catalog, cloudDeployment);
        }
        Map<String, String> storedConnectors = new LinkedHashMap<>();
        for (Resource resource : stored) {
            if (resource instanceof SourceResource source) storedConnectors.put(source.id(), source.connector());
        }
        Set<String> retargeted = submitted.stream()
                .filter(resource -> resource instanceof SourceResource source
                        && !Objects.equals(storedConnectors.get(source.id()), source.connector()))
                .map(Resource::id).collect(Collectors.toSet());
        if (retargeted.isEmpty()) return;
        Set<String> submittedIds = submitted.stream().map(Resource::id).collect(Collectors.toSet());
        for (Resource resource : known) {
            if (!submittedIds.contains(resource.id())) {
                judge(resource, retargeted::contains, byId, catalog, cloudDeployment);
            }
        }
    }

    /** Judges the sync elements of a pipeline's inline serve or of a serve definition that write to {@code judged}. */
    private static void judge(
            Resource resource,
            Predicate<String> judged,
            Map<String, Resource> byId,
            TapstateCatalog catalog,
            boolean cloudDeployment) {
        if (resource instanceof PipelineResource pipeline) {
            checkPipeline(pipeline, judged, byId, catalog, cloudDeployment);
        } else if (resource instanceof ServeResource serve) {
            checkSync(serve.sync(), serve.id(), "sync", judged, byId, catalog, cloudDeployment);
        }
    }

    private static void checkPipeline(
            PipelineResource pipeline,
            Predicate<String> judged,
            Map<String, Resource> byId,
            TapstateCatalog catalog,
            boolean cloudDeployment) {
        if (pipeline.serve() instanceof ServeBlock.Inline inline) {
            checkSync(inline.sync(), pipeline.id(), "serve.sync", judged, byId, catalog, cloudDeployment);
        } else if (pipeline.serve() instanceof ServeBlock.Use use
                && byId.get(use.use()) instanceof ServeResource definition) {
            checkSync(definition.sync(), definition.id(), "sync", judged, byId, catalog, cloudDeployment);
        }
    }

    private static void checkSync(
            List<SyncElement> sync,
            String owner,
            String prefix,
            Predicate<String> judged,
            Map<String, Resource> byId,
            TapstateCatalog catalog,
            boolean cloudDeployment) {
        if (sync == null) return;
        String supported = cloudDeployment
                ? "mongodb-atlas"
                : catalog.all().stream().filter(entry -> entry.sink().capable())
                        .map(ConnectorCatalogEntry::id).sorted().collect(Collectors.joining(", "));
        for (int i = 0; i < sync.size(); i++) {
            SyncElement element = sync.get(i);
            if (!judged.test(element.source())) continue;
            if (!(byId.get(element.source()) instanceof SourceResource target)) continue;
            ConnectorCatalogEntry connector;
            try {
                connector = catalog.byId(target.connector());
            } catch (IllegalArgumentException missing) {
                connector = null;
            }
            // On-prem deployments may register private connectors outside the bundled catalog. Keep
            // their target validation in the deployment's hands; cloud remains an explicit Atlas-only
            // boundary and must refuse connectors that are not known to be Atlas.
            if (connector == null && !cloudDeployment) continue;
            if (connector != null && connector.sink().capable()
                    && (!cloudDeployment || "mongodb-atlas".equals(connector.id()))) continue;

            String path = prefix + "[" + i + "].source";
            throw new DslException(DslError.UNSUPPORTED_TARGET_CONNECTOR, path, 0, 0, null,
                    Map.of("connector", target.connector(), "source", element.source(), "resource", owner,
                            "supported", supported, "path", path));
        }
    }
}
