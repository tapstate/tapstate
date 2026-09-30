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
import java.util.stream.Collectors;

/** Deployment-aware validation for connections used by {@code serve.sync}. */
public final class TargetConnectorRules {

    private TargetConnectorRules() {
    }

    /** Validates write targets against the live sink catalog and deployment profile. */
    public static void validate(
            Collection<Resource> submitted,
            Collection<Resource> known,
            TapstateCatalog catalog,
            boolean cloudDeployment) {
        Map<String, Resource> byId = new LinkedHashMap<>();
        for (Resource resource : known) byId.put(resource.id(), resource);
        for (Resource resource : submitted) {
            if (resource instanceof PipelineResource pipeline) {
                checkPipeline(pipeline, byId, catalog, cloudDeployment);
            } else if (resource instanceof ServeResource serve) {
                checkSync(serve.sync(), serve.id(), "sync", byId, catalog, cloudDeployment);
            }
        }
    }

    private static void checkPipeline(
            PipelineResource pipeline,
            Map<String, Resource> byId,
            TapstateCatalog catalog,
            boolean cloudDeployment) {
        if (pipeline.serve() instanceof ServeBlock.Inline inline) {
            checkSync(inline.sync(), pipeline.id(), "serve.sync", byId, catalog, cloudDeployment);
        } else if (pipeline.serve() instanceof ServeBlock.Use use
                && byId.get(use.use()) instanceof ServeResource definition) {
            checkSync(definition.sync(), definition.id(), "sync", byId, catalog, cloudDeployment);
        }
    }

    private static void checkSync(
            List<SyncElement> sync,
            String owner,
            String prefix,
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
