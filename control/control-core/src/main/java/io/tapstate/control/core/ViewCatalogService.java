package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.TableSnapshot;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.ServeResource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TableRef;
import io.tapstate.core.model.ViewBlock;
import io.tapstate.core.model.ViewResource;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Builds the Views read from stored definitions and one observation per maintaining pipeline. */
public final class ViewCatalogService {

    private static final long FRESH_LIMIT_SECONDS = 60;
    private static final String LAG_METRIC = "tapstate.pipeline.lag";

    private final ArtifactQueryService artifacts;
    private final PipelineObservationQueryService observations;
    private final DataBrowserService browser;
    private final SourceSchemaQueryService schemas;
    private final Clock clock;

    public ViewCatalogService(ArtifactQueryService artifacts, PipelineObservationQueryService observations,
            DataBrowserService browser, SourceSchemaQueryService schemas) {
        this(artifacts, observations, browser, schemas, Clock.systemUTC());
    }

    public ViewCatalogService(ArtifactQueryService artifacts, PipelineObservationQueryService observations,
            DataBrowserService browser, SourceSchemaQueryService schemas, Clock clock) {
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.observations = Objects.requireNonNull(observations, "observations");
        this.browser = Objects.requireNonNull(browser, "browser");
        this.schemas = Objects.requireNonNull(schemas, "schemas");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public List<ViewCatalogItem> list() {
        Map<String, Resource> resources = new LinkedHashMap<>();
        for (StoredResource stored : artifacts.listResources()) {
            resources.put(stored.resource().id(), stored.resource());
        }
        String storeId = storeId(resources);
        List<ViewCatalogItem> result = new ArrayList<>();
        for (Resource resource : resources.values()) {
            if (!(resource instanceof PipelineResource pipeline)) continue;
            PipelineStatus status = observations.findStatus(pipeline.id()).orElse(null);
            PipelineMetrics metrics = status == null ? null : observations.metrics(pipeline.id());
            PipelineSnapshot snapshot = status == null ? null : observations.snapshot(pipeline.id());
            ViewBlock.Inline view = inlineView(pipeline.view(), resources);
            if (view != null) {
                String collection = view.storage() != null && view.storage().warm() != null
                        ? view.storage().warm().collection() : view.id();
                result.add(item("view", pipeline.id(), pipeline.id() + ":" + view.id(), storeId, collection,
                        status, metrics, snapshot, resources));
            }
            List<SyncElement> sync = syncElements(pipeline.serve(), resources);
            for (SyncElement element : sync) {
                Map<String, String> explicitNames = element.rename() == null ? null : element.rename().map();
                List<String> outputTables = explicitNames != null && !explicitNames.isEmpty()
                        ? List.copyOf(explicitNames.keySet()) : sourceTables(pipeline, resources);
                String kind = pipeline.transforms() != null && !pipeline.transforms().isEmpty()
                        ? "view" : "replica";
                for (String table : outputTables) {
                    String collection = element.rename() != null && element.rename().map() != null
                            ? element.rename().map().getOrDefault(table, table) : table;
                    result.add(item(kind, pipeline.id(), pipeline.id() + ":" + element.id() + ":" + table,
                            element.source(), collection, status, metrics, snapshot, resources));
                }
            }
        }
        result.sort(Comparator.comparing(ViewCatalogItem::pipelineId).thenComparing(ViewCatalogItem::id));
        return List.copyOf(result);
    }

    private ViewCatalogItem item(String kind, String pipelineId, String id, String sourceId,
            String collection, PipelineStatus pipelineStatus, PipelineMetrics metrics,
            PipelineSnapshot snapshot, Map<String, Resource> resources) {
        Resource resource = resources.get(sourceId);
        String database = resource instanceof SourceResource source ? database(source) : null;
        Long documents = null;
        Long sizeBytes = null;
        if (resource instanceof SourceResource) {
            try {
                DataBrowserStatsReport stats = browser.stats(sourceId, collection);
                documents = stats.numOfRows();
                sizeBytes = stats.storageSize();
            } catch (TapstateException unavailable) {
                // A declared output can precede its first collection. Keep the row and leave its size unknown.
            }
        }
        return new ViewCatalogItem(id, kind,
                new ViewCatalogItem.Location(sourceId, database, collection), pipelineId,
                freshness(pipelineStatus, metrics, snapshot), documents, sizeBytes, null);
    }

    private ViewCatalogItem.Freshness freshness(PipelineStatus status, PipelineMetrics metrics,
            PipelineSnapshot snapshot) {
        if (status == null || status.state() == PipelineState.NEW) {
            return new ViewCatalogItem.Freshness("not-loaded", null, null, null);
        }
        if (status.state() == PipelineState.RUNNING) {
            Integer progress = snapshot == null ? null : snapshot.snapshot().values().stream()
                    .map(TableSnapshot::donePct).filter(Objects::nonNull).min(Integer::compareTo).orElse(null);
            if (progress != null && progress < 100) {
                return new ViewCatalogItem.Freshness("loading", null, null, progress);
            }
            Long lag = lag(metrics);
            if (lag != null && lag <= FRESH_LIMIT_SECONDS && status.state() == PipelineState.RUNNING) {
                return new ViewCatalogItem.Freshness("fresh", lag, null, null);
            }
        }
        Long stale = status.observedAt() == null ? null
                : Math.max(0, Duration.between(status.observedAt(), Instant.now(clock)).toSeconds());
        return new ViewCatalogItem.Freshness("stale", null, stale, null);
    }

    private static Long lag(PipelineMetrics metrics) {
        if (metrics == null) return null;
        return metrics.facts().stream().filter(fact -> LAG_METRIC.equals(fact.name()))
                .map(MetricFact::points).flatMap(List::stream)
                .filter(point -> point.attributes().containsKey(MetricAttributes.TABLE_ID))
                .map(MetricPoint::value).filter(Objects::nonNull).max(Long::compareTo).orElse(null);
    }

    private static String storeId(Map<String, Resource> resources) {
        return resources.values().stream().filter(SourceResource.class::isInstance)
                .map(SourceResource.class::cast)
                .filter(source -> source.metadata() != null
                        && "true".equals(source.metadata().labels().get("store")))
                .map(SourceResource::id).findFirst().orElse("views");
    }

    private static String database(SourceResource source) {
        Object configured = source.config().get("database");
        if (configured instanceof String database) return database;
        Object uri = source.config().get("uri");
        if (!(uri instanceof String text)) return null;
        int scheme = text.indexOf("://");
        int path = text.indexOf('/', scheme < 0 ? 0 : scheme + 3);
        if (path < 0) return null;
        int query = text.indexOf('?', path);
        String name = text.substring(path + 1, query < 0 ? text.length() : query);
        return name.isBlank() ? null : name;
    }

    private static ViewBlock.Inline inlineView(ViewBlock block, Map<String, Resource> resources) {
        if (block instanceof ViewBlock.Inline inline) return inline;
        if (block instanceof ViewBlock.Use use && resources.get(use.use()) instanceof ViewResource view) {
            return new ViewBlock.Inline(use.id(), use.from(), view.primaryKey(), view.storage());
        }
        return null;
    }

    private static List<SyncElement> syncElements(ServeBlock block, Map<String, Resource> resources) {
        if (block instanceof ServeBlock.Inline inline) return inline.sync() == null ? List.of() : inline.sync();
        if (block instanceof ServeBlock.Use use && resources.get(use.use()) instanceof ServeResource serve) {
            return serve.sync() == null ? List.of() : serve.sync();
        }
        return List.of();
    }

    private List<String> sourceTables(PipelineResource pipeline, Map<String, Resource> resources) {
        List<String> names = new ArrayList<>();
        for (String sourceId : pipeline.sourceIds()) {
            if (!(resources.get(sourceId) instanceof SourceResource source)) continue;
            if (source.tables() == null) {
                schemas.find(sourceId).ifPresent(report -> report.tables().forEach(table -> names.add(table.name())));
            } else {
                for (TableRef table : source.tables()) {
                    if (table instanceof TableRef.Literal literal) names.add(literal.name());
                    if (table instanceof TableRef.Spec spec) names.add(spec.name());
                }
            }
        }
        return names;
    }
}
