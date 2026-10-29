package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.PipelineInputs;
import io.tapstate.core.model.FromClause;
import java.util.Set;
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
            var publication=observations.findRead(pipeline.id()).orElse(null);
            PipelineStatus status = publication == null ? null : publication.status();
            PipelineMetrics metrics = publication == null ? null : publication.metrics();
            PipelineSnapshot snapshot = publication == null ? null : publication.snapshot();
            ViewBlock.Inline view = inlineView(pipeline.view(), resources);
            if (view != null) {
                String collection = view.storage() != null && view.storage().warm() != null
                        ? view.storage().warm().collection() : view.id();
                result.add(item("view", pipeline.id(), pipeline.id() + ":" + view.id(), storeId, collection,
                        status, metrics, snapshot, resources,PipelineInputs.reaching(pipeline,resources,FromClause.list(view.from()))));
            }
            List<SyncElement> sync = syncElements(pipeline.serve(), resources);
            FromClause served=switch(pipeline.serve()) {
                case ServeBlock.Inline inline -> inline.from();
                case ServeBlock.Use use -> use.from();
                case null -> null;
            };
            var servedInputs=served==null?Set.<PipelineInputs.Input>of():PipelineInputs.reaching(pipeline,resources,served);
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
                            element.source(), collection, status, metrics, snapshot, resources,
                            servedInputs.stream().filter(input->input.table()==null || input.table().equals(table)
                                    || (input.source()+"."+input.table()).equals(table)).collect(java.util.stream.Collectors.toUnmodifiableSet())));
                }
            }
        }
        result.sort(Comparator.comparing(ViewCatalogItem::pipelineId).thenComparing(ViewCatalogItem::id));
        return List.copyOf(result);
    }

    private ViewCatalogItem item(String kind, String pipelineId, String id, String sourceId,
            String collection, PipelineStatus pipelineStatus, PipelineMetrics metrics,
            PipelineSnapshot snapshot, Map<String, Resource> resources,Set<PipelineInputs.Input> inputs) {
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
                freshness(pipelineStatus, metrics, snapshot,inputs), documents, sizeBytes, null);
    }

    private ViewCatalogItem.Freshness freshness(PipelineStatus status, PipelineMetrics metrics,
            PipelineSnapshot snapshot,Set<PipelineInputs.Input> inputs) {
        Instant now=clock.instant();
        if(status==null || status.state()==PipelineState.NEW)
            return freshness("not-loaded",null,null,null,now,null,"not-loaded");
        var measurement=eventAge(metrics,inputs,now);
        Instant observed=status.observedAt();
        if(measurement!=null && measurement.clockInvalid())
            return freshness("stale",null,null,measurement.observedAt(),now,null,"clock-or-time-unavailable");
        if(observed==null || observed.isAfter(now))
            return freshness("stale",measurement==null?null:measurement.seconds(),null,observed,now,null,"clock-or-time-unavailable");
        if(measurement!=null && (observed==null || measurement.observedAt().isBefore(observed)))observed=measurement.observedAt();
        Long age=observed==null || observed.isAfter(now)?null:Duration.between(observed,now).toSeconds();
        Long eventAge=measurement==null?null:measurement.seconds();
        if(age==null)return freshness("stale",eventAge,null,observed,now,null,"clock-or-time-unavailable");
        if(status.state()!=PipelineState.RUNNING)
            return freshness("stale",eventAge,age,observed,now,null,"pipeline-inactive");
        if(age>FRESH_LIMIT_SECONDS)return freshness("stale",eventAge,age,observed,now,null,"observation-stale");
        var tables=inputTables(inputs);
        Integer progress=snapshot==null?null:snapshot.snapshot().entrySet().stream()
                .filter(entry->tables.contains(entry.getKey())).map(entry->entry.getValue().donePct())
                .filter(Objects::nonNull).min(Integer::compareTo).orElse(null);
        if(progress!=null && progress<100)return freshness("loading",null,age,observed,now,progress,"snapshot-loading");
        if(measurement==null)return freshness("stale",null,age,observed,now,null,"measurement-unavailable");
        return freshness("fresh",eventAge,age,observed,now,null,"observed");
    }
    private static ViewCatalogItem.Freshness freshness(String state,Long eventAge,Long age,Instant observed,
            Instant now,Integer progress,String reason) {
        return new ViewCatalogItem.Freshness(state,state.equals("fresh")?eventAge:null,state.equals("stale")?age:null,
                progress,eventAge,observed,now,age,"s","max-input-table-event-age",reason);
    }
    private record EventAge(Long seconds,Instant observedAt,boolean clockInvalid) { }
    private static Set<String> inputTables(Set<PipelineInputs.Input> inputs) {
        var tables=new java.util.HashSet<String>();
        for(var input:inputs)if(input.table()!=null){tables.add(input.table());tables.add(input.source()+"."+input.table());}
        return Set.copyOf(tables);
    }
    private static EventAge eventAge(PipelineMetrics metrics,Set<PipelineInputs.Input> inputs,Instant now) {
        if(metrics==null || inputs.isEmpty() || inputs.stream().anyMatch(input->input.table()==null))return null;
        var points=new LinkedHashMap<String,MetricPoint>();
        var tables=inputTables(inputs);
        for(var fact:metrics.facts())if(LAG_METRIC.equals(fact.name()) && fact.type()==io.tapstate.core.lifecycle.MetricType.GAUGE && fact.unit().equals("s"))
            for(var point:fact.points()) {
                String table=point.attributes().get(MetricAttributes.TABLE_ID);
                if(table!=null && tables.contains(table) && point.value()!=null && point.value()>=0) {
                    if(points.putIfAbsent(table,point)!=null)return null;
                }
            }
        long maximum=0;Instant measured=null;
        for(var input:inputs) {
            var point=points.get(input.source()+"."+input.table());
            if(point==null) {
                if(inputs.stream().filter(other->input.table().equals(other.table())).count()>1)return null;
                point=points.get(input.table());
            }
            if(point==null)return null;
            if(point.observedAt()==null || point.observedAt().isAfter(now))return new EventAge(null,point.observedAt(),true);
            long elapsed=Duration.between(point.observedAt(),now).toSeconds();
            long seconds=point.value()>Long.MAX_VALUE-elapsed?Long.MAX_VALUE:point.value()+elapsed;
            maximum=Math.max(maximum,seconds);
            if(measured==null || point.observedAt().isBefore(measured))measured=point.observedAt();
        }
        return new EventAge(maximum,measured,false);
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
