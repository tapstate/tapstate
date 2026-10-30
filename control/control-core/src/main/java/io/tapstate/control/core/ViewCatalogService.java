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
import io.tapstate.core.model.SourceResource;

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
    private final PipelineOutputNames outputs;
    private final Clock clock;

    public ViewCatalogService(ArtifactQueryService artifacts, PipelineObservationQueryService observations,
            DataBrowserService browser, PipelineOutputNames outputs) {
        this(artifacts, observations, browser, outputs, Clock.systemUTC());
    }

    public ViewCatalogService(ArtifactQueryService artifacts, PipelineObservationQueryService observations,
            DataBrowserService browser, PipelineOutputNames outputs, Clock clock) {
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.observations = Objects.requireNonNull(observations, "observations");
        this.browser = Objects.requireNonNull(browser, "browser");
        this.outputs = Objects.requireNonNull(outputs, "outputs");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public List<ViewCatalogItem> list() {
        return catalog().items();
    }

    public record Catalog(List<ViewCatalogItem> items, List<PipelineOutputNames.Unresolved> unresolved) {
        public Catalog {
            items = List.copyOf(items);
            unresolved = List.copyOf(unresolved);
        }
    }

    public Catalog catalog() {
        Map<String, Resource> resources = new LinkedHashMap<>();
        for (StoredResource stored : artifacts.listResources()) {
            resources.put(stored.resource().id(), stored.resource());
        }
        List<PipelineOutputNames.Unresolved> unresolved = new ArrayList<>();
        List<ViewCatalogItem> result = new ArrayList<>();
        for (Resource resource : resources.values()) {
            if (!(resource instanceof PipelineResource pipeline)) continue;
            var publication=observations.findRead(pipeline.id()).orElse(null);
            PipelineStatus status = publication == null ? null : publication.status();
            PipelineMetrics metrics = publication == null ? null : publication.metrics();
            PipelineSnapshot snapshot = publication == null ? null : publication.snapshot();
            PipelineOutputNames.Description description = outputs.describe(pipeline, resources);
            unresolved.addAll(description.names().unresolved());
            for (PipelineOutputNames.Output output : description.names().outputs()) {
                result.add(item(output.kind(), pipeline.id(), pipeline.id() + ":" + output.identity(),
                        output.connection(), output.table(), status, metrics, snapshot, resources,
                        description.inputs().getOrDefault(output.identity(), Set.of()), description.qualifiedTables()));
            }
        }
        result.sort(Comparator.comparing(ViewCatalogItem::pipelineId).thenComparing(ViewCatalogItem::id));
        return new Catalog(result, unresolved);
    }

    private ViewCatalogItem item(String kind, String pipelineId, String id, String sourceId,
            String collection, PipelineStatus pipelineStatus, PipelineMetrics metrics,
            PipelineSnapshot snapshot, Map<String, Resource> resources,Set<PipelineInputs.Input> inputs,Set<String> qualifiedTables) {
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
                freshness(pipelineStatus, metrics, snapshot,inputs,qualifiedTables), documents, sizeBytes, null);
    }

    private ViewCatalogItem.Freshness freshness(PipelineStatus status, PipelineMetrics metrics,
            PipelineSnapshot snapshot,Set<PipelineInputs.Input> inputs,Set<String> qualifiedTables) {
        Instant now=clock.instant();
        if(status==null || status.state()==PipelineState.NEW)
            return freshness("not-loaded",null,null,null,now,null,"not-loaded");
        var measurement=eventAge(metrics,inputs,qualifiedTables,now);
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
        var tables=inputTables(inputs,qualifiedTables);
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
    private static Set<String> inputTables(Set<PipelineInputs.Input> inputs,Set<String> qualifiedTables) {
        var tables=new java.util.HashSet<String>();
        for(var input:inputs)if(input.table()!=null){if(!qualifiedTables.contains(input.table()))tables.add(input.table());tables.add(input.source()+"."+input.table());}
        return Set.copyOf(tables);
    }
    private static EventAge eventAge(PipelineMetrics metrics,Set<PipelineInputs.Input> inputs,Set<String> qualifiedTables,Instant now) {
        if(metrics==null || inputs.isEmpty() || inputs.stream().anyMatch(input->input.table()==null))return null;
        var points=new LinkedHashMap<String,MetricPoint>();
        var tables=inputTables(inputs,qualifiedTables);
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
                if(qualifiedTables.contains(input.table()) || inputs.stream().filter(other->input.table().equals(other.table())).count()>1)return null;
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

}
