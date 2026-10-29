package io.tapstate.control.core;

import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.*;
import io.tapstate.spi.store.*;
import io.tapstate.core.model.Resource;
import java.util.concurrent.atomic.AtomicInteger;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ViewCatalogServiceTest {
    static final Instant T=Instant.parse("2026-10-29T00:00:00Z");
    record Fixture(ViewCatalogService service,AtomicInteger reads) { }
    Fixture fixture(Instant now,Observation sample) {
        var parser=new DslParser();
        var pipeline=parser.parse("""
                version: tapstate/v1
                kind: pipeline
                id: pipeline
                source: src
                view: { id: order_state, from: orders, primary_key: id }
                """);
        var source=parser.parse("""
                version: tapstate/v1
                kind: source
                id: src
                connector: test
                config: {}
                tables: [orders, unrelated]
                """);
        var values=new HashMap<String,Resource>();values.put(pipeline.id(),pipeline);values.put(source.id(),source);
        ArtifactStore store=new ArtifactStore() {
            public void saveAll(List<Resource> resources){resources.forEach(r->values.put(r.id(),r));}
            public Optional<Resource> get(String id){return Optional.ofNullable(values.get(id));}
            public List<Resource> list(){return List.copyOf(values.values());}
        };
        var artifacts=new ArtifactQueryService(store);var reads=new AtomicInteger();
        ObservationStore observations=new ObservationStore() {
            public void save(Observation ignored){throw new AssertionError("read only");}
            public Optional<Observation> read(String id){reads.incrementAndGet();return Optional.of(sample);}
            public void delete(String ignored){throw new AssertionError("read only");}
        };
        SchemaStore schemaStore=new SchemaStore() {
            public void save(DiscoveredSourceModel ignored){throw new AssertionError("read only");}
            public Optional<DiscoveredSourceModel> get(String id){return Optional.empty();}
        };
        var browser=new DataBrowserService(store,schemaStore,config->{throw new AssertionError("no target probe");},
                (config,table)->{throw new AssertionError("no target probe");},
                (config,query)->{throw new AssertionError("no target probe");},
                (config,request,listener)->{throw new AssertionError("no target probe");});
        var schemas=new SourceSchemaQueryService(store,schemaStore);
        return new Fixture(new ViewCatalogService(artifacts,new PipelineObservationQueryService(artifacts,observations),browser,schemas,Clock.fixed(now,ZoneOffset.UTC)),reads);
    }
    static MetricPoint point(String table,long seconds,Instant observed) {
        return MetricPoint.reading(Map.of(MetricAttributes.TABLE_ID,table,MetricAttributes.PIPELINE_ID,"pipeline"),observed,seconds);
    }
    static Observation observation(Instant at,MetricPoint... points) {
        return new Observation("pipeline",PipelineState.RUNNING,Map.of(),Map.of(),Map.of(),null,at,
                List.of(new MetricFact("tapstate.pipeline.lag",MetricType.GAUGE,"s",List.of(points))));
    }
    @Test void aRetainedLagSampleDoesNotStayFreshAfterThePublisherStops() {
        var fixture=fixture(T.plusSeconds(65),observation(T,point("orders",1,T)));
        var status=fixture.service().list().getFirst().status();
        assertThat(status.state()).isEqualTo("stale");assertThat(status.reason()).isEqualTo("observation-stale");
        assertThat(status.eventAgeSeconds()).isEqualTo(66);assertThat(status.observationAgeSeconds()).isEqualTo(65);
        assertThat(status.observedAt()).isEqualTo(T);assertThat(status.refreshedAt()).isEqualTo(T.plusSeconds(65));
        assertThat(status.unit()).isEqualTo("s");assertThat(fixture.reads().get()).isOne();
    }
    @Test void sourceIdleEventAgeDoesNotMeanObservationFailureOrSyncStall() {
        var fixture=fixture(T.plusSeconds(1),observation(T,point("orders",900,T)));
        var status=fixture.service().list().getFirst().status();
        assertThat(status.state()).isEqualTo("fresh");assertThat(status.eventAgeSeconds()).isEqualTo(901);
        assertThat(status.reason()).isEqualTo("observed");
    }
    @Test void anotherOutputTableCannotSupplyThisViewsEventAge() {
        var fixture=fixture(T.plusSeconds(1),observation(T,point("orders",1,T),point("unrelated",900,T)));
        assertThat(fixture.service().list().getFirst().status().eventAgeSeconds()).isEqualTo(2);
    }
    @Test void aMissingInputMeasurementStaysUnavailable() {
        var status=fixture(T.plusSeconds(1),observation(T,point("unrelated",1,T))).service().list().getFirst().status();
        assertThat(status.state()).isEqualTo("stale");assertThat(status.reason()).isEqualTo("measurement-unavailable");
        assertThat(status.eventAgeSeconds()).isNull();
    }
    @Test void aFutureMetricHasAnExplicitClockDiagnostic() {
        var status=fixture(T.plusSeconds(1),observation(T,point("orders",1,T.plusSeconds(5)))).service().list().getFirst().status();
        assertThat(status.state()).isEqualTo("stale");assertThat(status.reason()).isEqualTo("clock-or-time-unavailable");
        assertThat(status.eventAgeSeconds()).isNull();assertThat(status.refreshedAt()).isEqualTo(T.plusSeconds(1));
    }
    @Test void anOlderMetricCannotHideAFuturePublicationClock() {
        var status=fixture(T.plusSeconds(1),observation(T.plusSeconds(5),point("orders",1,T))).service().list().getFirst().status();
        assertThat(status.state()).isEqualTo("stale");assertThat(status.reason()).isEqualTo("clock-or-time-unavailable");
        assertThat(status.observationAgeSeconds()).isNull();
    }

}
