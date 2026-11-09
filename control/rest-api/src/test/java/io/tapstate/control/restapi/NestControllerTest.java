package io.tapstate.control.restapi;

import io.tapstate.control.core.ArtifactQueryService;
import io.tapstate.control.core.DataBrowserService;
import io.tapstate.control.core.NestStateInspector;
import io.tapstate.control.core.NestTreeReader;
import io.tapstate.control.core.PipelineObservationQueryService;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.NestInspection;
import io.tapstate.core.lifecycle.NestTree;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.model.Resource;
import io.tapstate.messages.MessageCatalog;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ObservationStore;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.util.LinkedMultiValueMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * The nest reads as the controller serves them: never cached, the inspect view handed back as the step's
 * inspector read it, and an explanation whose warnings carry their catalog text so a reader needs no catalog
 * of its own. Built without a web context: what is under test is the projection, and the routes are held by
 * the registry checks elsewhere.
 */
class NestControllerTest {

    private static final String PIPELINE = """
            version: tapstate/v1
            kind: pipeline
            id: order_doc
            source: [ shop ]
            transforms:
              - id: doc
                type: nest
                from: { o: orders, c: customers }
                root:
                  from: o
                  key: [ id ]
                  embed:
                    - { from: c, on: { id: customer_id }, as: object, path: customer }
            serve:
              from: doc
              sync:
                - source: shop_target
            """;

    private static final NestTree TREE = new NestTree("order_doc", "doc", "o", List.of("id"), 20_000, List.of(
            new NestTree.Level("customer", "c", "object", false, Map.of("id", "customer_id"), List.of("id"), 10,
                    List.of())));

    @Test
    void anInspectionIsServedAsTheInspectorReadItAndNeverCached() {
        NestInspection held = new NestInspection(NestInspection.VERSION, "order_doc", "doc", List.of("id"),
                List.of(1L), true, "1:3", List.of(), List.of(), List.of(), 0L, false, Map.of("id", 1L));
        NestController controller = controller((p, s, k) -> new NestStateInspector.Result.Held(held), null);

        ResponseEntity<NestInspection> answer = controller.inspect("order_doc", "doc",
                new LinkedMultiValueMap<>(Map.of("key", List.of("1"))));

        assertThat(answer.getBody()).isSameAs(held);
        assertThat(answer.getHeaders().getCacheControl()).isEqualTo("no-store");
    }

    @Test
    void aKeyValueHoldingACommaReachesTheInspectorAsOneColumn() throws Exception {
        List<List<String>> asked = new java.util.ArrayList<>();
        NestInspection held = new NestInspection(NestInspection.VERSION, "order_doc", "doc", List.of("id"),
                List.of("north,west"), true, "1:3", List.of(), List.of(), List.of(), 0L, false, Map.of());
        NestController controller = controller((p, s, k) -> {
            asked.add(List.copyOf(k));
            return new NestStateInspector.Result.Held(held);
        }, null);
        MockMvc mvc = MockMvcBuilders.standaloneSetup(controller).build();

        mvc.perform(get("/pipelines/order_doc/nest/doc/documents").param("key", "north,west"))
                .andExpect(status().isOk());

        assertThat(asked).containsExactly(List.of("north,west"));
    }

    @Test
    void anExplanationCarriesEachWarningWithItsCatalogText() {
        NestController controller = controller(NestStateInspector.NONE, (p, s) -> TREE);

        ResponseEntity<NestExplainResponse> answer = controller.explain("order_doc", "doc");

        NestExplainResponse body = answer.getBody();
        assertThat(body.tree()).isSameAs(TREE);
        // No data browser is wired here, so every table reads as not sampled and nothing is warned about.
        assertThat(body.samples()).singleElement()
                .satisfies(sample -> assertThat(sample.notSampled()).isEqualTo("no source can be read from here"));
        assertThat(body.warnings()).isEmpty();
        assertThat(answer.getHeaders().getCacheControl()).isEqualTo("no-store");
    }

    @Test
    void aWarningIsRenderedFromTheCatalogWithItsArgumentsByName() {
        io.tapstate.control.core.NestExplanation explanation = new io.tapstate.control.core.NestExplanation(TREE,
                "no", List.of(), List.of(new io.tapstate.control.core.NestExplanation.Warning(
                        io.tapstate.control.core.MonitorError.NEST_REFERENCE_WIDELY_SHARED, "customer",
                        Map.of("embed", "customer", "table", "customers", "count", 9L, "limit", 10L))));

        NestExplainResponse.Warning warning = NestExplainResponse.of(explanation, MessageCatalog.bundled())
                .warnings().getFirst();

        assertThat(warning.code()).isEqualTo("monitor.nest-reference-widely-shared");
        assertThat(warning.message()).contains("customers", "customer", "9", "10");
        assertThat(warning.solution()).isNotBlank();
    }

    @Test
    void progressOfAPipelineThatHasPublishedNothingIsACodedRefusal() {
        NestController controller = controller(NestStateInspector.NONE, null);

        TapstateException refused = catchThrowableOfType(TapstateException.class,
                () -> controller.progress("order_doc", " "));

        assertThat(refused.code().code()).startsWith("monitor.");
    }

    private static NestController controller(NestStateInspector inspector, NestTreeReader trees) {
        Resource pipeline = new DslParser().parse(PIPELINE);
        ArtifactQueryService artifacts = new ArtifactQueryService(new ArtifactStore() {
            @Override
            public void saveAll(List<Resource> saved) {
            }

            @Override
            public Optional<Resource> get(String id) {
                return Optional.of(pipeline).filter(r -> r.id().equals(id));
            }

            @Override
            public List<Resource> list() {
                return List.of(pipeline);
            }
        });
        ObservationStore nothing = new ObservationStore() {
            @Override
            public void save(Observation observation) {
            }

            @Override
            public Optional<Observation> read(String pipelineId) {
                return Optional.empty();
            }

            @Override
            public void delete(String pipelineId) {
            }
        };
        StaticListableBeanFactory beans = new StaticListableBeanFactory();
        beans.addBean("inspector", inspector);
        if (trees != null) {
            beans.addBean("trees", trees);
        }
        return new NestController(artifacts, new PipelineObservationQueryService(artifacts, nothing),
                beans.getBeanProvider(NestStateInspector.class), beans.getBeanProvider(NestTreeReader.class),
                beans.getBeanProvider(DataBrowserService.class), MessageCatalog.bundled());
    }
}
