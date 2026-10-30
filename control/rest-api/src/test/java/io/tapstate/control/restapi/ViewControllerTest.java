package io.tapstate.control.restapi;

import io.tapstate.control.core.ArtifactQueryService;
import io.tapstate.control.core.DataBrowserService;
import io.tapstate.control.core.PipelineObservationQueryService;
import io.tapstate.control.core.SourceSchemaQueryService;
import io.tapstate.control.core.ViewCatalogService;
import io.tapstate.spi.store.DataBrowserTableInfo;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.mock.web.MockServletContext;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class ViewControllerTest {

    @Test
    void listsTheSyncRenamedCollectionOnTheApiWire() throws Exception {
        try (var context = new AnnotationConfigWebApplicationContext()) {
            context.setServletContext(new MockServletContext());
            context.register(TestConfiguration.class);
            context.refresh();

            String response = MockMvcBuilders.webAppContextSetup(context).build()
                    .perform(get("/api/views"))
                    .andExpect(status().isOk())
                    .andReturn().getResponse().getContentAsString();

            Map<?, ?> body = JsonMapper.builder().build().readValue(response, Map.class);
            List<?> items = (List<?>) body.get("items");
            assertThat(items).hasSize(1);
            Map<?, ?> view = (Map<?, ?>) items.getFirst();
            assertThat(view.get("id")).isEqualTo("copy:sink:ACC_COMPOSITE");
            assertThat(view.get("location")).isEqualTo(Map.of(
                    "storeId", "atlas-store", "database", "tapstate_acc", "collection", "mysql_acc_composite_copy"));
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableWebMvc
    @Import({RestApiConfiguration.class, ViewController.class})
    static class TestConfiguration {

        @Bean
        ViewCatalogService viewCatalogService() {
            var store = new PipelineApiTest.FakeArtifactStore();
            store.seed("""
                    version: tapstate/v1
                    kind: source
                    id: mysql_source
                    connector: mysql
                    mode: cdc
                    tables: [ ACC_COMPOSITE ]
                    """);
            store.seed("""
                    version: tapstate/v1
                    kind: source
                    id: atlas-store
                    connector: mongodb
                    config: { uri: "mongodb://localhost:27017/tapstate_acc" }
                    """);
            store.seed("""
                    version: tapstate/v1
                    kind: pipeline
                    id: copy
                    source: mysql_source
                    serve:
                      from: ACC_COMPOSITE
                      sync:
                        - id: sink
                          source: atlas-store
                          rename: { case: lower, prefix: "mysql_", suffix: "_copy" }
                    """);
            var artifacts = new ArtifactQueryService(store);
            var schemas = new EmptySchemaStore();
            var browser = new DataBrowserService(store, schemas,
                    config -> List.of("mysql_acc_composite_copy"),
                    (config, collection) -> new DataBrowserTableInfo(7L, 42L, 6L),
                    (config, query) -> { throw new AssertionError("Listing views must not read rows"); },
                    (config, request, listener) -> { throw new AssertionError("Listing views must not follow rows"); });
            return new ViewCatalogService(artifacts,
                    new PipelineObservationQueryService(artifacts, new PipelineApiTest.TestApp().observationStore()),
                    browser, pipeline -> List.of(new io.tapstate.control.core.PipelineOutputNames.Output(
                            "sink", "replica", "atlas-store", "mysql_acc_composite_copy", "sink:ACC_COMPOSITE")));
        }
    }
}
