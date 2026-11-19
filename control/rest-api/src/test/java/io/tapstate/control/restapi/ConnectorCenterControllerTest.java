package io.tapstate.control.restapi;

import io.tapstate.control.core.AuditGate;
import io.tapstate.control.core.ConnectorCenter;
import io.tapstate.control.core.ConnectorCenterService;
import io.tapstate.control.core.ConnectorUsageQuery;
import io.tapstate.control.core.ControlError;
import io.tapstate.control.core.CredentialType;
import io.tapstate.control.core.Scope;
import io.tapstate.control.core.TapstatePrincipal;
import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.mock.web.MockServletContext;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class ConnectorCenterControllerTest {
    @AfterEach
    void clearAuthentication() {
        SecurityContextHolder.clearContext();
    }

    @Test
    void mountsTheCenterAndRawUploadWithOperationProgressUnderTheApiPrefix() throws Exception {
        SecurityContextHolder.getContext().setAuthentication(UsernamePasswordAuthenticationToken.authenticated(
                new TapstatePrincipal("admin", CredentialType.HUMAN_JWT, Set.of(Scope.ADMIN, Scope.READ), Optional.empty()),
                "", List.of()));
        try (var context = new AnnotationConfigWebApplicationContext()) {
            context.setServletContext(new MockServletContext());
            context.register(Fixture.class);
            context.refresh();
            var mvc = MockMvcBuilders.webAppContextSetup(context).build();
            var center = context.getBean(CapturedCenter.class);

            assertThat(mvc.perform(get("/api/connector-center")).andExpect(status().isOk())
                    .andReturn().getResponse().getContentAsString()).contains("mongodb");
            mvc.perform(post("/api/connector-center:refresh")).andExpect(status().isOk());
            mvc.perform(post("/api/connector-center/mongodb:download")).andExpect(status().isOk());
            mvc.perform(get("/api/connector-center/operations/original-operation")).andExpect(status().isOk());
            String hash = "a".repeat(64);
            String usage = mvc.perform(get("/api/connector-center/mongodb/versions/" + hash + "/usages")
                    .param("search", "Reader").param("page", "2").param("pageSize", "10"))
                    .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
            assertThat(usage).contains("reader_10", hash, "\"total\":11");
            assertThatThrownBy(() -> mvc.perform(get("/api/connector-center/mongodb/versions/invalid/usages")))
                    .hasCauseInstanceOf(TapstateException.class)
                    .satisfies(failure -> assertThat(((TapstateException) failure.getCause()).code())
                            .isEqualTo(ControlError.MALFORMED_REQUEST));
            byte[] artifact = new byte[]{1, 2, 3, 4};
            mvc.perform(post("/api/connector-center:upload").contentType("application/java-archive").content(artifact))
                    .andExpect(status().isOk());

            assertThat(center.expectedConnectorId).isNull();
            mvc.perform(post("/api/connector-center:upload").param("connectorId", "mysql")
                    .contentType("application/java-archive").content(artifact)).andExpect(status().isOk());
            assertThat(center.expectedConnectorId).isEqualTo("mysql");
            assertThat(center.uploaded).isEqualTo(artifact);
            assertThat(center.length).isEqualTo(artifact.length);
            assertThat(center.download).isEqualTo("mongodb");
            assertThat(center.operation).isEqualTo("original-operation");
        }
    }

    @Test
    void aLegacyAssemblyWithoutTheCenterReportsItsCodedUnavailableState() {
        var services = new DefaultListableBeanFactory().getBeanProvider(ConnectorCenterService.class);
        var controller = new ConnectorCenterController(services);
        assertThatThrownBy(controller::list).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(ControlError.UNREACHABLE));
    }

    @Configuration(proxyBeanMethods = false)
    @EnableWebMvc
    @Import({RestApiConfiguration.class, ConnectorCenterController.class})
    static class Fixture {
        @Bean CapturedCenter center() { return new CapturedCenter(); }
        @Bean ConnectorUsageQuery usage() {
            return new ConnectorUsageQuery() {
                @Override public List<Version> forPipeline(String id) { return List.of(); }
                @Override public Page usages(String id, String sha256, String search, int page, int pageSize) {
                    assertThat(id).isEqualTo("mongodb"); assertThat(search).isEqualTo("Reader");
                    assertThat(page).isEqualTo(2); assertThat(pageSize).isEqualTo(10);
                    return new Page(page, pageSize, 11, List.of(new Pipeline("reader_10", "Reader", List.of(
                            new Version("input", io.tapstate.core.model.PipelineNode.Purpose.CAPTURE,
                                    id, "1.0", sha256, "capture", "chain")))));
                }
            };
        }
        @Bean ConnectorCenterService connectorCenterService(CapturedCenter center) {
            return new ConnectorCenterService(center, new AuditGate(record -> { }, Clock.systemUTC()));
        }
    }

    static final class CapturedCenter implements ConnectorCenter {
        private byte[] uploaded;
        private long length;
        private String expectedConnectorId;
        private String download;
        private String operation;

        @Override public Map<String, Object> catalog() { return Map.of("connectors", List.of("mongodb")); }
        @Override public Map<String, Object> refresh() { return Map.of("stage", "refreshed"); }
        @Override public Map<String, Object> download(String id) { download = id; return Map.of("id", "original-operation"); }
        @Override public Map<String, Object> operation(String id) { operation = id; return Map.of("stage", "installing"); }
        @Override public Map<String, Object> upload(InputStream artifact, long contentLength, String expectedConnectorId) {
            try {
                uploaded = artifact.readAllBytes();
            } catch (IOException failure) {
                throw new UncheckedIOException(failure);
            }
            length = contentLength;
            this.expectedConnectorId = expectedConnectorId;
            return Map.of("id", "uploaded-operation");
        }
    }
}
