package io.tapstate.control.restapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import io.tapstate.control.core.AggregateStepChecks;
import io.tapstate.control.core.Scope;
import io.tapstate.control.core.TapstatePrincipal;
import io.tapstate.control.core.VerifiedToken;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockServletContext;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import tools.jackson.databind.json.JsonMapper;

class AggregateStepControllerTest {

    private static final List<String> ASKED = new ArrayList<>();

    @Test
    @DisplayName("a check with no body asks for the first page of the step's groups and answers with what was found")
    void aCheckWithNoBodyWalksFromTheFirstGroup() throws Exception {
        try (var context = context()) {
            MockMvc mvc = MockMvcBuilders.webAppContextSetup(context).build();
            ASKED.clear();

            String response = mvc.perform(post("/api/pipelines/sales/aggregates/by_region:check"))
                    .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();

            assertThat(ASKED).containsExactly("check sales by_region group=null after=null");
            Map<?, ?> body = JsonMapper.builder().build().readValue(response, Map.class);
            assertThat(body.get("pipeline")).isEqualTo("sales");
            assertThat(body.get("step")).isEqualTo("by_region");
            assertThat(body.get("repaired")).isEqualTo(false);
            assertThat(body.get("next")).isEqualTo("cursor-2");
            Map<?, ?> finding = (Map<?, ?>) ((List<?>) body.get("findings")).getFirst();
            assertThat(finding.get("verdict")).isEqualTo("mismatch");
            assertThat(finding.get("fields")).isEqualTo(List.of("revenue"));
            Map<String, Object> group = new LinkedHashMap<>();
            group.put("region", "EU");
            group.put("day", null);
            assertThat(finding.get("group")).as("a null key value stays a null").isEqualTo(group);
        }
    }

    @Test
    @DisplayName("a repair names its group and where to go on from in the body, and is made as the caller")
    void aRepairCarriesItsGroup() throws Exception {
        try (var context = context()) {
            MockMvc mvc = MockMvcBuilders.webAppContextSetup(context).build();
            ASKED.clear();

            SecurityContextHolder.getContext().setAuthentication(UsernamePasswordAuthenticationToken.authenticated(
                    TapstatePrincipal.humanJwt(new VerifiedToken("operator", Scope.WRITE)), "unused", List.of()));
            try {
                mvc.perform(post("/api/pipelines/sales/aggregates/by_region:repair")
                                .contentType(MediaType.APPLICATION_JSON)
                                .content("{\"group\":{\"region\":\"EU\",\"day\":null},\"after\":\"cursor-1\"}"))
                        .andExpect(status().isOk());
            } finally {
                SecurityContextHolder.clearContext();
            }

            assertThat(ASKED).containsExactly(
                    "repair sales by_region group={region=EU, day=null} after=cursor-1 as operator");
        }
    }

    private static AnnotationConfigWebApplicationContext context() {
        var context = new AnnotationConfigWebApplicationContext();
        context.setServletContext(new MockServletContext());
        context.register(TestConfiguration.class);
        context.refresh();
        return context;
    }

    @Configuration(proxyBeanMethods = false)
    @EnableWebMvc
    @Import({RestApiConfiguration.class, AggregateStepController.class})
    static class TestConfiguration {

        @Bean
        AggregateStepChecks aggregateStepChecks() {
            return new AggregateStepChecks() {

                @Override
                public Report check(String pipelineId, String step, Request request) {
                    ASKED.add("check " + pipelineId + " " + step + " group=" + request.group() + " after=" + request.after());
                    return report(pipelineId, step, false);
                }

                @Override
                public Report repair(String principal, String pipelineId, String step, Request request) {
                    ASKED.add("repair " + pipelineId + " " + step + " group=" + request.group() + " after="
                            + request.after() + " as " + principal);
                    return report(pipelineId, step, true);
                }
            };
        }

        private static AggregateStepChecks.Report report(String pipeline, String step, boolean repaired) {
            Map<String, String> group = new LinkedHashMap<>();
            group.put("region", "EU");
            group.put("day", null);
            return new AggregateStepChecks.Report(pipeline, step, repaired, 2,
                    List.of(new AggregateStepChecks.Finding(group, "mismatch", List.of("revenue"), null, null)),
                    "cursor-2", 0, 0);
        }
    }
}
