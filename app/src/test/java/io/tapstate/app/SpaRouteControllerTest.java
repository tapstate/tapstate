package io.tapstate.app;

import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.forwardedUrl;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** The SPA fallback is narrow enough that it cannot turn an unknown server endpoint into HTML. */
class SpaRouteControllerTest {

    private final MockMvc mvc = MockMvcBuilders.standaloneSetup(new SpaRouteController()).build();

    @Test
    void browserEntryPointsAndClientDeepRoutesForwardToThePackagedIndex() throws Exception {
        mvc.perform(get("/"))
                .andExpect(status().isOk())
                .andExpect(forwardedUrl("/index.html"));
        mvc.perform(get("/pipelines/example/edit/logs"))
                .andExpect(status().isOk())
                .andExpect(forwardedUrl("/index.html"));
    }

    @Test
    void serverNamespacesAreNotSpaFallbackRoutes() throws Exception {
        mvc.perform(get("/api/not-found")).andExpect(status().isNotFound());
        mvc.perform(get("/auth/not-found")).andExpect(status().isNotFound());
        mvc.perform(get("/healthz")).andExpect(status().isNotFound());
        mvc.perform(get("/connector-icons/missing")).andExpect(status().isNotFound());
        mvc.perform(get("/assets/index.js")).andExpect(status().isNotFound());
    }
}
