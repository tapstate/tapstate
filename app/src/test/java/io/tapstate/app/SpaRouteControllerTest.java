package io.tapstate.app;

import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.head;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.forwardedUrl;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** The SPA fallback is narrow enough that it cannot turn an unknown server endpoint into HTML. */
class SpaRouteControllerTest {

    private final MockMvc mvc = MockMvcBuilders.standaloneSetup(new SpaRouteController()).build();

    @Test
    void browserEntryPointsAndClientDeepRoutesForwardToThePackagedIndex() throws Exception {
        for (String path : java.util.List.of("/", "/login", "/overview", "/views/orders",
                "/pipelines/example/edit/logs", "/future-client/route")) {
            mvc.perform(get(path).with(request -> {
                        request.setServletPath(path);
                        return request;
                    }).accept(MediaType.TEXT_HTML))
                    .andExpect(status().isOk())
                    .andExpect(forwardedUrl("/index.html"));
        }
        mvc.perform(head("/overview").servletPath("/overview").accept(MediaType.TEXT_HTML))
                .andExpect(status().isOk())
                .andExpect(forwardedUrl("/index.html"));
    }

    @Test
    void serverNamespacesAreNotSpaFallbackRoutes() throws Exception {
        for (String path : java.util.List.of("/api/not-found", "/auth/not-found", "/healthz",
                "/connector-icons/missing", "/assets/index.js", "/version", "/error")) {
            mvc.perform(get(path).servletPath(path).accept(MediaType.TEXT_HTML))
                    .andExpect(status().isNotFound());
        }
    }

    @Test
    void nonHtmlRequestsDoNotReceiveTheClientShell() throws Exception {
        mvc.perform(get("/overview").servletPath("/overview").accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isNotFound());
        mvc.perform(get("/overview").servletPath("/overview")
                        .header("Accept", "text/html;q=0, application/json"))
                .andExpect(status().isNotFound());
        mvc.perform(get("/overview").servletPath("/overview"))
                .andExpect(status().isNotFound());
    }
}
