package io.tapstate.e2e;

import io.tapstate.app.Bootstrap;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;

import java.net.URI;

/**
 * The product booted inside the test JVM, from its real assembly root.
 *
 * <p>This is the fast tier, and the store is still real: the control plane's audit gate is built
 * from a live store connection, so a specification cannot be served by an in-memory stand-in without
 * ceasing to exercise the write verbs it is there to check. Speed comes from skipping the process
 * launch, not from skipping the product.
 */
final class InProcessServer implements ServerHandle {

    private final ConfigurableApplicationContext context;
    private final URI baseUrl;

    private InProcessServer(ConfigurableApplicationContext context, URI baseUrl) {
        this.context = context;
        this.baseUrl = baseUrl;
    }

    /** Boots the assembly against the given store and returns once its surface is listening. */
    static InProcessServer start(String storeUri) {
        ConfigurableApplicationContext context = new SpringApplicationBuilder(Bootstrap.class)
                .properties(
                        "tapstate.store.mongo.enabled=true",
                        "tapstate.store.mongo.uri=" + storeUri,
                        // The container speaks plaintext; store TLS is opt-in, so no flag is needed.
                        "tapstate.store.mongo.server-selection-timeout=5s",
                        // This tier's working directory is the harness's own module, and the setting's
                        // default is relative to it.
                        ServerHandle.PLUGINS_DIRECTORY_SETTING + "=" + ServerHandle.privateStagingDirectory(),
                        ServerHandle.ALSO_ACCEPT_IDS_SETTING + "=" + E2eConnectorJar.CONNECTOR_ID)
                // Port zero, then read back what was granted: a hard-coded port turns a busy machine
                // into a flaky suite. This is a command-line argument rather than a default property
                // because the product's application configuration publishes 8080 as its default.
                .run("--server.port=0");
        int port = ((WebServerApplicationContext) context).getWebServer().getPort();
        return new InProcessServer(context, URI.create("http://localhost:" + port));
    }

    @Override
    public URI baseUrl() {
        return baseUrl;
    }

    @Override
    public void close() {
        context.close();
    }
}
