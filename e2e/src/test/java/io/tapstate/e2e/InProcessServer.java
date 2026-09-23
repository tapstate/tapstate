package io.tapstate.e2e;

import io.tapstate.app.Bootstrap;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.scheduling.config.ScheduledTask;
import org.springframework.scheduling.config.ScheduledTaskHolder;

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

    /**
     * The scheduled pass that republishes every pipeline's observation, as the scheduler renders it:
     * a scheduled method's string form is its declaring class and method name.
     */
    private static final String PUBLISHING_PASS = "io.tapstate.app.ConvergenceDriver.reconcile";

    private final ConfigurableApplicationContext context;
    private final URI baseUrl;

    private InProcessServer(ConfigurableApplicationContext context, URI baseUrl) {
        this.context = context;
        this.baseUrl = baseUrl;
    }

    /** Boots the assembly against the given store and returns once its surface is listening. */
    static InProcessServer start(String storeUri) {
        return start(storeUri, SharedMongo.OPERATOR_STATE_DATABASE);
    }

    /** Boots the assembly with an explicit operator-state database. */
    static InProcessServer start(String storeUri, String operatorStateDatabase) {
        ConfigurableApplicationContext context = new SpringApplicationBuilder(Bootstrap.class)
                .properties(
                        "tapstate.store.mongo.enabled=true",
                        "tapstate.store.mongo.uri=" + storeUri,
                        ServerHandle.OPERATOR_STATE_DATABASE_SETTING + "=" + operatorStateDatabase,
                        // The container speaks plaintext; store TLS is opt-in, so no flag is needed.
                        "tapstate.store.mongo.server-selection-timeout=5s",
                        // This tier's working directory is the harness's own module, and the setting's
                        // default is relative to it.
                        ServerHandle.PLUGINS_DIRECTORY_SETTING + "=" + ServerHandle.privateStagingDirectory(),
                        ServerHandle.ALSO_ACCEPT_IDS_SETTING + "=" + E2eConnectorJar.CONNECTOR_ID)
                // Port zero, then read back what was granted: a hard-coded port turns a busy machine
                // into a flaky suite. Both are command-line arguments rather than default properties
                // because the product's application configuration publishes 8080 as its default, and a
                // default property is the lowest-ranked source Spring has.
                //
                // The address is half of it. A free port alone binds the wildcard, and a wildcard bind
                // does not reserve 127.0.0.1:<port>: the allocator hands one out even when another local
                // process already holds that port on the loopback alone, and a connection to the loopback
                // is then routed to that more specific holder. The server would come up, report the port
                // it was granted, and receive none of the requests -- every route answering whatever bare
                // status the stranger returns. Binding the loopback makes the collision impossible,
                // because the allocator will not hand out a loopback port that is already taken.
                .run("--server.address=127.0.0.1", "--server.port=0");
        int port = ((WebServerApplicationContext) context).getWebServer().getPort();
        // The literal address, not the name: "localhost" resolves to both 127.0.0.1 and ::1, and only
        // one of those is the address bound above.
        return new InProcessServer(context, URI.create("http://127.0.0.1:" + port));
    }

    @Override
    public URI baseUrl() {
        return baseUrl;
    }

    /**
     * Stops this server publishing observations, while leaving everything that answers a read alone.
     *
     * <p>The situation a reader has to be able to tell apart from a healthy run is one where the thing
     * that keeps observations current has stopped and the surface serving them has not. Nothing in the
     * product separates those two: there is one supported role and it runs both, and the store backs
     * both, so making the store unreachable takes the read face down with the publisher and leaves
     * nothing to read. Cancelling the scheduled task is the only way to produce the situation without
     * inventing a switch in the product for a test to flip.
     *
     * <p>In this tier only, and deliberately so -- it reaches inside the running application, which is
     * exactly what the tier boundary exists to keep out of {@link ServerHandle}. A case that needs this
     * is a case about the server's internals, and it names this tier rather than pretending to be
     * portable.
     *
     * <p>The pass is matched by name because it is not visible from here, and the count returned is what
     * makes that safe: rename or move it and this cancels nothing, so a caller that insists on exactly one
     * gets a red rather than a case that keeps passing while measuring a publisher which never stopped.
     * Without that insistence the two are the same green -- measured, not imagined: the first version of
     * this matched on the runnable's type, the scheduler wraps it, and the cancel found nothing.
     *
     * @return how many scheduled tasks were cancelled; a caller must refuse anything but what it expects
     */
    int stopPublishingObservations() {
        int cancelled = 0;
        for (ScheduledTaskHolder holder : context.getBeansOfType(ScheduledTaskHolder.class).values()) {
            for (ScheduledTask task : holder.getScheduledTasks()) {
                // The runnable itself is wrapped by the scheduler, so it is matched by how it renders
                // rather than by its type: the wrapper delegates toString to the scheduled method, which
                // renders as its declaring class and method name.
                if (PUBLISHING_PASS.equals(String.valueOf(task.getTask().getRunnable()))) {
                    task.cancel();
                    cancelled++;
                }
            }
        }
        return cancelled;
    }

    @Override
    public void close() {
        context.close();
    }
}
