package io.tapstate.e2e;

import io.tapstate.app.Bootstrap;
import io.tapstate.app.IsolatedConnectionDiagnostics;
import io.tapstate.app.ObservationWorkerFixture;
import io.tapstate.testsupport.MongoRenewedTopology;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.boot.web.server.context.WebServerInitializedEvent;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.env.MapPropertySource;
import org.springframework.scheduling.config.ScheduledTask;
import org.springframework.scheduling.config.ScheduledTaskHolder;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

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
    private static final String PUBLISHING_PASS = "io.tapstate.app.ConvergenceDriver.schedule";

    private final ConfigurableApplicationContext context;
    private final URI baseUrl;
    private final Path stagingDirectory;
    private final List<ConfigurableApplicationContext> refreshedContexts;

    private InProcessServer(ConfigurableApplicationContext context, URI baseUrl, Path stagingDirectory,
            List<ConfigurableApplicationContext> refreshedContexts) {
        this.context = context;
        this.refreshedContexts = refreshedContexts;
        this.baseUrl = baseUrl;
        this.stagingDirectory = stagingDirectory;
    }

    void inspectStorageNow() throws ReflectiveOperationException {
        Class<?> rootType = Class.forName("io.tapstate.app.StorageRoot");
        var root = context.getBean(rootType);
        var runtimeMethod = rootType.getDeclaredMethod("runtime");
        runtimeMethod.setAccessible(true);
        var runtime = runtimeMethod.invoke(root);
        var businessMethod = runtime.getClass().getDeclaredMethod("business");
        businessMethod.setAccessible(true);
        var business = ((java.util.Optional<?>) businessMethod.invoke(runtime)).orElseThrow();
        var field = business.getClass().getDeclaredField("storageInspection");
        field.setAccessible(true);
        var inspection = field.get(business);
        var run = inspection.getClass().getDeclaredMethod("runOnce");
        run.setAccessible(true);
        run.invoke(inspection);
    }

    /** Boots the assembly against the given store and returns once its surface is listening. */
    static InProcessServer start(String storeUri) {
        return start(storeUri, SharedMongo.OPERATOR_STATE_DATABASE);
    }

    static InProcessServer startPrepared(PreparedNativeStore storage) {
        return startPrepared(storage, List.of());
    }

    static InProcessServer startPrepared(PreparedNativeStore storage, List<String> additionalArguments) {
        return startPrepared(storage, additionalArguments, null);
    }
    static InProcessServer startPrepared(PreparedNativeStore storage, List<String> additionalArguments,
            io.tapstate.control.core.AiProviderClient provider) {
        return startPrepared(storage, additionalArguments, provider, null);
    }

    static InProcessServer startWithConnectionDiagnostics(PreparedNativeStore storage) {
        return startPrepared(storage, List.of(), null, new IsolatedConnectionDiagnostics(storage.clusterId(), storage.cacheDatabase()));
    }

    private static InProcessServer startPrepared(PreparedNativeStore storage, List<String> additionalArguments,
            io.tapstate.control.core.AiProviderClient provider, IsolatedConnectionDiagnostics diagnostics) {
        var arguments = new java.util.ArrayList<String>();
        for (String value : List.of("--tapstate.cluster.id=" + storage.clusterId(),
                "--tapstate.hz.member-port=0", "--tapstate.hz.jet.cooperative-thread-count=2")) {
            String key = value.substring(0, value.indexOf('=') + 1);
            if (additionalArguments.stream().noneMatch(argument -> argument.startsWith(key))) arguments.add(value);
        }
        arguments.addAll(additionalArguments);
        return start(storage.configurationUri(), null, arguments, provider, diagnostics);
    }

    /** Boots the assembly with an explicit operator-state database. */
    static InProcessServer start(String storeUri, String operatorStateDatabase) {
        return start(storeUri, operatorStateDatabase, List.of());
    }

    /** Additional settings let a focused witness select the actual Cloud assembly. */
    static InProcessServer start(String storeUri, String operatorStateDatabase, List<String> additionalArguments) {
        return start(storeUri, operatorStateDatabase, additionalArguments, null);
    }

    static InProcessServer start(String storeUri, String operatorStateDatabase, List<String> additionalArguments,
            io.tapstate.control.core.AiProviderClient provider) {
        return start(storeUri, operatorStateDatabase, additionalArguments, provider, null);
    }

    private static InProcessServer start(String storeUri, String operatorStateDatabase, List<String> additionalArguments,
            io.tapstate.control.core.AiProviderClient provider, IsolatedConnectionDiagnostics diagnostics) {
        Path stagingDirectory = ServerHandle.privateStagingDirectory();
        ConfigurableApplicationContext context;
        var refreshed = new java.util.concurrent.CopyOnWriteArrayList<ConfigurableApplicationContext>();
        try {
            context = boot(storeUri, operatorStateDatabase, stagingDirectory, additionalArguments, refreshed, provider, diagnostics);
        } catch (RuntimeException e) {
            // Nothing is handed back to close, so nothing else will ever clear this up. Attached rather
            // than thrown: why the assembly did not boot is the thing a reader needs.
            try {
                ServerHandle.discardStagingDirectory(stagingDirectory);
            } catch (RuntimeException cleanup) {
                e.addSuppressed(cleanup);
            }
            throw e;
        }
        int port = ((WebServerApplicationContext) context).getWebServer().getPort();
        String address = context.getEnvironment().getProperty("server.address", "127.0.0.1");
        URI endpoint = UriComponentsBuilder.newInstance().scheme("http").host(address).port(port).build().toUri();
        return new InProcessServer(context, endpoint, stagingDirectory, refreshed);
    }

    private static ConfigurableApplicationContext boot(String storeUri, String operatorStateDatabase,
            Path stagingDirectory, List<String> additionalArguments,
            java.util.concurrent.CopyOnWriteArrayList<ConfigurableApplicationContext> refreshedContexts,
            io.tapstate.control.core.AiProviderClient provider, IsolatedConnectionDiagnostics diagnostics) {
        var arguments = new java.util.ArrayList<>(additionalArguments);
        if (arguments.stream().noneMatch(value -> value.startsWith("--server.address="))) {
            arguments.add("--server.address=127.0.0.1");
        }
        if (arguments.stream().noneMatch(value -> value.startsWith("--server.port="))) {
            arguments.add("--server.port=0");
        }
        var defaults = new java.util.ArrayList<>(List.of(
                        "tapstate.store.mongo.enabled=true",
                        "tapstate.store.mongo.uri=" + storeUri,
                        // The container speaks plaintext; store TLS is opt-in, so no flag is needed.
                        "tapstate.store.mongo.server-selection-timeout=5s",
                        // This tier's working directory is the harness's own module, and the setting's
                        // default is relative to it.
                        ServerHandle.PLUGINS_DIRECTORY_SETTING + "=" + stagingDirectory,
                        ServerHandle.ALSO_ACCEPT_IDS_SETTING + "=" + E2eConnectorJar.CONNECTOR_ID));
        if (operatorStateDatabase != null) defaults.add(ServerHandle.OPERATOR_STATE_DATABASE_SETTING + "=" + operatorStateDatabase);
        var builder = new SpringApplicationBuilder(Bootstrap.class);
        String advertiseSetting = "--" + MongoRenewedTopology.CONTROL_ADVERTISE_URL + "=";
        if (arguments.stream().anyMatch(argument -> argument.startsWith(advertiseSetting) && argument.endsWith(":0"))) {
            builder.listeners((ApplicationListener<WebServerInitializedEvent>) event -> {
                var environment = ((ConfigurableApplicationContext) event.getApplicationContext()).getEnvironment();
                var url = MongoRenewedTopology.controlUrl(environment.getRequiredProperty("server.address"),
                        event.getWebServer().getPort());
                environment.getPropertySources().addFirst(new MapPropertySource("fixtureControlEndpoint",
                        Map.of(MongoRenewedTopology.CONTROL_ADVERTISE_URL, url.toString())));
            });
        }
        if (diagnostics != null) {
            builder.initializers(context -> context.getBeanFactory().registerSingleton("isolatedConnectionDiagnostics", diagnostics));
        }
        if (provider != null) {
            builder.initializers(context -> context.addBeanFactoryPostProcessor(factory -> {
                var registry = (org.springframework.beans.factory.support.BeanDefinitionRegistry) factory;
                if (registry.containsBeanDefinition("aiProviderClient")) registry.removeBeanDefinition("aiProviderClient");
                var definition = new org.springframework.beans.factory.support.RootBeanDefinition(
                        io.tapstate.control.core.AiProviderClient.class, () -> provider);
                definition.setPrimary(true);
                if (provider instanceof AutoCloseable) definition.setDestroyMethodName("close");
                registry.registerBeanDefinition("aiProviderClient", definition);
            }));
        }
        return builder
                .listeners((ApplicationListener<ContextRefreshedEvent>) event -> {
                    if (event.getApplicationContext() instanceof ConfigurableApplicationContext refreshed)
                        refreshedContexts.addIfAbsent(refreshed);
                })
                .properties(defaults.toArray(String[]::new))
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
                .run(arguments.toArray(String[]::new));
    }

    @Override
    public URI baseUrl() {
        return baseUrl;
    }

    Object nativeLifecycleActuator() {
        return refreshedContexts.stream()
                .filter(owner -> owner.isActive() && belongsToThisServer(owner)
                        && owner.getBeanFactory().containsLocalBean("lifecycleActuator"))
                .findFirst().orElseThrow().getBean("lifecycleActuator");
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
    private final java.util.List<Runnable> stoppedPublishers=new java.util.concurrent.CopyOnWriteArrayList<>();
    int publishObservationsOnce(){stoppedPublishers.forEach(Runnable::run);return stoppedPublishers.size();}
    int stopPublishingObservations() {
        int cancelled = 0;
        for (var owner : refreshedContexts) {
            if (!owner.isActive() || !belongsToThisServer(owner)) continue;
            for (ScheduledTaskHolder holder : owner.getBeansOfType(ScheduledTaskHolder.class).values()) {
                for (ScheduledTask task : holder.getScheduledTasks()) {
                    // The runnable is wrapped by the scheduler; its rendering names the scheduled method.
                    if (PUBLISHING_PASS.equals(String.valueOf(task.getTask().getRunnable()))) {
                        Runnable scheduled = task.getTask().getRunnable();
                        Object driver = owner.getBean("convergenceDriver");
                        stoppedPublishers.add(synchronousPublishingPass(driver));
                        task.cancel();
                        awaitPublishingIdle(driver);
                        cancelled++;
                    }
                }
            }
        }
        return cancelled;
    }

    /** Stops future dispatch and waits for in-flight writes before a test plants its own observation. */
    int stopPublishingObservationsAndAwaitWorkers() {
        int cancelled = stopPublishingObservations();
        ObservationWorkerFixture.stopAndAwait(refreshedContexts.stream()
                .filter(owner -> owner.isActive() && belongsToThisServer(owner)).toList());
        return cancelled;
    }

    void awaitObservationWorkersIdle() {
        ObservationWorkerFixture.awaitIdle(refreshedContexts.stream()
                .filter(owner -> owner.isActive() && belongsToThisServer(owner)).toList());
    }

    /** The real scheduled pass, for a case that controls its storage interleaving. */
    Runnable publishingObservations() {
        var passes = new java.util.ArrayList<Object>();
        for (var owner : refreshedContexts) {
            if (!owner.isActive() || !belongsToThisServer(owner)) continue;
            for (ScheduledTaskHolder holder : owner.getBeansOfType(ScheduledTaskHolder.class).values()) {
                for (ScheduledTask task : holder.getScheduledTasks()) {
                    if (PUBLISHING_PASS.equals(String.valueOf(task.getTask().getRunnable())))
                        passes.add(owner.getBean("convergenceDriver"));
                }
            }
        }
        if (passes.size() != 1) throw new AssertionError("expected one native convergence pass: " + passes.size());
        return synchronousPublishingPass(passes.getFirst());
    }

    /** The real publisher in this server's scheduled business context, for a scoped read barrier. */
    Object observationPublisher() {
        var publishers = new java.util.ArrayList<Object>();
        for (var owner : refreshedContexts) {
            if (!owner.isActive() || !belongsToThisServer(owner)) continue;
            for (ScheduledTaskHolder holder : owner.getBeansOfType(ScheduledTaskHolder.class).values())
                for (ScheduledTask task : holder.getScheduledTasks())
                    if (PUBLISHING_PASS.equals(String.valueOf(task.getTask().getRunnable())))
                        publishers.add(owner.getBean("observationPublisher"));
        }
        if (publishers.size() != 1) throw new AssertionError("expected one native publisher: " + publishers.size());
        return publishers.getFirst();
    }

    /** Test-only mutation access; no production setting admits a serial worker pool. */
    java.util.concurrent.ThreadPoolExecutor convergenceWorkers() {
        for (var owner : refreshedContexts) {
            if (!owner.isActive() || !belongsToThisServer(owner)) continue;
            for (ScheduledTaskHolder holder : owner.getBeansOfType(ScheduledTaskHolder.class).values())
                for (ScheduledTask task : holder.getScheduledTasks())
                    if (PUBLISHING_PASS.equals(String.valueOf(task.getTask().getRunnable())))
                        return (java.util.concurrent.ThreadPoolExecutor) field(field(owner.getBean("convergenceDriver"),"passes"),"workers");
        }
        throw new AssertionError("no convergence worker pool belongs to this server");
    }

    private static Runnable synchronousPublishingPass(Object driver) {
        var method = org.springframework.util.ReflectionUtils.findMethod(driver.getClass(),"reconcile");
        if (method == null) throw new AssertionError("the native synchronous pass is missing");
        org.springframework.util.ReflectionUtils.makeAccessible(method);
        return () -> {
            awaitPublishingIdle(driver);
            org.springframework.util.ReflectionUtils.invokeMethod(method,driver);
        };
    }

    private static void awaitPublishingIdle(Object driver) {
        var scanning = (java.util.concurrent.atomic.AtomicBoolean) field(driver,"scanning");
        var active = (java.util.Set<?>) field(field(driver,"passes"),"active");
        Await.until("all cancelled publishing work to finish",java.time.Duration.ofSeconds(30),
                () -> !scanning.get() && active.isEmpty(),() -> "active Pipeline passes=" + active);
    }

    private static Object field(Object owner,String name) {
        var field = org.springframework.util.ReflectionUtils.findField(owner.getClass(),name);
        if (field == null) throw new AssertionError("missing native field " + name);
        org.springframework.util.ReflectionUtils.makeAccessible(field);
        return org.springframework.util.ReflectionUtils.getField(field,owner);
    }

    private boolean belongsToThisServer(ConfigurableApplicationContext candidate) {
        for (org.springframework.context.ApplicationContext owner = candidate; owner != null; owner = owner.getParent())
            if (owner == context) return true;
        return false;
    }

    /** Observe the real selected guard on its executing thread; the original check always runs. */
    AutoCloseable observeManagedAuthorization(java.util.function.Consumer<String> before,
            java.util.function.BiConsumer<String, java.util.Map<String, Object>> after) {
        String name = "managedAuthenticationContexts";
        var beans = (org.springframework.beans.factory.support.DefaultListableBeanFactory) context.getBeanFactory();
        var original = beans.getBean(name, io.tapstate.control.core.ManagedAuthenticationContext.Provider.class);
        io.tapstate.control.core.ManagedAuthenticationContext.Provider observed = () -> {
            var selected = original.current();
            if (selected == null) return null;
            return new io.tapstate.control.core.ManagedAuthenticationContext(selected.service(), () -> {
                try (var measurement = io.tapstate.adapters.mongostore.MongoHttpDiagnosticsScope.open()) {
                    String thread = Thread.currentThread().getName();
                    before.accept(thread);
                    selected.requireActive().run();
                    after.accept(thread, measurement.snapshot());
                }
            });
        };
        beans.destroySingleton(name);
        beans.registerSingleton(name, observed);
        return () -> {
            if (beans.getSingleton(name) != observed) throw new AssertionError("the observed context provider changed");
            beans.destroySingleton(name);
            beans.registerSingleton(name, original);
        };
    }

    @Override
    public void close() {
        try { context.close(); }
        finally {
            refreshedContexts.clear();
            ServerHandle.discardStagingDirectory(stagingDirectory);
        }
    }
}
