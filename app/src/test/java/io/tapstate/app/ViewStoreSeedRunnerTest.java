package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * The managed state store is registered by the deployment at startup, not applied by whoever is using it.
 */
class ViewStoreSeedRunnerTest {

    @Test
    void the_store_is_there_without_anyone_applying_it() {
        InMemoryStorePort store = new InMemoryStorePort();

        new ViewStoreSeedRunner(store.artifacts(), "mongodb://mongo:27017/tapstate", null).seed();

        Resource seeded = store.artifacts().get(ViewTargetResolver.STATE_STORE_SOURCE_ID).orElseThrow();
        assertThat(seeded).isInstanceOf(SourceResource.class);
        SourceResource source = (SourceResource) seeded;
        assertThat(source.connector()).isEqualTo("mongodb");
        // A connection supplier, not something to read: a resource under this id that declares capture
        // settings is refused as an authored source, so seeding one would break the deployment it serves.
        assertThat(source.mode()).as("no read mode").isNull();
        assertThat(source.tables()).as("no tables to read").isNull();
    }

    @Test
    void an_author_who_declared_their_own_store_keeps_it() {
        // The seed runs on every boot, so overwriting would silently undo a deliberate change on restart --
        // the kind of loss whose cause is a week away from its effect.
        InMemoryStorePort store = new InMemoryStorePort();
        SourceResource mine = new SourceResource(
                ViewTargetResolver.STATE_STORE_SOURCE_ID, null, "mongodb",
                java.util.Map.of("isUri", true, "uri", "mongodb://elsewhere:27017/mine"),
                null, null, null, null);
        store.artifacts().create(mine);

        new ViewStoreSeedRunner(store.artifacts(), "mongodb://mongo:27017/tapstate", null).seed();

        SourceResource kept = (SourceResource)
                store.artifacts().get(ViewTargetResolver.STATE_STORE_SOURCE_ID).orElseThrow();
        assertThat(kept.config().get("uri")).isEqualTo("mongodb://elsewhere:27017/mine");
    }

    @Test
    void the_views_database_is_its_own_but_reached_the_same_way() {
        // Credentials, host list and options are what it took to reach the server at all, so dropping any
        // of them would leave a URI that addresses the right database on a server it can no longer log in
        // to. And carrying them is not enough on its own: with no explicit authSource the spec
        // authenticates against the database in the URI, so rewriting the path alone moves the
        // authentication database as well, and a user defined in the control-plane database does not
        // exist in the derived one. This assertion used to say the credentials survive without ever
        // asking whether they would still work.
        assertThat(ViewStoreSeedRunner.viewsUri("mongodb://mongo:27017/tapstate"))
                .isEqualTo("mongodb://mongo:27017/views?authSource=tapstate");
        assertThat(ViewStoreSeedRunner.viewsUri("mongodb://mongo:27017/tapstate?directConnection=true"))
                .as("options are carried, not dropped")
                .isEqualTo("mongodb://mongo:27017/views?directConnection=true&authSource=tapstate");
        assertThat(ViewStoreSeedRunner.viewsUri("mongodb://user:pw@a:27017,b:27017/tapstate?replicaSet=rs0"))
                .as("credentials, every member of the set, and the database they authenticate against")
                .isEqualTo("mongodb://user:pw@a:27017,b:27017/views?replicaSet=rs0&authSource=tapstate");
        assertThat(ViewStoreSeedRunner.viewsUri("mongodb://user:pw@h:27017/tapstate?authSource=admin"))
                .as("an authSource the deployment set is left exactly as it is")
                .isEqualTo("mongodb://user:pw@h:27017/views?authSource=admin");
        assertThat(ViewStoreSeedRunner.viewsUri("mongodb://user:pw@h:27017/tapstate?authsource=admin"))
                .as("option names are case-insensitive in a connection string, so this one counts too")
                .isEqualTo("mongodb://user:pw@h:27017/views?authsource=admin");
        assertThat(ViewStoreSeedRunner.viewsUri("mongodb://user:pw@h:27017/tapstate?appName=authSource=x"))
                .as("the characters inside another option's value are not an authSource anybody set")
                .isEqualTo("mongodb://user:pw@h:27017/views?appName=authSource=x&authSource=tapstate");
        assertThat(ViewStoreSeedRunner.viewsUri("mongodb://mongo:27017"))
                .as("a URI that names no database still gets one, and has no default to preserve")
                .isEqualTo("mongodb://mongo:27017/views");
        assertThat(ViewStoreSeedRunner.viewsUri("mongodb://mongo:27017?directConnection=true"))
                .as("no database, but options")
                .isEqualTo("mongodb://mongo:27017/views?directConnection=true");
    }

    @Test
    void a_uri_that_names_no_database_still_had_somewhere_it_authenticated() {
        // The spec's fallback is admin, and it is a fallback the derived URI does not inherit: the
        // original named no database, the derived one names views, so leaving it implicit moves
        // authentication to the view store exactly as rewriting a named path would. Only where
        // credentials exist at all -- there is nothing to preserve for a URI that authenticates nowhere.
        assertThat(ViewStoreSeedRunner.viewsUri("mongodb://user:pw@mongo:27017"))
                .isEqualTo("mongodb://user:pw@mongo:27017/views?authSource=admin");
        assertThat(ViewStoreSeedRunner.viewsUri("mongodb://user:pw@mongo:27017?replicaSet=rs0"))
                .as("no database, but options")
                .isEqualTo("mongodb://user:pw@mongo:27017/views?replicaSet=rs0&authSource=admin");
    }

    @Test
    void a_store_reached_over_tls_with_a_private_ca_is_not_seeded_from_the_uri_alone() {
        // The trust that lets the server reach its own store is a CA file beside the URI, and it cannot
        // travel in a URI. Seeding anyway would register a store that can never be connected to, so the
        // deployment is left to declare one with the connector's own TLS settings.
        InMemoryStorePort store = new InMemoryStorePort();

        new ViewStoreSeedRunner(store.artifacts(), "mongodb://mongo:27017/tapstate?tls=true", "/etc/ca.pem")
                .seed();

        assertThat(store.artifacts().get(ViewTargetResolver.STATE_STORE_SOURCE_ID)).isEmpty();
    }

    @Test
    void a_ca_file_on_a_plaintext_connection_changes_nothing() {
        // The driver applies a CA file only when the URI asks for TLS, so an inert one must not turn a
        // working deployment into one with no store.
        InMemoryStorePort store = new InMemoryStorePort();

        new ViewStoreSeedRunner(store.artifacts(), "mongodb://mongo:27017/tapstate", "/etc/ca.pem").seed();

        assertThat(store.artifacts().get(ViewTargetResolver.STATE_STORE_SOURCE_ID)).isPresent();
    }

    @Test
    void the_store_is_registered_before_anything_in_the_start_phase_runs() {
        // Every readiness signal this product has is a poll of an HTTP endpoint, and that endpoint starts
        // answering when the web server's lifecycle is started -- the same phase the recorder below runs
        // in. Seeding after that point is a race the demo script loses by design: it waits for healthy
        // and then immediately applies a pipeline whose view needs this store to exist.
        //
        // So the assertion is about the phase rather than the value. A seed that runs as an application
        // runner satisfies "the store ends up registered" and still loses that race, because runners fire
        // after the refresh this recorder is part of.
        InMemoryStorePort store = new InMemoryStorePort();
        AtomicBoolean seededBeforeStart = new AtomicBoolean();

        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            context.registerBean("seed", ViewStoreSeedRunner.class, () ->
                    new ViewStoreSeedRunner(store.artifacts(), "mongodb://mongo:27017/tapstate", null));
            context.registerBean("surface", SurfaceStart.class, () -> new SurfaceStart(() ->
                    seededBeforeStart.set(store.artifacts()
                            .get(ViewTargetResolver.STATE_STORE_SOURCE_ID).isPresent())));
            context.refresh();
        }

        assertThat(seededBeforeStart.get())
                .as("the store was already registered when the start phase began")
                .isTrue();
    }

    /** Stands in for the web server: started in the same phase, so it observes the same moment. */
    private static final class SurfaceStart implements SmartLifecycle {

        private final Runnable onStart;
        private boolean running;

        SurfaceStart(Runnable onStart) {
            this.onStart = onStart;
        }

        @Override
        public void start() {
            onStart.run();
            running = true;
        }

        @Override
        public void stop() {
            running = false;
        }

        @Override
        public boolean isRunning() {
            return running;
        }
    }
}
