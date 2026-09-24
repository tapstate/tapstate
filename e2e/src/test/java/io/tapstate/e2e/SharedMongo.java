package io.tapstate.e2e;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.testsupport.DockerGate;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * One replica set for every specification in the JVM.
 *
 * <p>A container per test class costs a start-up each time and buys nothing: runs stay independent
 * by taking a database of their own, not a daemon of their own. Ryuk reaps the container when the
 * JVM exits, so there is no stop to forget.
 */
final class SharedMongo {

    private static final DockerImageName IMAGE = DockerImageName.parse("mongo:7.0");

    /** Kept as its own constant rather than reached for across modules: this module depends on neither. */
    static final String OPERATOR_STATE_DATABASE = "tapstate_nest";

    private static MongoDBContainer container;

    private SharedMongo() {
    }

    /**
     * The operator-state database configured for this test deployment. Every server here uses the
     * compatible default, while its control database takes a per-run name. The helper therefore has to
     * reset operator state explicitly between independent runs.
     *
     * <p>Two runs of one pipeline therefore meet in it. That is right in a deployment - a pipeline that
     * restarts is meant to resume what it had assembled - and wrong here, where two runs of one
     * specification share every id while sharing no data: the second inherits documents built from the
     * first one's rows and serves them as its own. Measured as a value appearing in a run whose source
     * never held it, whose store began empty, and whose change step had not executed.
     *
     * <p>Discarded per run rather than per specification: the id a run collides on is the pipeline's, and
     * every tier of every example writes one.
     */
    static synchronized void discardNestState() {
        if (container == null) {
            return;
        }
        try (MongoClient client = MongoClients.create(container.getReplicaSetUrl())) {
            client.getDatabase(OPERATOR_STATE_DATABASE).drop();
        }
    }
    /**
     * The URL of the one database above, for a case whose subject is what lives in it.
     *
     * <p>Named here rather than spelled out at the call site because this is the value configured for the
     * test deployment. A case that wrote the default again would keep working after the harness changed
     * its configuration and would be asserting about a database nothing uses.
     */
    static synchronized String assemblyStateUrl() {
        return replicaSetUrl(OPERATOR_STATE_DATABASE);
    }


    /** The URL of a database on the shared replica set; the caller's name keeps its data its own. */
    static synchronized String replicaSetUrl(String database) {
        if (container == null) {
            DockerGate.require();
            MongoDBContainer starting = new MongoDBContainer(IMAGE);
            starting.start();
            container = starting;
        }
        return container.getReplicaSetUrl(database);
    }
}
