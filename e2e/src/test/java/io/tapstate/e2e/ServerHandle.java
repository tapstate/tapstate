package io.tapstate.e2e;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * A running product, and the only thing that differs between tiers.
 *
 * <p>Everything a specification does travels over {@link #baseUrl}, so how the server got there -
 * booted in this JVM or launched as the shipped jar - is invisible above this interface. That is the
 * whole point: the fidelity axis lives here and nowhere else, which is what lets one specification
 * run on both tiers rather than two specifications drifting apart.
 */
interface ServerHandle extends AutoCloseable {

    /** The product setting naming where it stages the connector artifacts it resolves. */
    String PLUGINS_DIRECTORY_SETTING = "tapstate.connectors.plugins-dir";

    /** The product setting naming the database that holds durable operator state. */
    String OPERATOR_STATE_DATABASE_SETTING = "tapstate.store.mongo.operator-state-database";

    /**
     * The product setting naming further connector ids the register path accepts. The harness supplies
     * its own synthetic connector, which is by construction not one this release supports, so a server
     * the harness starts must be told to accept it. No shipped artifact sets this.
     */
    String ALSO_ACCEPT_IDS_SETTING = "tapstate.connectors.also-accept-ids";

    /** Where the product's HTTP surface answers, on loopback. */
    URI baseUrl();

    @Override
    void close();

    /**
     * A directory of this launch's own for the server to stage resolved connectors into.
     *
     * <p>Every launcher takes one rather than being trusted to remember, because the setting's default
     * is a relative path and both ways it can go wrong are quiet. Left alone, a server booted in this
     * JVM stages into the harness's own source tree, since that is what its working directory is. And
     * the staging cache is content-addressed and reused when the file is already there, so a second run
     * of an edited connector would silently be served the first run's jar - a test that passes against
     * code that no longer exists. A fresh directory per launch cannot do either.
     */
    static Path privateStagingDirectory() {
        try {
            return Files.createTempDirectory("tapstate-e2e-plugins");
        } catch (IOException e) {
            throw new UncheckedIOException("could not create a staging directory for the server", e);
        }
    }
}
