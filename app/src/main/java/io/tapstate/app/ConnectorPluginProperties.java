package io.tapstate.app;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.nio.file.Path;
import java.util.List;

/**
 * The connector runtime settings, bound from {@code tapstate.connectors.*}. The plugins directory is the
 * on-disk cache the provisioner stages resolved connector artifacts into, content-addressed by hash and
 * reused across resolves. The seed directory is where a release ships connector jars; it is swept once
 * at startup, each jar going through the same register-if-absent path as an explicit register, and a
 * deployment without one is valid. Both default to directories under the working directory, which the
 * distribution's {@code conf/} and environment variables override (build once, run anywhere).
 */
@ConfigurationProperties("tapstate.connectors")
public class ConnectorPluginProperties {

    /** The on-disk cache directory for staged connector artifacts (a file per content hash). */
    private Path pluginsDir = Path.of("plugins");

    /** The seed directory swept at startup: every {@code *.jar} in it is registered if absent. */
    private Path seedDir = Path.of("connectors");
    private Path dropDir = Path.of("connectors");

    public Path getDropDir() { return dropDir; }

    public void setDropDir(Path dropDir) { this.dropDir = dropDir; }

    /**
     * Further connector ids the runtime register path accepts, beyond the ones this release officially
     * supports. Empty by default and left empty by every shipped artifact: it exists for a deployment
     * that stands up its own server with its own connector, and naming an id here does not make that
     * connector supported — it only stops the register path refusing it.
     */
    private List<String> alsoAcceptIds = List.of();

    public List<String> getAlsoAcceptIds() {
        return alsoAcceptIds;
    }

    public void setAlsoAcceptIds(List<String> alsoAcceptIds) {
        this.alsoAcceptIds = List.copyOf(alsoAcceptIds);
    }

    public Path getPluginsDir() {
        return pluginsDir;
    }

    public void setPluginsDir(Path pluginsDir) {
        this.pluginsDir = pluginsDir;
    }

    public Path getSeedDir() {
        return seedDir;
    }

    public void setSeedDir(Path seedDir) {
        this.seedDir = seedDir;
    }
}
