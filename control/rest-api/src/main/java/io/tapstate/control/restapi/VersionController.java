package io.tapstate.control.restapi;

import io.tapstate.core.model.Resource;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * What version of Tapstate this process is. Like the liveness probe it is anonymous, at the root and
 * outside the {@code /api} prefix — a client has to be able to ask before it authenticates, because
 * connecting is decoupled from logging in and the answer is what tells a client whether it is talking
 * to a build it understands. It is a plain {@code @Controller} (not {@code @RestController}) precisely
 * so it stays outside that prefix, and it reveals nothing beyond the three numbers below.
 *
 * <p>It answers three independent numbers, none derived from any other:
 *
 * <ul>
 *   <li>{@code version} — the product release this process was built from.</li>
 *   <li>{@code dslVersions} — the authoring grammar versions it accepts.</li>
 *   <li>{@code dataVersion} — the schema version of the system data it is running against.</li>
 * </ul>
 *
 * <p>All three are in the shape from the first release on. {@code dataVersion} is null on a run with
 * no store: a client has to be able to tell that from a store nothing has migrated yet, which is zero.
 */
@Controller
class VersionController {

    private static final String VERSION = readVersion();

    private final SystemDataVersion systemDataVersion;

    /**
     * Takes the store's schema version if this run has a store. A run without one -- a substrate check,
     * say -- has no such bean, and reports the field as absent rather than inventing a number for it.
     */
    VersionController(@Nullable SystemDataVersion systemDataVersion) {
        this.systemDataVersion = systemDataVersion;
    }

    // The projection marker, on a handler that stays outside /api. What this annotation records is
    // which registered operation the endpoint answers for, not where it is mounted -- so the one
    // anonymous endpoint serves the CLI, a model over MCP, and a plain curl alike, and there is no
    // second place that reports a version.
    @Verb("system.version")
    @GetMapping("/version")
    ResponseEntity<Map<String, Object>> version() {
        // LinkedHashMap rather than Map.of: the reserved fields are reported as present-and-empty, and
        // an absent value has to travel as an explicit null for a client to read it that way.
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("version", VERSION);
        // The same list the parser accepts against, not a copy of it: a client deciding what it may
        // send and the server deciding what it will read must not be able to disagree.
        body.put("dslVersions", Resource.SUPPORTED_VERSIONS);
        body.put("dataVersion", systemDataVersion == null ? null : systemDataVersion.current());
        return ResponseEntity.ok(body);
    }

    /**
     * Reads the version the build filtered in. Every failure here is a build defect rather than
     * anything a caller did, so each one crashes bare instead of becoming a coded diagnostic — including
     * an unsubstituted placeholder, which is what an accidentally disabled resource filter leaves behind
     * and is otherwise served to clients as if it were a version.
     */
    private static String readVersion() {
        return versionIn(VersionController.class.getResourceAsStream("/tapstate-version.properties"));
    }

    /**
     * The reading itself, taking the stream rather than finding it, so that the shapes a broken build
     * leaves behind can be put in front of it. Package-private for that reason and no other: each
     * refusal below is reachable only from a build that is already wrong, and a guard nothing can put
     * into its failing state is a guard nobody knows still works.
     */
    static String versionIn(InputStream properties) {
        if (properties == null) {
            throw new IllegalStateException("tapstate-version.properties is not on the classpath");
        }
        try (properties) {
            Properties parsed = new Properties();
            parsed.load(properties);
            String version = parsed.getProperty("version");
            if (version == null || version.isBlank() || version.startsWith("${")) {
                throw new IllegalStateException(
                        "tapstate-version.properties carries no substituted version: " + version);
            }
            return version;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
