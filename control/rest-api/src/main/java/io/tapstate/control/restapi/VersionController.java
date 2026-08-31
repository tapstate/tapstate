package io.tapstate.control.restapi;

import org.springframework.http.ResponseEntity;
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
 * <p>The last two are in the shape from the first release on, empty until what fills them lands: a
 * client that learns to read them must never have to tell "this server predates the field" from "this
 * server chose to omit it", and a field added later cannot be told apart from either.
 */
@Controller
class VersionController {

    private static final String VERSION = readVersion();

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
        body.put("dslVersions", List.of());
        body.put("dataVersion", null);
        return ResponseEntity.ok(body);
    }

    /**
     * Reads the version the build filtered in. Every failure here is a build defect rather than
     * anything a caller did, so each one crashes bare instead of becoming a coded diagnostic — including
     * an unsubstituted placeholder, which is what an accidentally disabled resource filter leaves behind
     * and is otherwise served to clients as if it were a version.
     */
    private static String readVersion() {
        try (InputStream properties =
                     VersionController.class.getResourceAsStream("/tapstate-version.properties")) {
            if (properties == null) {
                throw new IllegalStateException("tapstate-version.properties is not on the classpath");
            }
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
