package io.tapstate.ci;

import org.apache.maven.eventspy.AbstractEventSpy;
import org.apache.maven.execution.ExecutionEvent;
import org.apache.maven.artifact.Artifact;
import org.codehaus.plexus.classworlds.realm.ClassRealm;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import org.apache.maven.execution.MojoExecutionEvent;
import org.apache.maven.model.io.xpp3.MavenXpp3Writer;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.util.*;

/** Optional local observer; consumers must reject missing or partial input declarations. */
public final class BuildInputSpy extends AbstractEventSpy {
    private final List<String> events = new ArrayList<>();
    private long eventBytes;
    private long hashedBytes;
    private long droppedEvents;
    private static final int MAX_EVENTS = 20000, MAX_FILES = 10000, MAX_ITEMS = 2048;
    private final Map<String, Object> files = new TreeMap<>();
    private final Map<Path, Cached> cache = new HashMap<>();
    private Path output;
    private long began;
    private int failures;
    private Object runtimeIdentity;
    private record Cached(Object key, long size, Object modified, Object changed, String digest) { }

    @Override public void init(Context context) {
        String target = System.getenv("TAPSTATE_BUILD_INPUT_OBSERVATION");
        if (target != null && !target.isBlank()) output = Path.of(target);
        began = System.nanoTime();
    }

    private static Map<String, Object> row(Object... fields) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (int i = 0; i < fields.length; i += 2) result.put((String) fields[i], fields[i + 1]);
        return result;
    }

    private static String digest(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    private String file(Path path) throws Exception {
        path = path.toAbsolutePath().normalize();
        String id = digest(path.toString().getBytes(StandardCharsets.UTF_8));
        BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (files.size() >= MAX_FILES) throw new IllegalStateException("File state count exceeds bound");
        if (!attributes.isRegularFile() || attributes.size() > 4L * 1024 * 1024 * 1024) {
            files.put(id, row("state", "not-observed", "reason", "nonregular-or-oversize"));
            return id;
        }
        Object changed = null;
        try { changed = Files.getAttribute(path, "unix:ctime", LinkOption.NOFOLLOW_LINKS); }
        catch (UnsupportedOperationException | java.io.IOException ignored) { }
        Cached previous = cache.get(path);
        String hash;
        if (previous != null && Objects.equals(previous.key(), attributes.fileKey())
                && changed != null && changed.equals(previous.changed())
                && previous.size() == attributes.size() && previous.modified().equals(attributes.lastModifiedTime())) {
            hash = previous.digest();
        } else {
            MessageDigest sha = MessageDigest.getInstance("SHA-256");
            byte[] block = new byte[65536];
            try (InputStream stream = Files.newInputStream(path, LinkOption.NOFOLLOW_LINKS)) {
                long read = 0;
                for (int n; (n = stream.read(block)) >= 0;) {
                    read += n;
                    hashedBytes += n;
                    if (hashedBytes > 4L * 1024 * 1024 * 1024) throw new IllegalStateException("Total input hashing exceeds bound");
                    if (read > attributes.size()) throw new IllegalStateException("Input grew during hashing");
                    sha.update(block, 0, n);
                }
                if (read != attributes.size()) throw new IllegalStateException("Input size changed during hashing");
            }
            BasicFileAttributes after = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            Object changedAfter = changed == null ? null : Files.getAttribute(path, "unix:ctime", LinkOption.NOFOLLOW_LINKS);
            if (!Objects.equals(changed, changedAfter) || !Objects.equals(attributes.fileKey(), after.fileKey()) || attributes.size() != after.size()
                    || !attributes.lastModifiedTime().equals(after.lastModifiedTime())) {
                throw new IllegalStateException("Observed input changed during hashing");
            }
            hash = HexFormat.of().formatHex(sha.digest());
            cache.put(path, new Cached(attributes.fileKey(), attributes.size(), attributes.lastModifiedTime(), changed, hash));
        }
        String version = digest((id + ":" + attributes.size() + ":" + hash).getBytes(StandardCharsets.UTF_8));
        files.putIfAbsent(version, row("state", "observed-file", "size", attributes.size(), "sha256", hash,
                "path_identity_sha256", id));
        return version;
    }

    private Object artifact(Artifact value) throws Exception {
        if (value == null) return row("state", "not-observed", "reason", "no-artifact");
        Map<String, Object> record = row("group", value.getGroupId(), "artifact", value.getArtifactId(),
                "version", value.getVersion(), "type", value.getType(), "classifier", value.getClassifier(),
                "scope", value.getScope(), "resolved", value.isResolved());
        if (value.getFile() == null) record.put("state", "not-observed-no-file");
        else record.put("file", file(value.getFile().toPath()));
        return record;
    }

    private Object realm(ClassRealm value, Set<String> visited, int depth) throws Exception {
        if (value == null) return row("state", "not-observed", "reason", "no-realm");
        if (depth > 32 || visited.size() > 128) return row("state", "partial-bound");
        if (!visited.add(digest(value.getId().getBytes(StandardCharsets.UTF_8)))) return row("state", "reference", "id", digest(value.getId().getBytes(StandardCharsets.UTF_8)));
        List<Object> urls = new ArrayList<>();
        if (value.getURLs().length > MAX_ITEMS) throw new IllegalStateException("Realm URL count exceeds bound");
        for (URL url : value.getURLs()) {
            if (!url.getProtocol().equals("file")) urls.add(row("state", "nonfile-url", "identity_sha256", digest(url.toString().getBytes(StandardCharsets.UTF_8))));
            else urls.add(row("file", file(Path.of(url.toURI()))));
        }
        List<Object> imports = new ArrayList<>();
        int importedCount = 0;
        for (ClassRealm imported : value.getImportRealms()) {
            if (++importedCount > MAX_ITEMS) throw new IllegalStateException("Import count exceeds bound");
            imports.add(realm(imported, visited, depth + 1));
        }
        return row("state", "observed-realm-urls", "id", digest(value.getId().getBytes(StandardCharsets.UTF_8)), "ordered_urls", urls,
                "parent", realm(value.getParentRealm(), visited, depth + 1), "imports", imports,
                "import_package_prefixes", "not-observed", "opaque_parent_class", value.getParentClassLoader() == null ? null : value.getParentClassLoader().getClass().getName());
    }

    private static void addConfiguration(List<Object> values, Object value, int[] visits) {
        if (++visits[0] > 20000 || values.size() >= MAX_ITEMS) {
            throw new IllegalStateException("Model configuration count exceeds bound");
        }
        if (value != null) values.add(value);
    }

    private static void buildConfigurations(org.apache.maven.model.BuildBase build,
            List<Object> values, int[] visits) {
        if (build == null) return;
        List<org.apache.maven.model.Plugin> plugins = new ArrayList<>();
        if (build.getPlugins().size() > MAX_ITEMS) throw new IllegalStateException("Plugin count exceeds bound");
        plugins.addAll(build.getPlugins());
        if (build.getPluginManagement() != null) {
            if (build.getPluginManagement().getPlugins().size() > MAX_ITEMS) throw new IllegalStateException("Managed plugin count exceeds bound");
            plugins.addAll(build.getPluginManagement().getPlugins());
        }
        for (var plugin : plugins) {
            addConfiguration(values, plugin.getConfiguration(), visits);
            if (plugin.getExecutions().size() > MAX_ITEMS) throw new IllegalStateException("Execution count exceeds bound");
            for (var execution : plugin.getExecutions()) addConfiguration(values, execution.getConfiguration(), visits);
        }
    }

    private static void reportConfigurations(org.apache.maven.model.Reporting reporting,
            List<Object> values, int[] visits) {
        if (reporting == null) return;
        if (reporting.getPlugins().size() > MAX_ITEMS) throw new IllegalStateException("Report plugin count exceeds bound");
        for (var plugin : reporting.getPlugins()) {
            addConfiguration(values, plugin.getConfiguration(), visits);
            if (plugin.getReportSets().size() > MAX_ITEMS) throw new IllegalStateException("Report set count exceeds bound");
            for (var report : plugin.getReportSets()) addConfiguration(values, report.getConfiguration(), visits);
        }
    }

    private static void boundedModelConfigurations(org.apache.maven.model.Model model) {
        List<Object> values = new ArrayList<>();
        int[] visits = {0};
        buildConfigurations(model.getBuild(), values, visits);
        reportConfigurations(model.getReporting(), values, visits);
        if (model.getProfiles().size() > MAX_ITEMS) throw new IllegalStateException("Model profile count exceeds bound");
        for (var profile : model.getProfiles()) {
            addConfiguration(values, null, visits);
            buildConfigurations(profile.getBuild(), values, visits);
            reportConfigurations(profile.getReporting(), values, visits);
        }
        record Node(org.codehaus.plexus.util.xml.Xpp3Dom value, int depth) { }
        int nodes = 0;
        for (Object value : values) {
            if (!(value instanceof org.codehaus.plexus.util.xml.Xpp3Dom root)) {
                throw new IllegalStateException("Unknown model configuration type");
            }
            Set<org.codehaus.plexus.util.xml.Xpp3Dom> seen = Collections.newSetFromMap(new IdentityHashMap<>());
            Deque<Node> pending = new ArrayDeque<>();
            pending.push(new Node(root, 0));
            while (!pending.isEmpty()) {
                Node current = pending.pop();
                if (current.depth() > 64 || ++nodes > 20000 || !seen.add(current.value())) {
                    throw new IllegalStateException("Model configuration depth, identity or count exceeds bound");
                }
                int count = current.value().getChildCount();
                if (count > MAX_ITEMS) throw new IllegalStateException("Configuration child count exceeds bound");
                for (int i = 0; i < count; i++) pending.push(new Node(current.value().getChild(i), current.depth() + 1));
            }
        }
    }

    private static String effectiveModelHash(org.apache.maven.project.MavenProject project) throws Exception {
        boundedModelConfigurations(project.getModel());
        MessageDigest sha = MessageDigest.getInstance("SHA-256");
        OutputStream bounded = new OutputStream() {
            private long count;
            @Override public void write(int value) {
                if (++count > 2L * 1024 * 1024) throw new IllegalStateException("Effective model exceeds bound");
                sha.update((byte) value);
            }
            @Override public void write(byte[] values, int offset, int length) {
                count += length;
                if (count > 2L * 1024 * 1024) throw new IllegalStateException("Effective model exceeds bound");
                sha.update(values, offset, length);
            }
        };
        try (var writer = new OutputStreamWriter(bounded, StandardCharsets.UTF_8)) {
            new MavenXpp3Writer().write(writer, project.getModel());
        }
        return HexFormat.of().formatHex(sha.digest());
    }

    private static Object pathIdentities(List<String> paths) throws Exception {
        if (paths.size() > MAX_ITEMS) throw new IllegalStateException("Source root count exceeds bound");
        List<Object> result = new ArrayList<>();
        for (String path : paths) {
            result.add(row("path_identity_sha256", digest(Path.of(path).toAbsolutePath().normalize()
                    .toString().getBytes(StandardCharsets.UTF_8)), "file_consumption_proven", false));
        }
        return result;
    }

    private Object actualRuntimeIdentity() throws Exception {
        if (runtimeIdentity != null) return runtimeIdentity;
        Path javaHome = Path.of(System.getProperty("java.home"));
        Map<String, Object> runtimeFiles = new LinkedHashMap<>();
        for (String relative : List.of("release", "bin/java", "lib/modules", "lib/ct.sym")) {
            Path input = javaHome.resolve(relative);
            runtimeFiles.put(relative, Files.exists(input, LinkOption.NOFOLLOW_LINKS)
                    ? row("file", file(input)) : row("state", "not-observed", "reason", "runtime-file-absent"));
        }
        URL core = org.apache.maven.execution.MavenSession.class.getProtectionDomain().getCodeSource().getLocation();
        Object mavenCore = core.getProtocol().equals("file") ? row("file", file(Path.of(core.toURI())))
                : row("state", "not-observed", "reason", "nonfile-maven-core");
        runtimeIdentity = row("maven_jvm_version", Runtime.version().toString(), "java_vendor", System.getProperty("java.vendor"),
                "java_home_identity_sha256", digest(javaHome.toString().getBytes(StandardCharsets.UTF_8)),
                "maven_core", mavenCore, "major_jvm_file_identities", runtimeFiles,
                "capture_phase", "first-configured-mojo", "whole_JDK_installation_proven", false, "selected_or_forked_compiler_proven", false);
        return runtimeIdentity;
    }

    /** Receives the configured Mojo without calling or changing any plugin parameter. */
    public synchronized void configured(MojoExecutionEvent event) {
        if (output == null) return;
        try {
            var project = event.getProject();
            var execution = event.getExecution();
            List<Object> activeProfiles = new ArrayList<>();
            if (project.getActiveProfiles().size() > MAX_ITEMS) throw new IllegalStateException("Profile count exceeds bound");
            for (var profile : project.getActiveProfiles()) {
                activeProfiles.add(row("id_sha256", digest(profile.getId().getBytes(StandardCharsets.UTF_8))));
            }
            String implementation = event.getMojo().getClass().getName();
            retain(row("event", "MojoConfigured", "nanos", System.nanoTime() - began,
                    "project", project.getGroupId() + ":" + project.getArtifactId() + ":" + project.getVersion(),
                    "execution", execution.getExecutionId(), "goal", execution.getGoal(),
                    "plugin", execution.getGroupId() + ":" + execution.getArtifactId() + ":" + execution.getVersion(),
                    "configured_implementation", implementation,
                    "effective_model_sha256", effectiveModelHash(project),
                    "active_profile_identities", activeProfiles,
                    "registered_compile_source_roots", pathIdentities(project.getCompileSourceRoots()),
                    "registered_test_source_roots", pathIdentities(project.getTestCompileSourceRoots()),
                    "requested_concurrency", event.getSession().getRequest().getDegreeOfConcurrency(),
                    "runtime_identity", actualRuntimeIdentity(),
                    "fully_injected_parameter_values", "not-observed",
                    "compiler_consumed_files", "not-observed",
                    "selected_or_forked_compiler", "not-observed"));
        } catch (Throwable failure) {
            failures++;
            try {
                retain(row("event", "MojoConfigured", "state", "partial", "reason_class", failure.getClass().getName()));
            } catch (Throwable recordingFailure) {
                droppedEvents++;
            }
        }
    }

    @Override public synchronized void onEvent(Object event) {
        if (output == null || !(event instanceof ExecutionEvent value)) return;
        String type = value.getType().name();
        if (!(type.equals("MojoStarted") || type.equals("MojoSucceeded") || type.equals("MojoFailed"))) return;
        if (events.size() >= MAX_EVENTS || eventBytes >= 24L * 1024 * 1024) {
            droppedEvents++;
            if (failures == 0) failures = 1;
            return;
        }
        try {
            var mojo = value.getMojoExecution();
            var project = value.getProject();
            Map<String, Object> record = row("event", type, "nanos", System.nanoTime() - began,
                    "project", project.getGroupId() + ":" + project.getArtifactId() + ":" + project.getVersion(),
                    "execution", mojo.getExecutionId(), "goal", mojo.getGoal(),
                    "plugin", mojo.getGroupId() + ":" + mojo.getArtifactId() + ":" + mojo.getVersion(),
                    "merged_xml_config_sha256", configurationHash(mojo.getConfiguration()),
                    "fully_injected_parameter_values", "not-observed");
            List<Object> dependencies = new ArrayList<>();
            int count = 0;
            for (Artifact dependency : project.getArtifacts()) {
                if (++count > MAX_ITEMS) throw new IllegalStateException("Project artifact count exceeds bound");
                dependencies.add(artifact(dependency));
            }
            record.put("project_artifacts_at_event", dependencies);
            if (!type.equals("MojoStarted")) {
                var descriptor = mojo.getMojoDescriptor().getPluginDescriptor();
                record.put("plugin_artifact", artifact(descriptor.getPluginArtifact()));
                List<Object> artifacts = new ArrayList<>();
                if (descriptor.getArtifacts() != null) {
                    int pluginCount = 0;
                    for (Artifact a : descriptor.getArtifacts()) {
                        if (++pluginCount > MAX_ITEMS) throw new IllegalStateException("Plugin artifact count exceeds bound");
                        artifacts.add(artifact(a));
                    }
                }
                record.put("plugin_artifacts", artifacts);
                record.put("realm", realm(descriptor.getClassRealm(), new HashSet<>(), 0));
            }
            retain(record);
        } catch (Exception failure) {
            failures++;
            retain(row("event", type, "state", "partial", "reason_class", failure.getClass().getName()));
        }
    }

    @Override public synchronized void close() throws Exception {
        if (output == null) return;
        Map<String, Object> root = row("schema", 3, "type", "local-configured-build-input-observation", "events", events,
                "files", files, "failures", failures, "dropped_events", droppedEvents, "hashed_bytes", hashedBytes, "java_version", System.getProperty("java.version"),
                "java_vendor", System.getProperty("java.vendor"), "os_arch", System.getProperty("os.arch"),
                "annotation_processor_inputs", "not-observed", "external_generator_inputs", "not-observed",
                "whole_build_provenance_complete", false, "elapsed_nanos", System.nanoTime() - began);
        // Events are already bounded JSON records; avoid allocating their nested object graphs again.
        root.put("events", null);
        String header = json(root);
        String text = header.substring(0, header.length() - 1) + ",\"event_records\":[" + String.join(",", events) + "]}";
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > 64 * 1024 * 1024) throw new IllegalStateException("Observation metadata exceeds bound");
        Files.write(output, bytes, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
    }

    private void retain(Object record) {
        String text = json(record, 256 * 1024);
        long bytes = text.getBytes(StandardCharsets.UTF_8).length;
        if (bytes > 1024 * 1024 || events.size() >= MAX_EVENTS || eventBytes + bytes > 24L * 1024 * 1024) {
            droppedEvents++;
            if (failures == 0) failures = 1;
            return;
        }
        eventBytes += bytes;
        events.add(text);
    }

    private static String configurationHash(org.codehaus.plexus.util.xml.Xpp3Dom value) throws Exception {
        if (value == null) return digest("null".getBytes(StandardCharsets.UTF_8));
        checkConfiguration(value, new long[] {0, 0}, 0);
        return digest(value.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static void checkConfiguration(org.codehaus.plexus.util.xml.Xpp3Dom node, long[] count, int depth) {
        if (depth > 64 || ++count[0] > 10000 || node.getChildCount() > MAX_ITEMS) throw new IllegalStateException("Configuration structure exceeds bound");
        count[1] += node.getName().length();
        if (node.getValue() != null) count[1] += node.getValue().length();
        if (node.getAttributeNames().length > MAX_ITEMS) throw new IllegalStateException("Configuration attributes exceed bound");
        for (String key : node.getAttributeNames()) count[1] += key.length() + node.getAttribute(key).length();
        if (count[1] > 1024 * 1024) throw new IllegalStateException("Configuration text exceeds bound");
        for (var child : node.getChildren()) checkConfiguration(child, count, depth + 1);
    }

    private static String json(Object value) {
        return json(value, 8 * 1024 * 1024);
    }

    private static String json(Object value, int limit) {
        StringBuilder out = new StringBuilder();
        encode(value, out, limit, new int[] {0}, 0);
        return out.toString();
    }

    private static void append(StringBuilder out, String text, int limit) {
        if (text.length() > limit - out.length()) throw new IllegalStateException("Observation serialization exceeds bound");
        out.append(text);
    }

    private static void append(StringBuilder out, char value, int limit) {
        if (out.length() >= limit) throw new IllegalStateException("Observation serialization exceeds bound");
        out.append(value);
    }

    private static void encode(Object value, StringBuilder out, int limit, int[] nodes, int depth) {
        if (depth > 128 || ++nodes[0] > 200000) throw new IllegalStateException("Observation graph exceeds bound");
        if (value == null) { append(out, "null", limit); return; }
        if (value instanceof String text) {
            if (text.length() > 65536) throw new IllegalStateException("Observation string exceeds bound");
            append(out, "\"", limit);
            for (char c : text.toCharArray()) {
                if (c == '"' || c == '\\') { append(out, '\\', limit); append(out, c, limit); }
                else if (c < 32) append(out, String.format("\\u%04x", (int) c), limit);
                else append(out, c, limit);
            }
            append(out, "\"", limit); return;
        }
        if (value instanceof Number || value instanceof Boolean) { append(out, value.toString(), limit); return; }
        if (value instanceof Map<?, ?> map) {
            append(out, "{", limit); boolean first = true;
            for (var item : map.entrySet()) {
                if (!first) append(out, ",", limit); first = false;
                encode(item.getKey().toString(), out, limit, nodes, depth + 1);
                append(out, ":", limit); encode(item.getValue(), out, limit, nodes, depth + 1);
            }
            append(out, "}", limit); return;
        }
        if (value instanceof Iterable<?> list) {
            append(out, "[", limit); boolean first = true;
            for (Object item : list) {
                if (!first) append(out, ",", limit); first = false;
                encode(item, out, limit, nodes, depth + 1);
            }
            append(out, "]", limit); return;
        }
        throw new IllegalArgumentException("Unexpected observation value type");
    }
}
