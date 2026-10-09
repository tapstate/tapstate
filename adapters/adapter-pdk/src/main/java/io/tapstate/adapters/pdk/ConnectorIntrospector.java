package io.tapstate.adapters.pdk;

import static io.tapstate.adapters.pdk.ConnectorError.Parameters.ARTIFACT;

import io.tapstate.core.common.TapstateException;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;

import java.io.IOException;
import java.lang.annotation.AnnotationFormatError;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.stream.Collectors;

/**
 * Self-scan: reads a connector artifact (its classpath) and reports the entry class, spec and declared
 * PDK API version it carries, so a connector can be registered without being told its entry class up
 * front. It narrows the classpath's classes to the ones whose constant pool bears the
 * {@code @TapConnectorClass} descriptor — a cheap, no-classload pre-filter — loads only those in
 * isolation to confirm the annotation and read the spec path it names, and, when several classes of one
 * inheritance family are annotated, keeps the most-derived, the way a connector's cloud variants
 * subclass a shared base.
 *
 * <p>PDK types stay inside this class. A malformed artifact — no connector class, more than one
 * unrelated connector class, or a spec the annotation names but the jar omits — is refused with a coded
 * connector-domain exception keyed by the artifact, including I/O failures reading the archive or its
 * spec resource.
 */
public final class ConnectorIntrospector {

    private static final String TAP_CONNECTOR_DESCRIPTOR =
            "L" + TapConnectorClass.class.getName().replace('.', '/') + ";";

    /** The connector jar manifest attribute a connector build stamps with its PDK API version. */
    private static final String PDK_API_VERSION = "PDK-API-Version";

    private static final String CLASS_SUFFIX = ".class";

    /** Reads {@code classpath} (a connector jar plus any bundled deps) into the connector it declares. */
    public IntrospectedConnector introspect(List<Path> classpath) {
        String artifact = artifact(classpath);
        List<Candidate> candidates = scanForAnnotatedClasses(classpath);
        Confirmed entry;
        try (ConnectorClassLoader loader = ConnectorClassLoader.open(classpath)) {
            entry = confirmSingleEntryClass(loader, candidates, artifact);
        }
        try (JarFile jar = new JarFile(entry.jar().toFile())) {
            String spec = readSpec(jar, entry, artifact);
            String pdkApiVersion = mainAttribute(jar, PDK_API_VERSION);
            return new IntrospectedConnector(entry.className(), pdkApiVersion, entry.specPath(), spec);
        } catch (IOException e) {
            throw new TapstateException(ConnectorError.ARTIFACT_UNREADABLE,
                    Map.of(ARTIFACT, entry.jar().getFileName().toString()), e);
        }
    }

    /** A class whose constant pool bears the annotation descriptor: a candidate for the entry class. */
    private record Candidate(Path jar, String className) {
    }

    /** A candidate confirmed to bear the annotation, with the spec path it names and its loaded type. */
    private record Confirmed(Path jar, String className, String specPath, Class<?> type) {
    }

    private static List<Candidate> scanForAnnotatedClasses(List<Path> classpath) {
        List<Candidate> candidates = new ArrayList<>();
        for (Path jarPath : classpath) {
            try (JarFile jar = new JarFile(jarPath.toFile())) {
                Enumeration<JarEntry> entries = jar.entries();
                while (entries.hasMoreElements()) {
                    JarEntry entry = entries.nextElement();
                    if (entry.isDirectory() || !entry.getName().endsWith(CLASS_SUFFIX)) {
                        continue;
                    }
                    if (ClassFileAnnotationScanner.bearsAnnotation(readAll(jar, entry), TAP_CONNECTOR_DESCRIPTOR)) {
                        candidates.add(new Candidate(jarPath, classNameOf(entry.getName())));
                    }
                }
            } catch (IOException e) {
                throw new TapstateException(ConnectorError.ARTIFACT_UNREADABLE,
                        Map.of(ARTIFACT, jarPath.getFileName().toString()), e);
            }
        }
        return candidates;
    }

    private static Confirmed confirmSingleEntryClass(ConnectorClassLoader loader, List<Candidate> candidates,
                                                     String artifact) {
        List<Confirmed> confirmed = new ArrayList<>();
        for (Candidate candidate : candidates) {
            Class<?> type;
            try {
                type = loader.load(candidate.className());
            } catch (ClassNotFoundException | LinkageError e) {
                // A class the pre-filter flagged that will not load or link (e.g. a connector missing
                // its bundled runtime) cannot be read: a load failure, not a missing connector.
                throw new TapstateException(ConnectorError.LOAD_FAILED, Map.of("connector", artifact), e);
            }
            TapConnectorClass annotation;
            try {
                annotation = type.getDeclaredAnnotation(TapConnectorClass.class);
            } catch (AnnotationFormatError e) {
                // The class loads but its annotation bytes are corrupt: an unreadable artifact, not a
                // missing connector — and never an error that escapes to take the caller down.
                throw new TapstateException(ConnectorError.LOAD_FAILED, Map.of("connector", artifact), e);
            }
            if (annotation != null) { // the class truly bears it; a bare constant-pool match would not
                confirmed.add(new Confirmed(candidate.jar(), candidate.className(), annotation.value(), type));
            }
        }
        List<Confirmed> leaves = mostDerived(confirmed);
        if (leaves.isEmpty()) {
            throw new TapstateException(ConnectorError.NO_CONNECTOR_CLASS, Map.of(ARTIFACT, artifact), null);
        }
        if (leaves.size() > 1) {
            throw new TapstateException(ConnectorError.AMBIGUOUS_CONNECTOR_CLASS,
                    Map.of(ARTIFACT, artifact, "classes", classList(leaves)), null);
        }
        return leaves.get(0);
    }

    /**
     * Drops any confirmed class that another confirmed class extends — an annotated parent kept only
     * because a subclass carries the same annotation — leaving the most-derived of each family.
     */
    private static List<Confirmed> mostDerived(List<Confirmed> confirmed) {
        List<Confirmed> leaves = new ArrayList<>();
        for (Confirmed candidate : confirmed) {
            boolean parentOfAnotherConfirmed = confirmed.stream()
                    .anyMatch(other -> other != candidate && candidate.type().isAssignableFrom(other.type()));
            if (!parentOfAnotherConfirmed) {
                leaves.add(candidate);
            }
        }
        return leaves;
    }

    private static String readSpec(JarFile jar, Confirmed entry, String artifact) throws IOException {
        JarEntry specEntry = jar.getJarEntry(entry.specPath());
        if (specEntry == null) {
            throw new TapstateException(ConnectorError.SPEC_NOT_FOUND,
                    Map.of(ARTIFACT, artifact, "spec", entry.specPath()), null);
        }
        return new String(readAll(jar, specEntry), StandardCharsets.UTF_8);
    }

    private static String classList(List<Confirmed> confirmed) {
        return confirmed.stream().map(Confirmed::className).sorted().collect(Collectors.joining(", "));
    }

    private static String mainAttribute(JarFile jar, String name) throws IOException {
        Manifest manifest = jar.getManifest();
        return manifest == null ? null : manifest.getMainAttributes().getValue(name);
    }

    private static byte[] readAll(JarFile jar, JarEntry entry) throws IOException {
        try (InputStream in = jar.getInputStream(entry)) {
            return in.readAllBytes();
        }
    }

    private static String classNameOf(String entryName) {
        return entryName.substring(0, entryName.length() - CLASS_SUFFIX.length()).replace('/', '.');
    }

    private static String artifact(List<Path> classpath) {
        return classpath.stream().map(path -> path.getFileName().toString()).collect(Collectors.joining(", "));
    }
}
