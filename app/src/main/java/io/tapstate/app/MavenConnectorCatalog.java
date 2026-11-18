package io.tapstate.app;

import io.tapstate.adapters.pdk.PdkApiLevels;
import io.tapstate.adapters.pdk.PdkLevelResolver;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.OptionalInt;
import java.util.regex.Pattern;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

/** Reads connector releases from Maven BOMs and orders compatible candidates for a host. */
final class MavenConnectorCatalog {
    private static final Pattern GROUP = Pattern.compile("[A-Za-z0-9_-]+(\\.[A-Za-z0-9_-]+)*");
    private static final Pattern ARTIFACT = Pattern.compile("[A-Za-z0-9][A-Za-z0-9_.-]*");
    private static final Pattern RELEASE = Pattern.compile("[0-9]+(\\.[0-9]+)*");
    private static final Pattern API = Pattern.compile("2\\.(?:0\\.)?([0-9]+)(?:-SNAPSHOT|-[0-9]{8}\\.[0-9]{6}-[0-9]+)?");

    private MavenConnectorCatalog() {
    }

    record Coordinate(String groupId, String artifactId, String version) {
        Coordinate {
            if (!GROUP.matcher(groupId).matches() || !ARTIFACT.matcher(artifactId).matches()
                    || !RELEASE.matcher(version).matches()) {
                throw new IllegalArgumentException("Invalid Maven release coordinate");
            }
        }

        URI file(URI repository, String extension) {
            return repository.resolve(directory() + version + "/" + artifactId + "-" + version + "." + extension);
        }

        URI metadata(URI repository) {
            return repository.resolve(directory() + "maven-metadata.xml");
        }

        private String directory() {
            return groupId.replace('.', '/') + "/" + artifactId + "/";
        }
    }

    record Release(Coordinate coordinate, String pdkApiVersion) {
    }

    static List<Release> read(InputStream input, Coordinate expectedBom) throws IOException {
        Element pom = parse(input);
        if (!"project".equals(pom.getLocalName()) || !expectedBom.groupId().equals(value(pom, "groupId"))
                || !expectedBom.artifactId().equals(value(pom, "artifactId"))
                || !expectedBom.version().equals(value(pom, "version"))) {
            throw new IOException("Maven repository returned a different BOM");
        }
        Element properties = child(pom, "properties");
        Element dependencies = child(child(pom, "dependencyManagement"), "dependencies");
        if (properties == null || dependencies == null) {
            throw new IOException("Connector BOM lacks properties or managed dependencies");
        }
        var releases = new ArrayList<Release>();
        for (Element dependency : children(dependencies, "dependency")) {
            String artifact = value(dependency, "artifactId");
            String api = value(properties, "tapdata.connector." + artifact + ".pdk.api.version");
            try {
                releases.add(new Release(new Coordinate(
                        value(dependency, "groupId"), artifact, value(dependency, "version")), api));
            } catch (IllegalArgumentException invalid) {
                throw new IOException("Connector BOM contains an invalid release coordinate", invalid);
            }
        }
        if (releases.isEmpty()) {
            throw new IOException("Connector BOM has no managed releases");
        }
        return List.copyOf(releases);
    }

    static List<String> readBomVersions(InputStream input, Coordinate configuredBom) throws IOException {
        Element metadata = parse(input);
        if (!"metadata".equals(metadata.getLocalName()) || !configuredBom.groupId().equals(value(metadata, "groupId"))
                || !configuredBom.artifactId().equals(value(metadata, "artifactId"))) {
            throw new IOException("Maven repository returned metadata for a different BOM");
        }
        return children(child(child(metadata, "versioning"), "versions"), "version").stream()
                .map(Element::getTextContent)
                .map(String::strip)
                .filter(version -> RELEASE.matcher(version).matches())
                .filter(version -> compareVersions(version, configuredBom.version()) < 0)
                .distinct()
                .sorted((left, right) -> compareVersions(right, left))
                .toList();
    }

    static List<Release> compatibleReleases(List<Release> releases, String artifactId, String hostApiVersion) {
        int ceiling = generation(hostApiVersion).orElseThrow(
                () -> new IllegalArgumentException("Unrecognized host PDK API version"));
        int hostLevel = PdkApiLevels.level(normalizedApi(ceiling));
        return releases.stream()
                .filter(release -> release.coordinate().artifactId().equals(artifactId))
                .filter(release -> compatible(release, ceiling, hostLevel))
                .distinct()
                .sorted(Comparator.<Release>comparingInt(release -> generation(release.pdkApiVersion()).orElseThrow())
                        .thenComparing(release -> release.coordinate().version(), MavenConnectorCatalog::compareVersions)
                        .reversed())
                .toList();
    }

    private static boolean compatible(Release release, int ceiling, int hostLevel) {
        OptionalInt required = generation(release.pdkApiVersion());
        if (required.isEmpty() || required.getAsInt() > ceiling) {
            return false;
        }
        var resolution = PdkLevelResolver.resolve(normalizedApi(required.getAsInt()), null);
        return resolution.compatible() && resolution.requiredLevel() <= hostLevel;
    }

    private static String normalizedApi(int generation) {
        return "2.0." + generation;
    }

    private static OptionalInt generation(String version) {
        var matcher = API.matcher(version);
        if (!matcher.matches()) return OptionalInt.empty();
        try {
            return OptionalInt.of(Integer.parseInt(matcher.group(1)));
        } catch (NumberFormatException invalid) {
            return OptionalInt.empty();
        }
    }

    static boolean sameGeneration(String left, String right) {
        return generation(left).equals(generation(right));
    }

    static int compareVersions(String left, String right) {
        String[] a = left.split("\\.");
        String[] b = right.split("\\.");
        for (int i = 0; i < Math.max(a.length, b.length); i++) {
            var x = new java.math.BigInteger(i < a.length ? a[i] : "0");
            var y = new java.math.BigInteger(i < b.length ? b[i] : "0");
            int order = x.compareTo(y);
            if (order != 0) return order;
        }
        return 0;
    }

    private static Element parse(InputStream input) throws IOException {
        try {
            var parser = DocumentBuilderFactory.newDefaultNSInstance();
            parser.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            parser.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
            parser.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
            return parser.newDocumentBuilder().parse(input).getDocumentElement();
        } catch (SAXException invalid) {
            throw new IOException("Invalid Maven catalog XML", invalid);
        } catch (ParserConfigurationException unavailable) {
            throw new IllegalStateException("Maven XML parser configuration is unavailable", unavailable);
        }
    }

    private static String value(Element parent, String name) {
        Element element = child(parent, name);
        return element == null ? "" : element.getTextContent().strip();
    }

    private static Element child(Element parent, String name) {
        return children(parent, name).stream().findFirst().orElse(null);
    }

    private static List<Element> children(Element parent, String name) {
        var result = new ArrayList<Element>();
        if (parent != null) {
            for (Node node = parent.getFirstChild(); node != null; node = node.getNextSibling()) {
                if (node instanceof Element element && name.equals(element.getLocalName())) {
                    result.add(element);
                }
            }
        }
        return result;
    }
}
