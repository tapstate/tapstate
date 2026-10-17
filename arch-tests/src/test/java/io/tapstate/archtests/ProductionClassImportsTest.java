package io.tapstate.archtests;

import com.tngtech.archunit.ArchConfiguration;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.core.importer.Location;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProductionClassImportsTest {
    private static final List<ImportOption.Predefined> PRODUCTION = List.of(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS);
    private static final List<List<String>> SCOPES = List.of(
            List.of("io.tapstate"), List.of("io.tapstate.core.sql"), List.of("io.tapstate.core.model"),
            List.of("io.tapstate.cli"), List.of("io.tapstate.control.core"),
            List.of("io.tapstate.control.restapi"), List.of("io.tapstate.control.core", "io.tapstate.control.restapi"));

    @Test
    void everyOriginalScopeRetainsEveryClassAndSourceOnTheCachedAndRawPaths() {
        for (List<String> scope : SCOPES) {
            JavaClasses cached = ProductionClassImports.cachedPackages(scope, PRODUCTION);
            JavaClasses raw = ProductionClassImports.rawPackages(scope, PRODUCTION);
            assertThat(ProductionClassImports.graphSources(cached)).as("complete names and sources: %s", scope)
                    .isNotEmpty().isEqualTo(ProductionClassImports.graphSources(raw));
            assertThat(dependencySources(cached)).as("complete dependency sources: %s", scope)
                    .isEqualTo(dependencySources(raw));
            assertThat(ProductionClassImports.cachedPackages(scope, PRODUCTION)).isSameAs(cached);
            System.out.println("tapstate.arch-graph=" + scope + " classes=" + cached.size()
                    + " cached=" + ProductionClassImports.graphSourceDigest(ProductionClassImports.graphSources(cached))
                    + " raw=" + ProductionClassImports.graphSourceDigest(ProductionClassImports.graphSources(raw)));

        }
    }

    @Test
    void distinctScopesAndImportOptionsCannotReuseAnUnrelatedGraph(@TempDir Path directory) throws IOException {
        Path one = copyFixture(directory.resolve("one/target/test-classes"));
        Path two = copyFixture(directory.resolve("two/target/test-classes"));
        JavaClasses included = ProductionClassImports.cachedLocations(List.of(Location.of(one)), List.of());
        JavaClasses excluded = ProductionClassImports.cachedLocations(List.of(Location.of(one)), PRODUCTION);
        JavaClasses elsewhere = ProductionClassImports.cachedLocations(List.of(Location.of(two)), List.of());
        assertThat(included).isNotEmpty();
        assertThat(excluded).isEmpty();
        assertThat(excluded).isNotSameAs(included);
        assertThat(elsewhere).isNotSameAs(included);
        assertThat(ProductionClassImports.graphSources(elsewhere)).isNotEqualTo(ProductionClassImports.graphSources(included));
        JavaClasses sql = ProductionClassImports.cachedPackages(List.of("io.tapstate.core.sql"), PRODUCTION);
        JavaClasses model = ProductionClassImports.cachedPackages(List.of("io.tapstate.core.model"), PRODUCTION);
        assertThat(sql).isNotSameAs(model);
        assertThat(ProductionClassImports.graphSources(sql)).isNotEqualTo(ProductionClassImports.graphSources(model));
    }

    @Test
    void aSamePathSizeAndTimestampCannotReusePreviousClasspathBytes(@TempDir Path directory) throws IOException {
        Path fixture = copyFixture(directory.resolve("classes"));
        FileTime timestamp = Files.getLastModifiedTime(fixture);
        byte[] original = Files.readAllBytes(fixture);
        JavaClasses first = ProductionClassImports.cachedLocations(List.of(Location.of(fixture)), List.of());
        byte[] changed = original.clone();
        byte[] oldMarker = "before".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] newMarker = "after!".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        int replacements = 0;
        for (int index = 0; index <= changed.length - oldMarker.length; index++) {
            boolean matches = true;
            for (int offset = 0; offset < oldMarker.length; offset++) matches &= changed[index + offset] == oldMarker[offset];
            if (matches) { System.arraycopy(newMarker, 0, changed, index, newMarker.length); replacements++; }
        }
        assertThat(replacements).isPositive();
        // Only ArchUnit reads this copied fixture. It is never defined or executed by a JVM loader.
        Files.write(fixture, changed);
        Files.setLastModifiedTime(fixture, timestamp);
        JavaClasses second = ProductionClassImports.cachedLocations(List.of(Location.of(fixture)), List.of());
        assertThat(Files.size(fixture)).isEqualTo(original.length);
        assertThat(Files.getLastModifiedTime(fixture)).isEqualTo(timestamp);
        assertThat(second).isNotSameAs(first);
        assertThat(ProductionClassImports.graphSources(second)).isEqualTo(ProductionClassImports.graphSources(first));
        assertThat(annotationValues(first)).contains("before");
        assertThat(annotationValues(second)).contains("after!").doesNotContain("before");
    }

    @Test
    void currentClassloaderAndImportConfigurationRemainPartOfTheKey(@TempDir Path directory) throws IOException {
        Path fixture = copyFixture(directory.resolve("classes"));
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        JavaClasses first = ProductionClassImports.cachedLocations(List.of(Location.of(fixture)), List.of());
        try {
            Thread.currentThread().setContextClassLoader(new ClassLoader(previous) { });
            assertThat(ProductionClassImports.cachedLocations(List.of(Location.of(fixture)), List.of())).isNotSameAs(first);
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
        ArchConfiguration.withThreadLocalScope(configuration -> {
            configuration.setResolveMissingDependenciesFromClassPath(!configuration.resolveMissingDependenciesFromClassPath());
            assertThat(ProductionClassImports.cachedLocations(List.of(Location.of(fixture)), List.of())).isNotSameAs(first);
        });
    }

    @Test
    void parsedCollectionsStayReadOnlyAndEveryRuleEvaluationRunsAgain(@TempDir Path directory) throws IOException {
        Path fixture = copyFixture(directory.resolve("classes"));
        JavaClasses graph = ProductionClassImports.cachedLocations(List.of(Location.of(fixture)), List.of());
        assertThatThrownBy(graph::clear).isInstanceOf(UnsupportedOperationException.class);
        AtomicBoolean forbid = new AtomicBoolean();
        AtomicInteger visits = new AtomicInteger();
        var rule = classes().should(new ArchCondition<JavaClass>("execute every time") {
            @Override public void check(JavaClass item, ConditionEvents events) {
                visits.incrementAndGet();
                if (forbid.get()) events.add(SimpleConditionEvent.violated(item, "owned fixture is now forbidden"));
            }
        });
        assertThat(rule.evaluate(graph).hasViolation()).isFalse();
        int firstVisits = visits.get();
        forbid.set(true);
        assertThat(rule.evaluate(graph).hasViolation()).isTrue();
        assertThat(visits.get()).isEqualTo(firstVisits * 2);
    }

    private static Set<String> dependencySources(JavaClasses graph) {
        return graph.stream().flatMap(type -> type.getDirectDependenciesFromSelf().stream())
                .map(dependency -> dependency.getTargetClass().getName() + "@" + dependency.getTargetClass().getSource()
                        .map(source -> source.getUri().toString()).orElse("unresolved"))
                .collect(java.util.stream.Collectors.toCollection(java.util.TreeSet::new));
    }

    private static Set<Object> annotationValues(JavaClasses graph) {
        return graph.stream().flatMap(type -> type.getAnnotations().stream())
                .flatMap(annotation -> annotation.getProperties().values().stream()).collect(java.util.stream.Collectors.toSet());
    }

    private static Path copyFixture(Path directory) throws IOException {
        String relative = AnnotatedFixture.class.getName().replace('.', '/') + ".class";
        Path destination = directory.resolve(relative);
        Files.createDirectories(destination.getParent());
        try (var bytes = ProductionClassImportsTest.class.getClassLoader().getResourceAsStream(relative)) {
            if (bytes == null) throw new AssertionError("Compiled owned fixture is missing");
            Files.copy(bytes, destination);
        }
        return destination;
    }

    @java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
    private @interface Marker { String value(); }

    @Marker("before")
    private static final class AnnotatedFixture { }
}
