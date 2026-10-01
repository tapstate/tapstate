package io.tapstate.archtests;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaMethod;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import io.tapstate.control.core.ControlOperations;
import io.tapstate.control.core.Frontend;
import io.tapstate.control.core.Operation;
import io.tapstate.control.restapi.Verb;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static java.util.stream.Collectors.toCollection;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The face-projection gates: the HTTP endpoint table must stay a derivation of the operation registry,
 * in both directions. The registry is the single source of truth for what the process can do; a face
 * composes registered operations and never invents one, and an operation the registry opens on a face
 * must actually be reachable there.
 *
 * <p>This is the gate the {@link Verb} annotation is declared for. It lives here rather than in a
 * face test because only a whole-module scan sees every handler at once: a Spring test stands up the
 * controllers it names, so it can only ever assert the slice it booted — which is what let an operation
 * the registry declares open sit with no endpoint at all, unnoticed.
 *
 * <p>Scanning reuses arch-tests' existing capability (ArchUnit's {@code ClassFileImporter}, production
 * scope only) — no second scanning library, and no application context.
 */
class ControlFaceProjectionGatesTest {

    private static JavaClasses restApi;

    @BeforeAll
    static void importFace() {
        restApi = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("io.tapstate.control.restapi");
    }

    /** Every operation id an HTTP handler declares it projects. */
    private static Set<String> projectedVerbs() {
        return restApi.stream()
                .flatMap(c -> c.getMethods().stream())
                .filter(m -> m.isAnnotatedWith(Verb.class))
                .map(m -> m.getAnnotationOfType(Verb.class).value())
                .collect(toCollection(TreeSet::new));
    }

    private static Set<String> apiHandlersWithoutVerb() {
        return restApi.stream()
                .filter(type -> type.isAnnotatedWith(RestController.class))
                .flatMap(type -> type.getMethods().stream())
                .filter(ControlFaceProjectionGatesTest::isRequestMapping)
                .filter(method -> !method.isAnnotatedWith(Verb.class))
                .map(JavaMethod::getDescription)
                .collect(toCollection(TreeSet::new));
    }

    private static boolean isRequestMapping(JavaMethod method) {
        return method.getAnnotations().stream()
                .map(annotation -> annotation.getRawType())
                .anyMatch(type -> type.isEquivalentTo(RequestMapping.class)
                        || type.isMetaAnnotatedWith(RequestMapping.class));
    }

    /** Every operation the registry opens on a face served by the HTTP surface. */
    private static Set<String> registeredVerbs() {
        return java.util.stream.Stream.concat(
                        ControlOperations.registry().exposedOn(Frontend.CLI).stream(),
                        ControlOperations.registry().exposedOn(Frontend.REST).stream())
                .map(Operation::id)
                .collect(toCollection(TreeSet::new));
    }

    @Test
    @DisplayName("every HTTP handler projects a registered operation")
    void everyProjectedVerbIsRegistered() {
        assertThat(projectedVerbs())
                .as("an endpoint may only project a registered operation on an HTTP-served face — a face composes "
                        + "registered operations, it never invents an endpoint")
                .isSubsetOf(registeredVerbs());
    }

    @Test
    @DisplayName("every HTTP handler carries an operation projection")
    void everyApiHandlerDeclaresVerb() {
        assertThat(apiHandlersWithoutVerb())
                .as("an /api handler without @Verb would bypass the registry and must fail the build")
                .isEmpty();
    }

    /**
     * The registered operations that have no HTTP endpoint yet, each a deliberate deferral rather than an
     * oversight. User administration still has no face; the first administrator is created through the
     * bootstrap entry point, which is guarded on its own terms. Two operations ({@code user.passwd},
     * {@code user.list}) have no control-plane service behind them either, so opening those is a
     * control-plane change and not merely a routing one.
     *
     * <p>An entry here is a reviewed decision, not a running to-do list: a verb added to the registry with
     * no face must turn this gate red, and deleting its entry is how it earns one.
     */
    private static final Set<String> DEFERRED_WITH_NO_FACE = Set.of(
            "user.create", "user.passwd", "user.list");

    @Test
    @DisplayName("every registered operation is reachable over HTTP, bar the deferred ones")
    void everyRegisteredVerbIsProjectedOntoHttp() {
        Set<String> unreachable = new TreeSet<>(registeredVerbs());
        unreachable.removeAll(projectedVerbs());
        unreachable.removeAll(DEFERRED_WITH_NO_FACE);

        assertThat(unreachable)
                .as("an operation the registry opens on an HTTP-served face with no endpoint behind it "
                        + "the CLI face with no endpoint behind it cannot be invoked at all — give it an "
                        + "endpoint, or record it as deliberately deferred")
                .isEmpty();
    }

    @Test
    @DisplayName("the deferred list holds only registered verbs that really have no endpoint")
    void theDeferredListDoesNotRot() {
        // A deferral that has quietly come true would otherwise sit here forever, silently excusing a verb
        // that no longer needs excusing — and hiding the next real gap behind a stale name.
        assertThat(registeredVerbs())
                .as("a deferred verb must still be a registered operation")
                .containsAll(DEFERRED_WITH_NO_FACE);
        assertThat(projectedVerbs())
                .as("a deferred verb that has since been given an endpoint must be taken off the list")
                .doesNotContainAnyElementsOf(DEFERRED_WITH_NO_FACE);
    }
}
