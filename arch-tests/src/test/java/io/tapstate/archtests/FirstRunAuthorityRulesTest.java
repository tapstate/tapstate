package io.tapstate.archtests;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaMethodCall;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The guided first run has one orchestration authority, and this gate pins it. {@code up} is a
 * composite of the calls the individual online verbs make - apply, discover, start, status - and it
 * reaches the server only through the session's shared cores, never by dialling the control plane
 * itself; and the guided {@code new} reaches the saved servers and the saved sign-ins only through
 * the services every other verb uses, never by opening the files behind them. A second way to the
 * server, or a second writer of the context and session files, turns this red rather than quietly
 * forking a lifecycle.
 *
 * <p>Each rule first asserts the call is actually made somewhere (a positive control) and only then
 * asserts no caller sits in the forbidden ring - written this way rather than as a plain
 * {@code noClasses()} ban so a rename cannot turn the rule vacuously green.
 *
 * <p>Classes are named rather than referenced: the front end's types are package-private on purpose,
 * and this gate must not be the reason one becomes public.
 */
class FirstRunAuthorityRulesTest {

    private static final String CLI = "io.tapstate.cli.";
    private static final String CONTROL_PLANE_CLIENT = CLI + "ControlPlaneClient";
    private static final String REPL = CLI + "Repl";
    private static final String UP_RUN = REPL + "$UpRun";
    private static final String CONTEXT_STORE = CLI + "ContextConfigStore";
    private static final String AUTH_STORE = CLI + "AuthFileStore";

    /** The session's shared cores, the only way {@code up}'s stages may reach the server. */
    private static final Set<String> CORES = Set.of(
            "applyDrafts", "discoverSchemaFor", "readSchema", "listConnectors",
            "lifecycleFor", "readStatus", "testConnectionFor");

    /**
     * The classes that make up the guided path: the questions, the recipes, the files they write, the
     * summary, the secrets file and the local stack. None of them may open a context or a session file.
     */
    private static final Set<String> GUIDED_PATH = Set.of(
            "GuidedNew", "LocalStack", "RecipeRun", "RecipeSupport", "NewCmd", "FirstRunSummary", "DotEnv");

    private static JavaClasses tapstateClasses;

    /** A call to any method of the control plane client, whichever implementation is behind it. */
    private static final DescribedPredicate<JavaMethodCall> TALKS_TO_THE_CONTROL_PLANE =
            new DescribedPredicate<>("a call to the control plane client") {
                @Override
                public boolean test(JavaMethodCall call) {
                    return call.getTargetOwner().isAssignableTo(CONTROL_PLANE_CLIENT);
                }
            };

    /** A call to any method of the store behind the saved servers or the store behind the saved sign-ins. */
    private static final DescribedPredicate<JavaMethodCall> OPENS_A_SAVED_STATE_FILE =
            new DescribedPredicate<>("a call to the context store or the auth store") {
                @Override
                public boolean test(JavaMethodCall call) {
                    String owner = call.getTargetOwner().getName();
                    return owner.equals(CONTEXT_STORE) || owner.equals(AUTH_STORE);
                }
            };

    /** A call from {@code up}'s run to one of the session's shared cores. */
    private static final DescribedPredicate<JavaMethodCall> UP_CALLS_A_CORE =
            new DescribedPredicate<>("a call from up's run to a shared core") {
                @Override
                public boolean test(JavaMethodCall call) {
                    return isUpRun(call.getOriginOwner().getName())
                            && call.getTargetOwner().getName().equals(REPL)
                            && CORES.contains(call.getName());
                }
            };

    @BeforeAll
    static void importTapstateClasses() {
        tapstateClasses = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("io.tapstate");
    }

    @Test
    @DisplayName("up reaches the server only through the session's shared cores")
    void upNeverTalksToTheControlPlaneOnItsOwn() {
        // Positive control on the forbidden ring itself: up's run exists under this name and chains
        // the cores. Without this, renaming UpRun would leave the ban below with nothing to ban.
        assertThat(calls(UP_CALLS_A_CORE))
                .as("positive control: up's run must call the shared cores, or this gate is checking nothing")
                .isNotEmpty();

        List<JavaMethodCall> calls = calls(TALKS_TO_THE_CONTROL_PLANE);
        assertThat(calls)
                .as("positive control: the session must call the control plane somewhere, or this gate is checking nothing")
                .anySatisfy(call -> assertThat(call.getOriginOwner().getName()).isEqualTo(REPL));
        assertThat(offenders(calls, call -> isUpRun(call.getOriginOwner().getName())))
                .as("up's stages may reach the server only through the session's shared cores - "
                        + "applyDrafts, discoverSchemaFor, readSchema, listConnectors, lifecycleFor, readStatus, "
                        + "testConnectionFor - so that there is one lifecycle, whichever verb asked; "
                        + "these dial the control plane themselves")
                .isEmpty();
    }

    @Test
    @DisplayName("the guided path never opens a saved context or session file itself")
    void theGuidedPathNeverTouchesSavedStateFiles() {
        List<JavaMethodCall> calls = calls(OPENS_A_SAVED_STATE_FILE);
        assertThat(calls)
                .as("positive control: the stores must be called somewhere in the front end, or this gate is checking nothing")
                .anySatisfy(call -> assertThat(call.getOriginOwner().getName()).startsWith(CLI));
        assertThat(offenders(calls, call -> isGuidedPath(outermostSimpleName(call.getOriginOwner().getName()))))
                .as("the guided first run reaches the saved servers and sign-ins only through ContextManager, "
                        + "ContextResolver and AuthService, so that what up resumes is exactly what new signed in; "
                        + "these open the store themselves")
                .isEmpty();
    }

    /** The calls in the forbidden ring, each named by where it is made and what it calls. */
    private static List<String> offenders(List<JavaMethodCall> calls, Predicate<JavaMethodCall> forbidden) {
        return calls.stream()
                .filter(forbidden)
                .map(call -> call.getOrigin().getFullName() + " -> " + call.getTarget().getFullName()
                        + " (line " + call.getLineNumber() + ")")
                .toList();
    }

    private static List<JavaMethodCall> calls(DescribedPredicate<JavaMethodCall> of) {
        return tapstateClasses.stream()
                .flatMap(type -> type.getMethodCallsFromSelf().stream())
                .filter(of::test)
                .toList();
    }

    /** {@code Repl$UpRun} and anything nested in it - a lambda's or an inner record's call is still up's. */
    private static boolean isUpRun(String className) {
        return className.equals(UP_RUN) || className.startsWith(UP_RUN + "$");
    }

    /** The guided classes by name, and every recipe: a class of the front end whose name ends in Recipe. */
    private static boolean isGuidedPath(String outermostSimpleName) {
        return GUIDED_PATH.contains(outermostSimpleName) || outermostSimpleName.endsWith("Recipe");
    }

    /** The top-level class a call sits in, so a nested helper counts as its enclosing class. */
    private static String outermostSimpleName(String className) {
        String simple = className.substring(className.lastIndexOf('.') + 1);
        int nested = simple.indexOf('$');
        return nested < 0 ? simple : simple.substring(0, nested);
    }
}
