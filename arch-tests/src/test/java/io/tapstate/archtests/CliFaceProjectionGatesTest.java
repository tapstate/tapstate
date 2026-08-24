package io.tapstate.archtests;

import io.tapstate.cli.Cli;
import io.tapstate.control.core.ControlOperations;
import io.tapstate.control.core.Frontend;
import io.tapstate.control.core.Operation;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.TreeSet;

import static java.util.stream.Collectors.toCollection;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The CLI face-projection gates: the peer of {@link ControlFaceProjectionGatesTest}, for the face the
 * registry actually opens. The registry is the single source of truth for what the process can do; a
 * face composes registered operations and never invents one, and an operation the registry opens on a
 * face must actually be reachable there.
 *
 * <p>It lives here rather than in the {@code cli} module because the relation it checks spans two
 * modules that must not link: the CLI reaches a server over HTTP and never imports the control ring
 * (rule R6), so only a place that may see both — this one — can compare its verb table against the
 * registry at all. That gap is what let an operation the registry declares open sit with no command
 * behind it, while a control-core test named for the CLI surface passed green without ever loading the
 * CLI.
 *
 * <p>The verb table is read by building the real command line, not by reading a list that claims to
 * describe it — the gate asserts against the thing the user actually types into.
 */
class CliFaceProjectionGatesTest {

    /** Every verb name the built command table actually answers to. */
    private static Set<String> commandTable() {
        return new TreeSet<>(Cli.newCommandLine().getSubcommands().keySet());
    }

    /** Every operation id the CLI face declares it projects onto a verb. */
    private static Set<String> projectedOperations() {
        return new TreeSet<>(Cli.VERB_BY_OPERATION.keySet());
    }

    /** Every operation the registry opens on the CLI face. */
    private static Set<String> registeredOperations() {
        return ControlOperations.registry().exposedOn(Frontend.CLI).stream()
                .map(Operation::id)
                .collect(toCollection(TreeSet::new));
    }

    @Test
    @DisplayName("every operation the CLI projects is a registered one")
    void everyProjectedOperationIsRegistered() {
        assertThat(projectedOperations())
                .as("a verb may only project a registered, CLI-exposed operation — a face composes "
                        + "registered operations, it never invents one")
                .isSubsetOf(registeredOperations());
    }

    @Test
    @DisplayName("every operation the CLI projects has a verb on the command table")
    void everyProjectedOperationHasACommand() {
        assertThat(commandTable())
                .as("an operation projected under a verb name that the command table does not answer to "
                        + "is a projection of nothing — the declaration and the table must agree")
                .containsAll(Cli.VERB_BY_OPERATION.values());
    }

    /**
     * The registered operations the CLI does not project, each a deliberate deferral rather than an
     * oversight. The security domain has no face anywhere — no command and no endpoint — so nothing
     * invokes those verbs today; the first administrator is created through the bootstrap entry point
     * instead. {@code cluster.members} is routed over HTTP but answers 501 with no topology service
     * behind it, so a command for it would surface a stub rather than a cluster. The frontend-only source
     * and connector detail and icon faces are HTTP contracts with no command shape yet.
     *
     * <p>An entry here is a reviewed decision, not a running to-do list: an operation added to the
     * registry with no verb must turn this gate red, and deleting its entry is how it earns one.
     */
    private static final Set<String> DEFERRED_WITH_NO_VERB = Set.of(
            "connector.get",
            "connector.icon",
            "cluster.members",
            "source.create", "source.delete", "source.draft", "source.get", "source.list", "source.update",
            "pipeline.get", "pipeline.list", "pipeline.layout.get", "pipeline.layout.update",
            "user.create", "user.passwd", "user.list");

    @Test
    @DisplayName("every registered operation is reachable from the CLI, bar the deferred ones")
    void everyRegisteredOperationIsProjectedOntoAVerb() {
        Set<String> unreachable = new TreeSet<>(registeredOperations());
        unreachable.removeAll(projectedOperations());
        unreachable.removeAll(DEFERRED_WITH_NO_VERB);

        assertThat(unreachable)
                .as("the CLI is the face the registry opens at this stage, so an operation it declares "
                        + "open there with no verb behind it cannot be invoked by a user at all — give it "
                        + "a verb, or record it as deliberately deferred")
                .isEmpty();
    }

    @Test
    @DisplayName("the deferred list holds only registered operations that really have no verb")
    void theDeferredListDoesNotRot() {
        // A deferral that has quietly come true would otherwise sit here forever, silently excusing an
        // operation that no longer needs excusing — and hiding the next real gap behind a stale name.
        assertThat(registeredOperations())
                .as("a deferred operation must still be a registered one")
                .containsAll(DEFERRED_WITH_NO_VERB);
        assertThat(projectedOperations())
                .as("a deferred operation that has since been given a verb must be taken off the list")
                .doesNotContainAnyElementsOf(DEFERRED_WITH_NO_VERB);
    }
}
