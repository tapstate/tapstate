package io.tapstate.runtime.engine.nest;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.junit.jupiter.api.Test;

/**
 * The one place a nest is told what it may hold. Every knob here shares two properties, and both of them
 * are why this could not stay a static method handing back a map configuration.
 *
 * <p><b>Per namespace, not per job.</b> One tree's documents differ in width by orders of magnitude - a
 * customer document holds as many elements as that customer has policies and claims against them, where a
 * document of a narrow tree beside it holds a handful - so a single number covering both is either too
 * small for the wide one or too large to catch the narrow one growing without bound. A limit that applies
 * to everything is a limit that catches nothing.
 *
 * <p><b>It travels.</b> The limit is decided where the job is assembled and enforced on whichever member
 * runs the vertex, so it has to survive the trip between them. A limit that stayed behind would leave
 * every vertex running unguarded while the configuration that was meant to guard them reads as set.
 */
class ACapacityLimitIsSetPerNamespaceAndTravelsWithTheJobTest {

    private static final String CUSTOMERS = "nest.p.doc.$root";
    private static final String ORDERS = "nest.p.other.$root";

    @Test
    void aNamespaceNobodyConfiguredTakesTheDefault() {
        NestSettings settings = NestSettings.defaults();

        assertThat(settings.elementsAllowedIn(CUSTOMERS)).isEqualTo(NestSettings.DEFAULT_ELEMENT_LIMIT);
    }

    @Test
    void aNamespaceGivenALimitOfItsOwnTakesThatOne() {
        NestSettings settings = NestSettings.defaults().withElementLimit(CUSTOMERS, 12L);

        assertThat(settings.elementsAllowedIn(CUSTOMERS)).isEqualTo(12L);
    }

    @Test
    void aLimitSetOnOneNestDoesNotReachAnother() {
        NestSettings settings = NestSettings.defaults().withElementLimit(CUSTOMERS, 12L);

        assertThat(settings.elementsAllowedIn(ORDERS))
                .describedAs("another nest's documents are a different width and keep their own limit")
                .isEqualTo(NestSettings.DEFAULT_ELEMENT_LIMIT);
    }

    @Test
    void everyLimitReachesTheMemberThatWillEnforceIt() throws Exception {
        NestSettings settings =
                NestSettings.defaults().withElementLimit(CUSTOMERS, 12L).withElementLimit(ORDERS, 34L);

        NestSettings arrived = roundTripped(settings);

        assertThat(arrived.elementsAllowedIn(CUSTOMERS)).isEqualTo(12L);
        assertThat(arrived.elementsAllowedIn(ORDERS)).isEqualTo(34L);
    }

    /** What the job submission does to anything the vertices are configured with, and nothing more. */
    private static NestSettings roundTripped(NestSettings settings) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(settings);
        }
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (NestSettings) in.readObject();
        }
    }
}
