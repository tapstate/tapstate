package io.tapstate.core.event;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The carrier a converted row value travels in, and the one operation every reader of a row value
 * performs on it.
 *
 * <p>Its whole reason to exist is that the two sides of a row want different things out of one value:
 * everything that reads a row wants what the value converts to, and only the write side wants the
 * object it converted from. What makes it worth a type rather than a convention is that using one
 * without unwrapping fails silently — it compares by identity, so a comparison is false and a key
 * matches nothing, with no error on either path.
 */
class ConvertedValueTest {

    /** Stands in for a driver's own object; its identity is what a carrier would be compared by. */
    private record DriverKey(String hex) {
    }

    @Test
    void unwrappingYieldsTheValueRatherThanTheCarrier() {
        ConvertedValue carried = new ConvertedValue("64f0c0de", new DriverKey("64f0c0de"));

        assertThat(ConvertedValue.unwrap(carried)).isEqualTo("64f0c0de");
    }

    @Test
    void aValueThatIsNotCarriedIsItsOwnAnswer() {
        assertThat(ConvertedValue.unwrap(5L)).isEqualTo(5L);
        assertThat(ConvertedValue.unwrap(null)).isNull();
    }

    @Test
    void unwrappingReachesIntoDocumentsAndArrays() {
        Object nested = Map.of(
                "doc", Map.of("_id", new ConvertedValue("aa", new DriverKey("aa"))),
                "keys", List.of(new ConvertedValue("bb", new DriverKey("bb"))));

        // A key one level down is as reachable from a join or an expression as a top-level one; an
        // unwrapping that stopped at the top would leave every nested one comparing by identity.
        assertThat(ConvertedValue.unwrap(nested))
                .isEqualTo(Map.of("doc", Map.of("_id", "aa"), "keys", List.of("bb")));
    }

    @Test
    void aContainerWithNothingCarriedInItIsHandedBackUncopied() {
        Map<String, Object> row = Map.of("qty", 5L, "name", "eu");

        // Every row on every chain passes through this, including the overwhelming majority that met
        // no conversion at all. Copying those would put a per-row allocation on the hot path, and
        // nothing in the build measures allocation, so it would stay invisible until a large read.
        assertThat(ConvertedValue.unwrap(row)).isSameAs(row);
    }

    @Test
    void unwrappingARowKeepsItsAbsenceApartFromAnEmptyOne() {
        assertThat(ConvertedValue.unwrapRow(null)).isNull();
        assertThat(ConvertedValue.unwrapRow(Map.of())).isEmpty();
    }

    @Test
    void aCarrierWithoutTheObjectItCameFromIsRefused() {
        // The write side reads that object and has no other source for it. A carrier holding null
        // would travel the whole chain and fail at the target, one pipeline-length away from here.
        assertThatThrownBy(() -> new ConvertedValue("64f0c0de", null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void aCarrierSurvivesTheWireTheRowsTravelOn() throws Exception {
        // Rows are handed to serializers that write each value as an object, and a row crosses both a
        // cluster and the resumable store before it reaches a target - which is the only reader of the
        // object inside. A carrier that could not be written would take the whole row with it.
        ConvertedValue carried = new ConvertedValue("64f0c0de", "the-origin");

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(carried);
        }
        Object read;
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            read = in.readObject();
        }

        assertThat(read).isEqualTo(carried);
    }
}
