package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The file endpoint driver, checked against files alone - no product, no connector.
 *
 * <p>This is the harness's independent reading of a target, so it is checked the way the product
 * never gets to define: by writing bytes with this driver and reading them back as text, and by
 * reading text this driver did not write. If this driver and the connector shared any code, a
 * specification's count would agree with the connector by construction and would keep agreeing while
 * nothing crossed the product at all.
 */
class FileEndpointsTest {

    @TempDir
    private Path directory;

    private final FileEndpoints endpoints = new FileEndpoints();

    /** The temporary directory this test drives, in the shape a driver is handed it. */
    private EndpointAddress at() {
        return EndpointAddress.uri(directory.toString());
    }

    @Test
    void seedingWritesAHeaderAndTheRowsNumberedFromOne() throws IOException {
        endpoints.seed(at(), "orders", SeedRows.generated(3));

        assertThat(lines("orders")).containsExactly("id,seq", "1,1", "2,2", "3,3");
    }

    @Test
    void seedingReplacesWhateverTheTableHeld() throws IOException {
        endpoints.seed(at(), "orders", SeedRows.generated(5));
        endpoints.seed(at(), "orders", SeedRows.generated(2));

        assertThat(lines("orders")).containsExactly("id,seq", "1,1", "2,2");
    }

    @Test
    void countingReadsTheRowsBackWithoutTheHeader() {
        endpoints.seed(at(), "orders", SeedRows.generated(3));

        assertThat(endpoints.count(at(), "orders")).isEqualTo(3L);
    }

    /**
     * The reading that makes an {@code await} on a target meaningful: before the product writes it, the
     * target table does not exist, and the honest count of a table that is not there is zero. Reporting
     * it as an error instead would turn every wait for a first write into a failure.
     */
    @Test
    void aTableNoOneHasWrittenYetCountsZero() {
        assertThat(endpoints.count(at(), "never_written")).isZero();
    }

    @Test
    void countingReadsRowsThisDriverDidNotWrite() throws IOException {
        Files.writeString(directory.resolve("orders.csv"), "id,seq\n7,7\n8,8\n");

        assertThat(endpoints.count(at(), "orders")).isEqualTo(2L);
    }

    @Test
    void insertingAppendsRowsAfterTheHighestIdTheTableHolds() throws IOException {
        endpoints.seed(at(), "orders", SeedRows.generated(3));

        endpoints.cdc(at(), "orders", CdcOp.INSERT, 2);

        assertThat(lines("orders")).containsExactly("id,seq", "1,1", "2,2", "3,3", "4,4", "5,5");
    }

    @Test
    void deletingRemovesTheLowestIdsAndLowersTheCount() throws IOException {
        endpoints.seed(at(), "orders", SeedRows.generated(4));

        endpoints.cdc(at(), "orders", CdcOp.DELETE, 2);

        assertThat(lines("orders")).containsExactly("id,seq", "3,3", "4,4");
        assertThat(endpoints.count(at(), "orders")).isEqualTo(2L);
    }

    /** An update changes rows without adding or removing any, so a count cannot witness it. */
    @Test
    void updatingRewritesTheSequenceOfTheLowestIdsAndLeavesTheCountAlone() throws IOException {
        endpoints.seed(at(), "orders", SeedRows.generated(3));

        endpoints.cdc(at(), "orders", CdcOp.UPDATE, 2);

        assertThat(lines("orders")).containsExactly("id,seq", "1,-1", "2,-2", "3,3");
        assertThat(endpoints.count(at(), "orders")).isEqualTo(3L);
    }

    /**
     * Changing a table that was never seeded is an authoring mistake, not a change: the specification
     * says produce changes "against a table that is already seeded", and silently creating one here
     * would let a specification whose seed and cdc name different tables pass by accident.
     */
    @Test
    void changingATableNoOneSeededRefusesRatherThanCreatingIt() {
        assertThatThrownBy(() -> endpoints.cdc(at(), "orders", CdcOp.INSERT, 1))
                .isInstanceOf(EnvelopeException.class)
                .hasMessageContaining("orders")
                .hasMessageContaining("not been seeded");
    }

    /**
     * The refusal that used to live in the binding, which read one setting and rejected a resource without
     * it. Now that the whole mapping is handed over, only the driver knows which setting it needed - so the
     * refusal belongs here, and has to keep naming both the resource at fault and the setting it lacked.
     * Left untested it would decay into a bare NullPointerException, and an author would learn that
     * something was null rather than that their resource carries no address.
     */
    @Test
    void anAddressCarryingNoUriRefusesAndNamesWhatItLacks() {
        EndpointAddress addressless = new EndpointAddress("src_jdbc", Map.of("host", "localhost"));

        assertThatThrownBy(() -> endpoints.count(addressless, "orders"))
                .isInstanceOf(EnvelopeException.class)
                .hasMessageContaining("src_jdbc")
                .hasMessageContaining("uri")
                .hasMessageContaining("host");
    }

    @Test
    void anEndpointNamingSomethingThatIsNotADirectoryRefuses() {
        EndpointAddress absent = EndpointAddress.uri(directory.resolve("absent").toString());
        assertThatThrownBy(() -> endpoints.count(absent, "orders"))
                .isInstanceOf(EnvelopeException.class)
                .hasMessageContaining("is not a directory");
    }

    private List<String> lines(String table) throws IOException {
        return Files.readAllLines(directory.resolve(table + ".csv"));
    }
}
