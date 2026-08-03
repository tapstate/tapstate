package io.tapstate.e2e;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The decisions the published-example sweep makes that need no run.
 *
 * <p>The sweep's closing guard asks whether a seed landed on something this run handed out. That
 * question is asked of an address, and an address can be built here - which matters, because the
 * guard's own failure case cannot be staged from an example: it would take a published example whose
 * resource points somewhere the run never provisioned, and the release manifest forbids publishing one.
 * So the reading is pinned here and the assertion that uses it is left to the sweep.
 */
class PublishedExamplesTest {

    @Test
    void anAddressCarryingADirectoryThisRunHandedOutIsRecognised() {
        EndpointAddress file = EndpointAddress.uri("/tmp/run-4711/target");

        assertThat(PublishedExamplesIT.namesOneOf(file, List.of("/tmp/run-4711/source", "/tmp/run-4711/target")))
                .isTrue();
    }

    /**
     * The case the guard exists for: a resource that ignored interpolation points at an endpoint this
     * run never minted. Read as "not one of ours", it is refused; read as "nothing to see", it would
     * shrink the seeded set and let a target sharing the source's store pass unremarked.
     */
    @Test
    void anAddressNamingSomethingThisRunNeverHandedOutIsNot() {
        EndpointAddress hardcoded = new EndpointAddress(
                "src_mysql",
                Map.of("host", "127.0.0.1", "port", "3306", "database", "someone_elses_database"));

        assertThat(PublishedExamplesIT.namesOneOf(
                        hardcoded, List.of("/tmp/run-4711/source", "/tmp/run-4711/target")))
                .isFalse();
    }

    /** A setting that is absent is not a match, and is not a crash either. */
    @Test
    void anAddressWithAMissingSettingIsReadWithoutFailing() {
        Map<String, Object> settings = new java.util.LinkedHashMap<>();
        settings.put("uri", null);
        settings.put("database", "/tmp/run-4711/source");

        assertThat(PublishedExamplesIT.namesOneOf(new EndpointAddress("src", settings),
                        List.of("/tmp/run-4711/source")))
                .isTrue();
    }
}
