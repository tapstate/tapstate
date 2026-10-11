package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class BenchmarkReturnRootClockWorkTest {
    @Test void metadata_upper_includes_hash_material_native_copies_and_its_own_bounded_json_work() {
        long hashes = 16L * 1024 * 1024 + 4L * 256 * 1024 + 5L * 8192;
        var upper = BenchmarkReturnRootClockWork.metadataUpper();
        assertThat(upper.requests()).isGreaterThan(2 * (hashes + 5) + 2053 + 1024);
        assertThat(upper.bytes()).isEqualTo(4 * (hashes + 16593) + 65536);
        assertThat(upper.elements()).isEqualTo(8 * (hashes + 16593) + 65536);
        assertThat(upper.passes()).isEqualTo(64);
        Map<String, Object> evidence = BenchmarkReturnRootClockWork.evidence();
        assertThat((Long) evidence.get("metadataJsonAsciiUpper")).isLessThan(65536);
        assertThat(evidence).containsEntry("actualColdInitializationObserved", false)
                .containsEntry("wholeMethodCostQualified", false).containsEntry("performanceAcceptanceEligible", false);
    }

    @Test void cold_setup_is_a_separate_once_only_source_tail_instead_of_hashing_cost_for_every_hot_read() {
        var cold = BenchmarkReturnRootClockWork.coldInitializationUpper();
        var metadata = BenchmarkReturnRootClockWork.metadataUpper();
        assertThat(cold.requests()).isEqualTo(2 * metadata.requests() + 64);
        assertThat(cold.bytes()).isEqualTo(2 * metadata.bytes() + 4096);
        assertThat(cold.calls()).isEqualTo(3);
    }
}
