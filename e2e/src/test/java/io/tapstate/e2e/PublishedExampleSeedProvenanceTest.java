package io.tapstate.e2e;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class PublishedExampleSeedProvenanceTest {

    @Test
    void creatingAnEmptyTargetTableCannotSupplyTheRowsTheExampleClaimsToHaveDelivered() {
        Set<String> seeded = new LinkedHashSet<>();
        PublishedExamplesIT.recordSeededStore(seeded,
                new Seed(new TableAlias("src_db", "orders"), SeedRows.generated(12)), Optional.of("source"));
        PublishedExamplesIT.recordSeededStore(seeded,
                new Seed(new TableAlias("tgt_db", "orders"), SeedRows.generated(0)), Optional.of("target"));

        assertThat(seeded).containsExactly("source");
    }

    @Test
    void evenOneTargetRowMakesTheIndependentDeliveryClaimIneligible() {
        Set<String> seeded = new LinkedHashSet<>();
        PublishedExamplesIT.recordSeededStore(seeded,
                new Seed(new TableAlias("tgt_db", "orders"), SeedRows.generated(1)), Optional.of("target"));

        assertThat(seeded).containsExactly("target");
    }
}
