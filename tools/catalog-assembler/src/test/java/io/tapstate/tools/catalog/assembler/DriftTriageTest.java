package io.tapstate.tools.catalog.assembler;

import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * When an upstream drift is worth opening a pull request for, and when it waits for company.
 */
class DriftTriageTest {

    @Test
    void holdsADriftThatTouchesNoSupportedConnector() {
        assertThat(DriftTriage.decide(List.of("zoho-desk"), 1, false)).isEqualTo(DriftTriage.Decision.HOLD);
    }

    @Test
    void opensAsSoonAsASupportedConnectorDrifts() {
        assertThat(DriftTriage.decide(List.of("zoho-desk", "mysql"), 1, false)).isEqualTo(DriftTriage.Decision.OPEN);
    }

    @Test
    void opensAnywayOnceHeldDriftHasWaitedTheFallbackDays() {
        assertThat(DriftTriage.decide(List.of("zoho-desk"), 7, false)).isEqualTo(DriftTriage.Decision.OPEN);
    }

    @Test
    void hasNothingToOpenWhenNothingDriftedHoweverLongItHasBeen() {
        assertThat(DriftTriage.decide(List.of(), 30, false)).isEqualTo(DriftTriage.Decision.NOTHING);
    }

    @Test
    void opensRatherThanHoldingWhileAPullRequestIsAlreadyOpen() {
        assertThat(DriftTriage.decide(List.of("zoho-desk"), 1, true)).isEqualTo(DriftTriage.Decision.OPEN);
    }

    @Test
    void stillHasNothingToOpenWhenNothingDriftedAndAPullRequestIsOpen() {
        assertThat(DriftTriage.decide(List.of(), 1, true)).isEqualTo(DriftTriage.Decision.NOTHING);
    }
}
