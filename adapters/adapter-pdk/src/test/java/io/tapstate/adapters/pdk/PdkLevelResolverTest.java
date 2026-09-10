package io.tapstate.adapters.pdk;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The PDK API-level resolve contract: judge a connector's declared level requirement against the
 * level this bridge provides, as a pure four-state decision (compatible / incompatible / unrecognized
 * version / undeclared). It only judges and names the levels; turning a refusal verdict into a coded
 * refuse-to-load error is the load path's job (the connector error domain), so this stays a
 * dependency-free pure function testable in isolation.
 */
class PdkLevelResolverTest {

    private static final int ENGINE = PdkApiLevels.ENGINE_LEVEL;

    // ---- three states ----

    @Test
    void compatibleWhenDeclaredLevelIsAtOrBelowEngine() {
        LevelResolution r = PdkLevelResolver.resolve(null, String.valueOf(ENGINE));
        assertThat(r.outcome()).isEqualTo(LevelOutcome.COMPATIBLE);
        assertThat(r.compatible()).isTrue();
        assertThat(r.requiredLevel()).isEqualTo(ENGINE);
        assertThat(r.engineLevel()).isEqualTo(ENGINE);
    }

    @Test
    void compatibleWhenDeclaredByFrozenBaselineVersion() {
        LevelResolution r = PdkLevelResolver.resolve("2.0.8", null);
        assertThat(r.outcome()).isEqualTo(LevelOutcome.COMPATIBLE);
        assertThat(r.requiredLevel()).isEqualTo(ENGINE);
    }

    @Test
    void incompatibleNamesTheRequiredLevelWhenAboveEngine() {
        int tooNew = ENGINE + 1;
        LevelResolution r = PdkLevelResolver.resolve(null, String.valueOf(tooNew));
        assertThat(r.outcome()).isEqualTo(LevelOutcome.INCOMPATIBLE);
        assertThat(r.compatible()).isFalse();
        // The verdict carries the required level so the load path can name it in a coded diagnosis.
        assertThat(r.requiredLevel()).isEqualTo(tooNew);
        assertThat(r.engineLevel()).isEqualTo(ENGINE);
    }

    @Test
    void undeclaredWhenNeitherVersionNorLevelIsGiven() {
        LevelResolution r = PdkLevelResolver.resolve(null, null);
        assertThat(r.outcome()).isEqualTo(LevelOutcome.UNDECLARED);
        assertThat(r.requiredLevel()).isNull();
        assertThat(r.engineLevel()).isEqualTo(ENGINE);
    }

    @Test
    void blankDeclarationsAreTreatedAsUndeclared() {
        assertThat(PdkLevelResolver.resolve("  ", "").outcome()).isEqualTo(LevelOutcome.UNDECLARED);
    }

    // ---- build-number drift within one base version is API-equivalent ----

    @Test
    void snapshotAndTimestampedBuildsResolveToTheBaselineLevel() {
        assertThat(PdkLevelResolver.resolve("2.0.8-SNAPSHOT", null).outcome())
                .isEqualTo(LevelOutcome.COMPATIBLE);
        assertThat(PdkLevelResolver.resolve("2.0.8-20260609.043233-3", null).outcome())
                .isEqualTo(LevelOutcome.COMPATIBLE);
    }

    @Test
    void baseVersionStripsTheSnapshotAndTimestampedQualifier() {
        assertThat(PdkApiLevels.baseVersion("2.0.8")).isEqualTo("2.0.8");
        assertThat(PdkApiLevels.baseVersion("2.0.8-SNAPSHOT")).isEqualTo("2.0.8");
        assertThat(PdkApiLevels.baseVersion("2.0.8-20260609.043233-3")).isEqualTo("2.0.8");
    }

    // ---- precedence: an explicit level wins over the version ----

    @Test
    void explicitLevelTakesPrecedenceOverVersion() {
        int tooNew = ENGINE + 1;
        // Version alone would be compatible; the explicit derived level is authoritative and wins.
        LevelResolution r = PdkLevelResolver.resolve("2.0.8", String.valueOf(tooNew));
        assertThat(r.outcome()).isEqualTo(LevelOutcome.INCOMPATIBLE);
        assertThat(r.requiredLevel()).isEqualTo(tooNew);
    }

    // ---- the inherited connector build lines resolve at the baseline level ----

    @Test
    void theInheritedConnectorVersionsResolveToTheBaselineLevel() {
        // 2.0.5 (mysql) and 2.0.7 (mongodb) are the two inherited connector build lines; they share the
        // baseline's API contract, so they resolve compatible at the same level rather than "too new".
        for (String version : new String[] {"2.0.5", "2.0.5-SNAPSHOT", "2.0.7", "2.0.7-SNAPSHOT"}) {
            LevelResolution r = PdkLevelResolver.resolve(version, null);
            assertThat(r.outcome()).as(version).isEqualTo(LevelOutcome.COMPATIBLE);
            assertThat(r.requiredLevel()).as(version).isEqualTo(ENGINE);
        }
    }

    @Test
    void auditedEnterpriseBuildsResolveToTheBaselineLevelWithoutOpeningFutureVersions() {
        for (String version : new String[] {"2.0.9", "2.0.9-SNAPSHOT", "2.0.9-20260902.031425-5"}) {
            LevelResolution result = PdkLevelResolver.resolve(version, null);
            assertThat(result.outcome()).as(version).isEqualTo(LevelOutcome.COMPATIBLE);
            assertThat(result.requiredLevel()).as(version).isEqualTo(ENGINE);
        }
        assertThat(PdkLevelResolver.resolve("2.0.10", null).outcome()).isEqualTo(LevelOutcome.UNKNOWN_VERSION);
    }

    // ---- an unrecognized declared version is a verdict (UNKNOWN_VERSION), not a bare crash ----

    @Test
    void unmappedDeclaredVersionResolvesToUnknownRatherThanCrashing() {
        // A connector's declared version the Tapstate-side table has no row for cannot be placed on the
        // level axis, but it is operator data — it gets a verdict the load path renders as a coded
        // refusal, never a bare stack (the earlier contract crashed here). No level is derived.
        LevelResolution r = PdkLevelResolver.resolve("9.9.9", null);
        assertThat(r.outcome()).isEqualTo(LevelOutcome.UNKNOWN_VERSION);
        assertThat(r.compatible()).isFalse();
        assertThat(r.requiredLevel()).isNull();
        assertThat(r.engineLevel()).isEqualTo(ENGINE);
    }

    @Test
    void theNonThrowingLookupReportsTheGapRatherThanThrowing() {
        assertThat(PdkApiLevels.levelOf("9.9.9")).isEmpty();
        assertThat(PdkApiLevels.levelOf("2.0.8")).hasValue(ENGINE);
    }

    // ---- malformed input and the bridge's own baseline stay bare-crash programmer/build errors ----

    @Test
    void malformedDeclaredLevelIsAProgrammerErrorNotAVerdict() {
        assertThatThrownBy(() -> PdkLevelResolver.resolve(null, "not-an-int"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void theStrictBaselineLevelLookupStillCrashesForAnUnregisteredBase() {
        // The strict variant is what the bridge takes for its OWN baseline: an unregistered base is a
        // build defect, not operator data, and must keep crashing loudly. Softening the connector path
        // to a verdict does not soften this one.
        assertThatThrownBy(() -> PdkApiLevels.level("9.9.9"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("9.9.9");
    }
}
