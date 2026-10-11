package io.tapstate.app;

import io.tapstate.adapters.pdk.SeedConnectorSweep;
import io.tapstate.adapters.pdk.SeedOutcome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;

import java.nio.file.Path;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Objects;

/**
 * Sweeps the connector seed directory once at startup. On-prem keeps optional, best-effort seeds;
 * Cloud requires its complete verified release to register and load before becoming ready.
 */
final class SeedSweepRunner implements ApplicationRunner {

    private static final Logger LOG = LoggerFactory.getLogger(SeedSweepRunner.class);

    private final SeedConnectorSweep sweep;
    private final Path seedDir;
    private final CloudConnectorSeedReadiness cloudReadiness;

    SeedSweepRunner(SeedConnectorSweep sweep, Path seedDir) {
        this(sweep, seedDir, null);
    }

    SeedSweepRunner(SeedConnectorSweep sweep, Path seedDir, CloudConnectorSeedReadiness cloudReadiness) {
        this.sweep = Objects.requireNonNull(sweep, "sweep");
        this.seedDir = Objects.requireNonNull(seedDir, "seedDir");
        this.cloudReadiness = cloudReadiness;
    }

    @Override
    public void run(ApplicationArguments args) {
        List<SeedOutcome> outcomes;
        try {
            outcomes = sweep.sweep(seedDir);
        } catch (UncheckedIOException failure) {
            if (cloudReadiness != null) {
                throw CloudConnectorSeedReadiness.sweepUnavailable();
            }
            throw failure;
        }
        if (cloudReadiness != null) {
            cloudReadiness.verifyRegistrations(outcomes);
        }
        report(outcomes);
    }

    /** One log line per artifact: seeded, already registered, or not registered with the reason. */
    void report(List<SeedOutcome> outcomes) {
        for (SeedOutcome outcome : outcomes) {
            switch (outcome) {
                case SeedOutcome.Seeded seeded when seeded.outcome().newlyRegistered() ->
                        LOG.info("Seeded connector '{}' ({}) from {}",
                                seeded.outcome().registration().connectorId(),
                                seeded.outcome().registration().contentHash(),
                                seeded.artifact());
                case SeedOutcome.Seeded seeded ->
                        LOG.info("Seed connector '{}' ({}) is already registered; left {} as is",
                                seeded.outcome().registration().connectorId(),
                                seeded.outcome().registration().contentHash(),
                                seeded.artifact());
                case SeedOutcome.Failed failed ->
                        LOG.warn("Seed artifact {} was not registered", failed.artifact(), failed.cause());
            }
        }
    }
}
