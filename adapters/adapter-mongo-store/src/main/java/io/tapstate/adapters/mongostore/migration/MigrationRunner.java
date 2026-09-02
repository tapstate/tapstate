package io.tapstate.adapters.mongostore.migration;

import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.ChangeSet;
import io.tapstate.adapters.mongostore.MigrationError;
import io.tapstate.adapters.mongostore.SystemMetaStore;
import io.tapstate.core.common.TapstateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Brings the system data up to the shape this build expects, on the way in — before anything is
 * allowed to read or write it.
 *
 * <p>It runs inside the verified connection rather than beside it. Everything that touches the store
 * gets its handle from that connection, so putting the migration inside it makes every one of those
 * things come after the migration without anybody having to remember to order them; outside it, the
 * ordering would be a line of wiring somebody could omit, and omitting it would not fail.
 *
 * <p>Members are kept apart by a conditional update on the one metadata document, not by anything the
 * cluster provides: at this point in startup the cluster does not exist yet. A member that loses the
 * race waits for the winner to finish and then carries on starting. A member that stops saying it is
 * alive is taken over, and the epoch it held stops matching, so its late writes are refused rather
 * than landing behind the member that replaced it.
 *
 * <p>There is no way back. Changesets only move forward, and a store that has been moved past what a
 * build understands is refused by that build rather than read as if it had not been.
 */
public final class MigrationRunner {

    private static final Logger LOG = LoggerFactory.getLogger(MigrationRunner.class);

    /**
     * Every changeset there is, in the order they run. The release path reads this package to check a
     * patch release for an older line is not carrying one of these, so what lives here is exactly the
     * changesets and this runner -- shared types belong in the package above.
     */
    private static final List<ChangeSet> CHANGE_SETS = List.of(new V1BaselineIndexes());

    /**
     * The highest version this build knows. A store above it is one this build must not open: it was
     * written by a later build and reading it here would mean interpreting a shape nobody described to
     * this code.
     */
    public static final int SUPPORTED_VERSION =
            CHANGE_SETS.stream().mapToInt(ChangeSet::version).max().orElse(0);

    /** The version of a store nothing has ever migrated, which is every store older than this scheme. */
    private static final int UNMIGRATED = 0;

    /** How long a holder may go quiet before another member is entitled to take the lock from it. */
    static final Duration LOCK_TTL = Duration.ofSeconds(60);

    /** How long a member waits for the holder to finish before refusing to start. */
    static final Duration WAIT_TIMEOUT = Duration.ofMinutes(5);

    /** How often the holder says it is still alive. Comfortably inside the interval above. */
    private static final Duration HEARTBEAT_INTERVAL = LOCK_TTL.dividedBy(3);

    /** How often a waiting member looks again. Short enough not to add noticeably to a start. */
    private static final Duration POLL_INTERVAL = Duration.ofMillis(500);

    private MigrationRunner() {
    }

    /**
     * Brings {@code database} to {@link #SUPPORTED_VERSION}, or refuses to let this process start.
     *
     * <p>On the overwhelmingly common path -- a store already at this version -- it reads one document
     * and returns. That path has to stay this cheap: it is on every start, including every restart of
     * an install nobody is upgrading.
     */
    public static void migrate(MongoDatabase database) {
        migrate(database, CHANGE_SETS, LOCK_TTL, WAIT_TIMEOUT, Clock.systemUTC());
    }

    /**
     * The same, with the changesets, the two intervals and the clock given rather than assumed.
     * Package-visible so that waiting, being taken over, and a changeset failing part way through can
     * each be put in front of a real server -- states this build's own two changesets cannot be made to
     * reach, and which would otherwise be reasoned about rather than witnessed.
     */
    static void migrate(MongoDatabase database, List<ChangeSet> changeSets,
            Duration lockTtl, Duration waitTimeout, Clock clock) {
        int supported = highestVersionIn(changeSets);
        SystemMetaStore meta = new SystemMetaStore(database);
        Object stored = meta.installedVersion().orElse(null);
        int installed = readVersion(stored);
        if (installed == supported) {
            return;
        }
        if (installed > supported) {
            throw new TapstateException(MigrationError.DATA_NEWER_THAN_BINARY,
                    Map.of("installed", String.valueOf(stored),
                            "supported", String.valueOf(supported)), null);
        }
        bringForward(database, meta, changeSets, supported, lockTtl, waitTimeout, clock);
    }

    private static int highestVersionIn(List<ChangeSet> changeSets) {
        return changeSets.stream().mapToInt(ChangeSet::version).max().orElse(UNMIGRATED);
    }

    /** What the store is at, and what has not run yet. The read behind the inspection commands. */
    public static Status inspect(MongoDatabase database) {
        int installed = readVersion(new SystemMetaStore(database).installedVersion().orElse(null));
        List<String> pending = new ArrayList<>();
        for (ChangeSet changeSet : CHANGE_SETS) {
            if (changeSet.version() > installed) {
                pending.add(changeSet.changeSetName());
            }
        }
        return new Status(installed, SUPPORTED_VERSION, pending);
    }

    /** Every changeset this build carries, in order. Read by the inspection commands. */
    public static List<ChangeSet> changeSets() {
        return CHANGE_SETS;
    }

    /**
     * Reads the stored value as a version. Only this runner ever writes the field, so anything there
     * that is not a number was written by something else; treating it as beyond every known version is
     * what turns that into a refusal rather than a silent decision that the data is not newer.
     */
    private static int readVersion(Object stored) {
        if (stored == null) {
            return UNMIGRATED;
        }
        return stored instanceof Number number ? number.intValue() : Integer.MAX_VALUE;
    }

    /** Takes the lock and runs what is pending, or waits for whoever holds it to finish doing so. */
    private static void bringForward(MongoDatabase database, SystemMetaStore meta,
            List<ChangeSet> changeSets, int supported, Duration lockTtl, Duration waitTimeout, Clock clock) {
        String member = ManagementFactory.getRuntimeMXBean().getName();
        Instant deadline = clock.instant().plus(waitTimeout);
        while (true) {
            OptionalLong epoch = meta.tryAcquire(member, lockTtl, clock.instant());
            if (epoch.isPresent()) {
                applyPending(database, meta, changeSets, epoch.getAsLong());
                return;
            }
            // Somebody else is doing it. Their finishing is what releases this member to start, and
            // their going quiet for longer than the lock lives is what lets this member take over.
            if (readVersion(meta.installedVersion().orElse(null)) >= supported) {
                return;
            }
            if (!clock.instant().isBefore(deadline)) {
                SystemMetaStore.Lock held = meta.lock().orElse(null);
                throw new TapstateException(MigrationError.LOCK_TIMEOUT,
                        Map.of("holder", held == null ? "unknown" : held.owner(),
                                "since", held == null ? "unknown" : held.since().toString()), null);
            }
            sleep(POLL_INTERVAL);
        }
    }

    /** Runs every changeset the store has not had yet, recording each as it succeeds. */
    private static void applyPending(MongoDatabase database, SystemMetaStore meta,
            List<ChangeSet> changeSets, long epoch) {
        ScheduledExecutorService heartbeat = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "system-data-migration-heartbeat");
            thread.setDaemon(true);
            return thread;
        });
        try {
            heartbeat.scheduleAtFixedRate(() -> meta.heartbeat(epoch, Instant.now()),
                    HEARTBEAT_INTERVAL.toMillis(), HEARTBEAT_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
            int installed = readVersion(meta.installedVersion().orElse(null));
            for (ChangeSet changeSet : changeSets) {
                if (changeSet.version() <= installed) {
                    continue;
                }
                runOne(database, meta, epoch, changeSet);
            }
        } finally {
            heartbeat.shutdownNow();
            meta.release(epoch);
        }
    }

    /** One changeset, with its outcome recorded the moment it succeeds rather than at the end of them all. */
    private static void runOne(MongoDatabase database, SystemMetaStore meta, long epoch, ChangeSet changeSet) {
        String name = changeSet.changeSetName();
        LOG.info("system data changeset {} (version {}) starting", name, changeSet.version());
        long startedAt = System.nanoTime();
        try {
            changeSet.up(database);
        } catch (RuntimeException e) {
            // The recorded version stays at the changeset before this one, so the next start runs this
            // one again from the top -- which is why every changeset has to be re-runnable.
            throw new TapstateException(MigrationError.CHANGESET_FAILED,
                    Map.of("changeset", name, "cause", String.valueOf(e.getMessage())), e);
        }
        if (!meta.recordVersion(epoch, changeSet.version())) {
            // The lock was taken from this member while the changeset ran, so another member is already
            // changing the same store. Recording the version now would overwrite whatever it has done.
            throw new TapstateException(MigrationError.CHANGESET_FAILED,
                    Map.of("changeset", name,
                            "cause", "the migration lock was taken over while this changeset was running"),
                    null);
        }
        LOG.info("system data changeset {} (version {}) completed in {} ms",
                name, changeSet.version(), (System.nanoTime() - startedAt) / 1_000_000L);
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while waiting for the system data migration", e);
        }
    }

    /** Where the store is and what has not run against it yet. */
    public record Status(int installed, int supported, List<String> pending) {
        public Status {
            pending = List.copyOf(pending);
        }
    }
}
