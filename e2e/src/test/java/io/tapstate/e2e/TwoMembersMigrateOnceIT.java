package io.tapstate.e2e;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.ChangeSet;
import io.tapstate.adapters.mongostore.SystemMetaStore;
import io.tapstate.adapters.mongostore.migration.MigrationRunner;
import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Two members starting on one store that has never been migrated, and only one of them brings it
 * forward.
 *
 * <p>The keeping-apart itself is already witnessed a level down, deterministically, against injected
 * changesets that can be made to wait: that is where a lock is properly tested. What no level down
 * can answer is whether the shipped server has the migration wired into its start at all, and whether
 * two of them - two real processes, two JVMs, no shared memory to accidentally coordinate through -
 * end up with one store brought forward once. A server that never called the runner would pass every
 * test the adapter has and fail here.
 *
 * <p>The two members do overlap inside the migration, which was the doubt worth settling before this
 * case was trusted: they are launched together and take about the same time to get as far as the
 * store, so the second arrives while the first is still inside. Measured by making the lock
 * unconditional - the filter that only takes a free or expired lock replaced by one that always takes
 * it - and running this case four times: it failed all four. What fails is the start, not the count.
 * The member that lost the lock finished a changeset, found the epoch it held no longer current, and
 * refused rather than record over the member that had taken over.
 *
 * <p>So the count below is a second guard rather than the one that catches a missing lock, and it is
 * worth saying which fault it is for: a changeset that ran twice because it selects the documents to
 * act on by a marker saying whether they have been done, rather than by the shape they are in. That
 * one leaves both members starting happily and shows up here as two.
 */
class TwoMembersMigrateOnceIT {

    /** Its own database, so the members race over a store no other specification has touched. */
    private static final String DATABASE = "e2e_two_members";

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void twoMembersOnOneUnmigratedStoreRunEachChangesetOnceBetweenThem() {
        String storeUri = SharedMongo.replicaSetUrl(DATABASE);

        // Started together rather than one after the other: a sequential start would make the second
        // member's "found it already done" the only history this can have, and the point is to let the
        // two arrive as they will.
        CompletableFuture<RealProcessServer> firstStarting =
                CompletableFuture.supplyAsync(() -> RealProcessServer.start(storeUri));
        CompletableFuture<RealProcessServer> secondStarting =
                CompletableFuture.supplyAsync(() -> RealProcessServer.start(storeUri));

        RealProcessServer first = await(firstStarting);
        RealProcessServer second = await(secondStarting);
        try {
            // Both answered their health probe, which is what start() returning means. Stated as an
            // assertion anyway: "both members came up" is half the claim, and a reader should not have
            // to know that the launcher throws to see that it was checked.
            assertThat(new ControlPlane(first.baseUrl()).healthy())
                    .as("the member that reached the store first is serving")
                    .isTrue();
            assertThat(new ControlPlane(second.baseUrl()).healthy())
                    .as("the member that did not do the migrating is serving too")
                    .isTrue();

            try (MongoClient client = MongoClients.create(storeUri)) {
                assertThat(new SystemMetaStore(client.getDatabase(DATABASE)).installedVersion())
                        .as("the store both members are now serving from")
                        .contains(MigrationRunner.SUPPORTED_VERSION);
            }

            // Derived from the runner's own list rather than named here, so that adding a changeset
            // needs no edit in this file - and so that a changeset dropped from the product cannot
            // leave a test still asserting about it.
            for (ChangeSet changeSet : MigrationRunner.changeSets()) {
                String name = changeSet.changeSetName();
                long ran = startsOf(name, first.output()) + startsOf(name, second.output());
                assertThat(ran)
                        .as("how many of the two members ran %s", name)
                        .isEqualTo(1);
            }
        } finally {
            first.close();
            second.close();
        }
    }

    /**
     * How many times this member's own output says it began that changeset. The server's log is the
     * only place the answer exists: the store records how far it got, not who did it or how often.
     */
    private static long startsOf(String changeSetName, Path output) {
        String began = "changeset " + changeSetName + " (version";
        return linesOf(output).stream()
                .filter(line -> line.contains(began) && line.endsWith("starting"))
                .count();
    }

    private static List<String> linesOf(Path output) {
        try {
            return Files.readAllLines(output);
        } catch (IOException e) {
            throw new UncheckedIOException("could not read the server output at " + output, e);
        }
    }

    private static RealProcessServer await(CompletableFuture<RealProcessServer> starting) {
        try {
            return starting.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while a member was starting", e);
        } catch (ExecutionException e) {
            throw new AssertionError("a member did not start", e.getCause());
        }
    }
}
