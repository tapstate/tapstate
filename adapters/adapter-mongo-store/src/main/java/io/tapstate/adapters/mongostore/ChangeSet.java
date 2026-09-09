package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoDatabase;

/**
 * One forward step in the shape of the system data. There is no way back: a changeset moves the store
 * on and the way to undo it is the backup taken before the upgrade, not a reverse operation nobody
 * would be able to test against the data it has to run on.
 *
 * <p>Every changeset must be re-runnable. There is one version number for the whole store rather than
 * one per collection, so a changeset that failed part way through is resumed by running it again from
 * the top — which only works if running it twice is the same as running it once. Select the documents
 * to act on by the shape they are in, never by a marker saying whether they have been done.
 *
 * <p>Changesets live in one package of their own and nothing but the runner may call {@link #up}. Both
 * are pinned by architecture rules: the release path reads that package to check a patch release is not
 * carrying a changeset it should not, and a second caller would mean a step running outside the lock.
 */
public interface ChangeSet {

    /**
     * This changeset's number: globally monotonic, one higher than the changeset before it. It is what
     * the store records once this one has run, and what a later build compares against to decide
     * whether it may open the store at all.
     */
    int version();

    /**
     * Moves the store on. Runs under the migration lock, with every other member either waiting or not
     * yet started. Throwing leaves the recorded version at the step before this one, so the next start
     * runs this changeset again — which is what makes re-runnability a requirement rather than a nicety.
     *
     * <p>Consult {@code fence} before each write. Holding the lock is not a fact that stays true for the
     * length of a changeset: a member that stalls past the lock's lifetime is taken over while it is
     * still inside this method, and the writes it makes on waking land behind whatever the member that
     * replaced it has already done. The store then records a version whose work has been partly undone,
     * and nothing revisits it — every changeset selects on the shape the store is now recorded as past.
     */
    void up(MongoDatabase database, Fence fence);

    /**
     * Whether the member running a changeset still holds the migration lock.
     *
     * <p>This exists because the epoch cannot travel with a changeset's own writes. Every write the lock
     * itself makes carries the epoch it acquired and is refused once that epoch is stale; a changeset
     * writes into documents that have no epoch on them, so nothing about the write can refuse it. What
     * is left is for the writer to ask first.
     *
     * <p>Asking is not free of a window — the lock can go stale between the question and the write that
     * follows it — but that window is one write long instead of one changeset long, and it is the one a
     * conditional update could close only if the documents carried the epoch.
     */
    @FunctionalInterface
    interface Fence {

        /**
         * Returns normally while the lock is still held, and throws once it is not.
         *
         * <p>Throwing here is the same outcome as a changeset failing for any other reason: the recorded
         * version stays where it was, and the member refuses to start rather than carrying on into a
         * store somebody else is changing.
         */
        void requireStillHeld();

        /**
         * A fence for a caller that holds nothing to lose — a test driving one changeset directly, or a
         * dry run. Never use it inside the runner: it answers the question the runner exists to ask.
         */
        Fence HELD = () -> {
        };
    }

    /**
     * The name this changeset is reported and compared under. The class's own name, so that renaming the
     * class is a visible change to the release comparison rather than a silent one.
     */
    default String changeSetName() {
        return getClass().getSimpleName();
    }

    /**
     * What this changeset would do to the store as it stands, without doing any of it — the answer the
     * inspection command gives an operator deciding whether to start an upgrade now. Reads only.
     *
     * <p>The default says nothing beyond the fact that it has not run: a changeset that touches
     * documents should count them, because how long an upgrade takes and whether it can be done in the
     * window available is a question about how many rows there are.
     */
    default String dryRunSummary(MongoDatabase database) {
        return "has not run";
    }
}
