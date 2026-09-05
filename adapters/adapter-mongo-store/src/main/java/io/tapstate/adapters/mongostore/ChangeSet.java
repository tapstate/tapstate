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
     */
    void up(MongoDatabase database);

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
