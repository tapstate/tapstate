package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.Severity;
import io.tapstate.core.common.TapstateErrorCode;

import java.util.Set;

/**
 * The {@code migration} domain's error codes: the system data the store holds is at a schema version
 * this process must not run against, or could not be brought to one. These are user-facing and
 * operator-diagnosable — an operator started the wrong build against a store, or two members raced to
 * migrate it and neither could finish.
 *
 * <p>Distinct from {@code store.*}, which reports that the store could not be reached at all: by the
 * time any of these is raised the store answered, and what is wrong is what it holds.
 *
 * <p>{@code placeholders()} is the named-argument contract: every throw site supplies a value for each
 * name, and the build-time placeholder gate checks the catalog templates against it.
 */
public enum MigrationError implements TapstateErrorCode {

    /**
     * The store holds system data at a version higher than the highest this build knows how to read.
     * {@code installed} is the version found in the store (rendered as text, so a value that is not a
     * number is reported as it was found rather than silently read as one); {@code supported} is the
     * highest version this build knows.
     *
     * <p>This is the one refusal that also ships to an older release line, so that a store already
     * moved forward is not opened by a binary that predates the move. Downgrading across a system-data
     * version is not supported: the way back is the backup taken before the upgrade.
     */
    DATA_NEWER_THAN_BINARY("migration.data-newer-than-binary", Set.of("installed", "supported"));

    private final String code;
    private final Set<String> placeholders;

    MigrationError(String code, Set<String> placeholders) {
        this.code = code;
        this.placeholders = placeholders;
    }

    @Override
    public String code() {
        return code;
    }

    @Override
    public Severity severity() {
        return Severity.ERROR;
    }

    @Override
    public Set<String> placeholders() {
        return placeholders;
    }
}
