package io.tapstate.core.lifecycle;

import java.util.Objects;

/** Key-address work, including writes whose entry processors may load missing state. */
public record NestKeyAccess(long reads, long writes, long coldLoads, String generation) {
    public NestKeyAccess { Objects.requireNonNull(generation, "generation"); }

    /** Missing or incomplete samples are unavailable, never a healthy zero. */
    public static NestKeyAccess unavailable() { return new NestKeyAccess(-1, -1, -1, "unavailable"); }
    public boolean available() { return reads >= 0 && writes >= 0 && coldLoads >= 0 && reads <= Long.MAX_VALUE - writes; }
    public long attempts() { return available() ? reads + writes : -1; }
    public boolean follows(NestKeyAccess before) {
        return before != null && available() && before.available() && generation.equals(before.generation)
                && reads >= before.reads && writes >= before.writes && coldLoads >= before.coldLoads;
    }
    public NestKeyAccess since(NestKeyAccess before) {
        return follows(before) ? new NestKeyAccess(reads - before.reads, writes - before.writes,
                coldLoads - before.coldLoads, generation) : this;
    }
}
