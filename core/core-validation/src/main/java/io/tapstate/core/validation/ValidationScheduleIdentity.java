package io.tapstate.core.validation;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Canonical FULL occurrence identity, independent of member clocks and launch sequence. */
public final class ValidationScheduleIdentity {
    private ValidationScheduleIdentity() {}

    public static UUID command(String cluster, String pipeline, String task, UUID generation, long revision, Instant occurrence) {
        if (revision < 1) throw new IllegalArgumentException("positive configuration revision required");
        try (var bytes = new ByteArrayOutputStream(); var out = new DataOutputStream(bytes)) {
            for (var value : List.of("validation-full-cycle-v1", cluster, pipeline, task, generation.toString(), ValidationSchedule.RULES_VERSION)) {
                var encoded = value.getBytes(StandardCharsets.UTF_8); out.writeInt(encoded.length); out.write(encoded);
            }
            out.writeLong(revision); out.writeLong(occurrence.getEpochSecond()); out.writeInt(occurrence.getNano()); out.flush();
            return UUID.nameUUIDFromBytes(bytes.toByteArray());
        } catch (IOException impossible) { throw new IllegalStateException("validation cycle identity unavailable", impossible); }
    }
}
