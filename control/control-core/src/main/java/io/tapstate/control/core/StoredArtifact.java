package io.tapstate.control.core;

import java.util.Objects;

/**
 * The truth-layer view of one stored artifact returned by a read: its id, kind, public representation,
 * and the content hash of the authoritative resource. A Source presentation omits connector config and
 * can carry redacted URI userinfo in other display fields. Its bytes therefore do not determine the hash.
 * Reapplying a config-omitting Source retains its stored config, while a display marker outside that
 * omitted field is refused. The server, not a local draft, remains the stored-resource source of truth.
 *
 * <p>The hash travels with the read because it is the precondition an edit or a removal must supply, and
 * not every caller can compute it: a remote model driving the tool surface cannot take a SHA-256 of the
 * text it just received. Handing it over here is what makes read-then-remove a closed loop on every face
 * rather than only on the ones that can hash locally. It is the value the write side issued for the
 * authoritative resource; redacting its public representation never weakens that precondition.
 */
public record StoredArtifact(String id, String kind, String canonicalForm, String contentHash) {

    public StoredArtifact {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(canonicalForm, "canonicalForm");
        Objects.requireNonNull(contentHash, "contentHash");
    }
}
