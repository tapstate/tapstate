package io.tapstate.core.model.canonical;

import io.tapstate.core.model.SourceResource;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The content hash of a resource. It underwrites the apply idempotency key: an unchanged resource
 * hashes equal, so re-applying it is a provable no-op. These tests pin the algorithm (SHA-256 over
 * UTF-8, lower-hex, full digest) so a drift reddens instead of silently changing every stored hash,
 * and pin what the hash is taken over -- the canonical structure, not the canonical text.
 */
class CanonicalHashTest {

    @Test
    void hashesUtf8BytesWithSha256AsLowerHex() {
        // The published SHA-256 vector for "abc": pins algorithm + UTF-8 encoding + lower-hex.
        assertThat(CanonicalHash.ofText("abc"))
                .as("SHA-256 of the UTF-8 bytes, lower-hex")
                .isEqualTo("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    }

    @Test
    void producesA64CharLowerHexDigest() {
        assertThat(CanonicalHash.ofText("version: tapstate/v1\nkind: source\nid: orders\n"))
                .hasSize(64)
                .matches("[0-9a-f]{64}");
    }

    @Test
    void hashesMultibyteUtf8CharactersByTheirUtf8Bytes() {
        // A canonical form can carry non-ASCII runtime data (descriptions, config values, table
        // names). The input is "caf" + U+00E9, which hashes over its UTF-8 bytes (63 61 66 c3 a9);
        // the pinned digest is the published SHA-256 of exactly those bytes, so an encoding drift to
        // a single-byte charset (accented char one byte, or unmappable) reddens here.
        assertThat(CanonicalHash.ofText("caf\u00e9"))
                .isEqualTo("850f7dc43910ff890f8879c0ed26fe697c93a067ad93a7d50f466a7028a9bf4e");
    }

    @Test
    void isStableForEqualInputAndDiffersForDifferentInput() {
        assertThat(CanonicalHash.ofText("one"))
                .as("stable for equal canonical text")
                .isEqualTo(CanonicalHash.ofText("one"));
        assertThat(CanonicalHash.ofText("one"))
                .as("differs when the canonical text differs")
                .isNotEqualTo(CanonicalHash.ofText("two"));
    }

    @Test
    void hashesTheStructureRatherThanTheCanonicalText() {
        // The decision this file exists to lock. Hashing the rendered text would tie a resource's
        // identity to layout and quoting, so every reformatting of the canonical form would become a
        // version change for every stored resource. Nothing else in the suite can tell the two apart:
        // both are 64 lower-hex characters and both are stable.
        SourceResource source = source("orders", Map.of());

        assertThat(CanonicalHash.of(source))
                .isNotEqualTo(CanonicalHash.ofText(new CanonicalWriter().write(source)));
    }

    @Test
    void isStableForAnEqualResourceAndDiffersWhenAFieldDiffers() {
        assertThat(CanonicalHash.of(source("orders", Map.of())))
                .isEqualTo(CanonicalHash.of(source("orders", Map.of())));
        assertThat(CanonicalHash.of(source("orders", Map.of())))
                .isNotEqualTo(CanonicalHash.of(source("customers", Map.of())));
    }

    @Test
    void doesNotRunAKeyAndAValueTogetherWhenTheBoundaryBetweenThemMoves() {
        // Two different resources that would encode identically if the pieces were simply concatenated:
        // written out without a length in front of each, {"aSb": "c"} and {"a": "bSc"} both flatten to
        // aSbSc, because the S that marks the start of a string also occurs inside the data. Two
        // resources sharing one version identity is a conditional write accepting the wrong version.
        Map<String, Object> left = new LinkedHashMap<>();
        left.put("aSb", "c");
        Map<String, Object> right = new LinkedHashMap<>();
        right.put("a", "bSc");

        assertThat(CanonicalHash.of(source("orders", left)))
                .isNotEqualTo(CanonicalHash.of(source("orders", right)));
    }

    @Test
    void ignoresTheOrderKeysWereWrittenInWithinAFreeMap() {
        // Canonical means one form per resource. A free map is sorted on the way into the tree, so two
        // authors who wrote the same keys in different orders get one identity rather than two.
        Map<String, Object> written = new LinkedHashMap<>();
        written.put("b", 2);
        written.put("a", 1);
        Map<String, Object> reversed = new LinkedHashMap<>();
        reversed.put("a", 1);
        reversed.put("b", 2);

        assertThat(CanonicalHash.of(source("orders", written)))
                .isEqualTo(CanonicalHash.of(source("orders", reversed)));
    }

    private static SourceResource source(String id, Map<String, Object> experimental) {
        return new SourceResource(id, null, "postgres", Map.of(), null, null, null, experimental);
    }
}
