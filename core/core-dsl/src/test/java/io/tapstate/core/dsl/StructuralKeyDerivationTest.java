package io.tapstate.core.dsl;

import io.tapstate.core.model.Doc;
import io.tapstate.core.model.Metadata;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.PushElement;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Srs;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TableRef;
import io.tapstate.core.model.TransformResource;
import io.tapstate.core.model.YamlFlatten;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.RecordComponent;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The parser's structural key whitelists are checked-in constants, and this re-derives each one from
 * the record it guards. The constants stay: the parser reads them at runtime with no reflection and
 * nothing is generated at build time. What the derivation buys is that a component added to a record
 * without its key reaching the whitelist turns this red — until now the two were kept in step by
 * whoever remembered, and forgetting produced a field the model carries and the parser refuses.
 *
 * <p>Three kinds of key are not record components and are named per record with the reason:
 *
 * <ul>
 *   <li>{@code version} / {@code kind} — document-level, on every top-level resource, held by no
 *       component;</li>
 *   <li>{@code type} — the discriminator of a flattened body, whose own keys are merged in
 *       separately by the parser;</li>
 *   <li>{@code options} — accepted so the engine-option vocabulary has somewhere to be checked. Its
 *       vocabulary is empty today, so no component holds it and every key inside is refused.</li>
 * </ul>
 */
class StructuralKeyDerivationTest {

    private static final Set<String> DOCUMENT = Set.of("version", "kind");

    /** YAML key for a component: an explicit {@code @Doc(key=…)} wins, else camelCase to snake_case. */
    private static String yamlKey(RecordComponent component) {
        Doc doc = component.getAnnotation(Doc.class);
        if (doc != null && !doc.key().isEmpty()) {
            return doc.key();
        }
        StringBuilder out = new StringBuilder();
        for (char c : component.getName().toCharArray()) {
            if (Character.isUpperCase(c)) {
                out.append('_').append(Character.toLowerCase(c));
            } else {
                out.append(c);
            }
        }
        return out.toString();
    }

    /** Every component's YAML key, minus any flattened body — the parser merges that one's keys itself. */
    private static Set<String> derive(Class<?> record) {
        Set<String> keys = new LinkedHashSet<>();
        for (RecordComponent component : record.getRecordComponents()) {
            if (component.isAnnotationPresent(YamlFlatten.class)) {
                continue;
            }
            keys.add(yamlKey(component));
        }
        return keys;
    }

    private static void assertWhitelist(Set<String> whitelist, Class<?> record, Set<String> extras) {
        Set<String> expected = new TreeSet<>(derive(record));
        expected.addAll(extras);
        assertThat(new TreeSet<>(whitelist))
                .as("%s: whitelist must be its record's components plus the named extras — a component "
                        + "with no key here is a field the model carries and the parser refuses",
                        record.getSimpleName())
                .isEqualTo(expected);
    }

    @Test
    @DisplayName("the derivation reads components at all (positive control)")
    void derivationFindsComponents() {
        // A derive() that silently returned nothing would make every assertion below pass on a
        // whitelist that is pure extras, which reads exactly like agreement.
        assertThat(derive(SourceResource.class)).contains("id", "connector", "config");
        assertThat(derive(SyncElement.class)).contains("write_mode");
    }

    @Test
    void topLevelResourceWhitelistsMatchTheirRecords() {
        assertWhitelist(DslParser.SOURCE_KEYS, SourceResource.class, union(DOCUMENT, Set.of("options")));
        assertWhitelist(DslParser.PIPELINE_KEYS, PipelineResource.class, DOCUMENT);
        assertWhitelist(DslParser.TRANSFORM_DEF_KEYS, TransformResource.class,
                union(DOCUMENT, Set.of("type", "options")));
    }

    @Test
    void nestedWhitelistsMatchTheirRecords() {
        assertWhitelist(DslParser.METADATA_KEYS, Metadata.class, Set.of());
        assertWhitelist(DslParser.SRS_KEYS, Srs.class, Set.of());
        assertWhitelist(DslParser.TABLE_SPEC_KEYS, TableRef.Spec.class, Set.of("options"));
        assertWhitelist(DslParser.SYNC_KEYS, SyncElement.class, Set.of("options"));
        assertWhitelist(DslParser.PUSH_KEYS, PushElement.class, Set.of("options"));
        assertWhitelist(DslParser.STEP_BASE_KEYS, Step.Inline.class, Set.of("type", "options"));
        assertWhitelist(DslParser.STEP_USE_KEYS, Step.Use.class, Set.of("options"));
    }

    private static Set<String> union(Set<String> a, Set<String> b) {
        Set<String> all = new LinkedHashSet<>(a);
        all.addAll(b);
        return all;
    }

    /**
     * Whitelists guarding something that is not a record, so nothing can be derived for them: YAML
     * shapes the model expresses another way (a sealed hierarchy, an enum, a nested block the parser
     * assembles by hand). Listed by name so that adding one is a decision rather than an omission.
     */
    private static final Set<String> NOT_RECORD_BACKED = Set.of(
            "NO_ENGINE_OPTIONS", "NEST_ROOT_KEYS", "EMBED_KEYS", "VIEW_INLINE_KEYS", "VIEW_USE_KEYS",
            "STORAGE_KEYS", "HOT_KEYS", "WARM_KEYS", "COLD_KEYS", "VIEW_SCHEMA_KEYS",
            "SERVE_USE_KEYS", "SERVE_INLINE_KEYS", "RENAME_KEYS", "QUERY_KEYS", "SETTINGS_KEYS",
            "VIEW_DEF_KEYS", "SERVE_DEF_KEYS");

    /** The whitelists the tests above re-derive from a record. */
    private static final Set<String> DERIVED = Set.of(
            "SOURCE_KEYS", "PIPELINE_KEYS", "TRANSFORM_DEF_KEYS", "METADATA_KEYS", "SRS_KEYS",
            "TABLE_SPEC_KEYS", "SYNC_KEYS", "PUSH_KEYS", "STEP_BASE_KEYS", "STEP_USE_KEYS");

    @Test
    @DisplayName("every key set in the parser is either derived here or named as not record-backed")
    void everyKeySetIsAccountedFor() {
        // The other direction, and the one a list of assertions cannot cover by itself: a whitelist
        // added to the parser and mentioned in neither set above would simply go unchecked, and the
        // only signal would be nobody noticing. Reflection is a test-only tool here; the parser reads
        // these as compile-time constants.
        Set<String> declared = new TreeSet<>();
        for (Field field : DslParser.class.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers()) && Set.class.isAssignableFrom(field.getType())) {
                declared.add(field.getName());
            }
        }

        assertThat(declared)
                .as("a key set in the parser that is neither derived from a record nor listed as "
                        + "not record-backed: decide which it is")
                .isEqualTo(new TreeSet<>(union(DERIVED, NOT_RECORD_BACKED)));
    }
}
