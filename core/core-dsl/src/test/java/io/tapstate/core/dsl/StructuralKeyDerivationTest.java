package io.tapstate.core.dsl;

import io.tapstate.core.model.Doc;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.Metadata;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.PushElement;
import io.tapstate.core.model.QueryElement;
import io.tapstate.core.model.RenameSpec;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.ServeResource;
import io.tapstate.core.model.Settings;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Srs;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.Storage;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TableRef;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.TransformResource;
import io.tapstate.core.model.ViewBlock;
import io.tapstate.core.model.ViewResource;
import io.tapstate.core.model.ViewSchema;
import io.tapstate.core.model.YamlFlatten;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.RecordComponent;
import java.util.LinkedHashSet;
import java.util.Map;
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

    // ---- what a document must carry, against what the model declares required ------------

    /** The YAML keys of a record's {@code @Doc(required = true)} components. */
    private static Set<String> deriveRequired(Class<?> record) {
        Set<String> keys = new LinkedHashSet<>();
        for (RecordComponent component : record.getRecordComponents()) {
            Doc doc = component.getAnnotation(Doc.class);
            if (doc != null && doc.required() && !component.isAnnotationPresent(YamlFlatten.class)) {
                keys.add(yamlKey(component));
            }
        }
        return keys;
    }

    /** What the document must carry: everything the model requires, less what the parser supplies. */
    private static Set<String> requiredOfTheDocument(Class<?> record) {
        Set<String> keys = new TreeSet<>(deriveRequired(record));
        keys.removeAll(ANSWERED_BY_THE_PARSER.getOrDefault(record, Set.of()));
        return keys;
    }

    private static void assertRequired(Set<String> demanded, Class<?> record) {
        assertThat(new TreeSet<>(demanded))
                .as("%s: what the parser demands of a document must be what the model declares "
                        + "required — a component that becomes required without reaching this list is "
                        + "a NullPointerException at the boundary rather than a coded refusal",
                        record.getSimpleName())
                .isEqualTo(requiredOfTheDocument(record));
    }

    @Test
    @DisplayName("the required derivation reads the annotation at all (positive control)")
    void requiredDerivationFindsRequiredComponents() {
        // Symmetrical to the control above and needed for the same reason: a deriveRequired() that
        // always answered nothing would agree with any list that happened to be empty, and most of
        // the records below have exactly one entry.
        assertThat(deriveRequired(SourceResource.class)).containsExactlyInAnyOrder("id", "connector");
        assertThat(deriveRequired(SourceResource.class)).doesNotContain("mode", "config");
    }

    @Test
    void whatTheParserDemandsMatchesWhatTheModelRequires() {
        assertRequired(DslParser.REQUIRED_SOURCE_KEYS, SourceResource.class);
        assertRequired(DslParser.REQUIRED_PIPELINE_KEYS, PipelineResource.class);
        assertRequired(DslParser.REQUIRED_TABLE_SPEC_KEYS, TableRef.Spec.class);
        assertRequired(DslParser.REQUIRED_SYNC_KEYS, SyncElement.class);
        assertRequired(DslParser.REQUIRED_PUSH_KEYS, PushElement.class);
        assertRequired(DslParser.REQUIRED_QUERY_KEYS, QueryElement.class);
        assertRequired(DslParser.REQUIRED_HOT_KEYS, Storage.Hot.class);
        assertRequired(DslParser.REQUIRED_WARM_KEYS, Storage.Warm.class);
        assertRequired(DslParser.REQUIRED_NEST_ROOT_KEYS, NestRoot.class);
        assertRequired(DslParser.REQUIRED_EMBED_KEYS, Embed.class);
        // The three definition bodies each require an id and nothing else of their own, so one set
        // serves all three; TransformResource's other required component is its flattened body.
        assertRequired(DslParser.REQUIRED_VIEW_DEF_KEYS, ViewResource.class);
        assertRequired(DslParser.REQUIRED_VIEW_INLINE_KEYS, ViewBlock.Inline.class);
        assertRequired(DslParser.REQUIRED_DEFINITION_KEYS, ServeResource.class);
        assertRequired(DslParser.REQUIRED_DEFINITION_KEYS, TransformResource.class);
    }

    @Test
    void whatTheParserDemandsOfABodyMatchesItsVariant() {
        assertRequired(DslParser.requiredPayloadKeys("js"), TransformBody.Js.class);
        assertRequired(DslParser.requiredPayloadKeys("map"), TransformBody.MapProjection.class);
        assertRequired(DslParser.requiredPayloadKeys("filter"), TransformBody.Filter.class);
        assertRequired(DslParser.requiredPayloadKeys("nest"), TransformBody.Nest.class);
        assertRequired(DslParser.requiredPayloadKeys("join"), TransformBody.Join.class);
        assertRequired(DslParser.requiredPayloadKeys("union"), TransformBody.Union.class);
    }

    /** The records checked one by one above, named by the class whose requirement each covers. */
    private static final Set<Class<?>> DEMANDED_OF_THE_DOCUMENT = Set.of(
            SourceResource.class, PipelineResource.class, TableRef.Spec.class, SyncElement.class,
            PushElement.class, QueryElement.class, Storage.Hot.class, Storage.Warm.class,
            NestRoot.class, Embed.class, ViewResource.class, ServeResource.class,
            ViewBlock.Inline.class,
            TransformResource.class, TransformBody.Js.class, TransformBody.MapProjection.class,
            TransformBody.Filter.class, TransformBody.Nest.class, TransformBody.Join.class);

    /**
     * Records with a required component that the parser answers for rather than demanding of the
     * document. Named so that arriving here is a decision — the alternative is a required field
     * nothing checks, which is the state this whole test exists to end.
     *
     * <ul>
     *   <li>{@link Step.Inline} / {@link ViewBlock.Inline} / {@link ServeBlock.Inline} — the id is
     *       generated when omitted, and {@code from} comes from natural order, whose own absence the
     *       parser refuses separately when there is no predecessor to take it from;</li>
     *   <li>{@link Step.Use} / {@link ViewBlock.Use} / {@link ServeBlock.Use} — reached only through
     *       a branch guarded on {@code use:} being present, so the key is there by construction.</li>
     * </ul>
     */
    private static final Map<Class<?>, Set<String>> ANSWERED_BY_THE_PARSER = Map.of(
            Step.Inline.class, Set.of("id", "from"),
            Step.Use.class, Set.of("use", "from"),
            // Not primary_key: nothing supplies a view's key, so it is demanded of the document.
            // Naming the record alone used to exempt every component it would ever require, which
            // is how a key the model calls required reached the sink as a null.
            ViewBlock.Inline.class, Set.of("id", "from"),
            ViewBlock.Use.class, Set.of("use", "from"),
            ServeBlock.Inline.class, Set.of("from"),
            ServeBlock.Use.class, Set.of("use", "from"));

    /** Every record the grammar is made of — the population the account below is taken over. */
    private static final Set<Class<?>> MODEL_RECORDS = Set.of(
            SourceResource.class, PipelineResource.class, TransformResource.class, ViewResource.class,
            ServeResource.class, Metadata.class, Srs.class, Settings.class, Storage.Hot.class,
            Storage.Warm.class, Storage.Cold.class, ViewSchema.class, RenameSpec.class,
            TableRef.Literal.class, TableRef.Regex.class, TableRef.Spec.class, SyncElement.class,
            PushElement.class, QueryElement.class, NestRoot.class, Embed.class, Step.Inline.class,
            Step.Use.class, ViewBlock.Inline.class, ViewBlock.Use.class, ServeBlock.Inline.class,
            ServeBlock.Use.class, TransformBody.Js.class, TransformBody.MapProjection.class,
            TransformBody.Filter.class, TransformBody.Union.class, TransformBody.Nest.class,
            TransformBody.Join.class);

    @Test
    @DisplayName("every record with a required component is demanded of the document or named as answered")
    void everyRequiredComponentIsAccountedFor() {
        Set<String> withARequirement = new TreeSet<>();
        for (Class<?> record : MODEL_RECORDS) {
            if (!deriveRequired(record).isEmpty()) {
                withARequirement.add(record.getSimpleName());
            }
        }
        Set<String> accountedFor = new TreeSet<>();
        DEMANDED_OF_THE_DOCUMENT.forEach(c -> accountedFor.add(c.getSimpleName()));
        ANSWERED_BY_THE_PARSER.forEach((c, answered) -> {
            if (requiredOfTheDocument(c).isEmpty()) {
                accountedFor.add(c.getSimpleName());
            }
        });

        assertThat(withARequirement)
                .as("a model record declaring a required component must either be demanded of the "
                        + "document or be named as one the parser answers for")
                .isEqualTo(accountedFor);
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

    /** The required-key sets, re-derived above from their record's {@code @Doc(required = true)}. */
    private static final Set<String> REQUIRED_DERIVED = Set.of(
            "REQUIRED_SOURCE_KEYS", "REQUIRED_PIPELINE_KEYS", "REQUIRED_DEFINITION_KEYS",
            "REQUIRED_TABLE_SPEC_KEYS", "REQUIRED_SYNC_KEYS", "REQUIRED_PUSH_KEYS",
            "REQUIRED_QUERY_KEYS", "REQUIRED_HOT_KEYS", "REQUIRED_WARM_KEYS",
            "REQUIRED_VIEW_DEF_KEYS", "REQUIRED_VIEW_INLINE_KEYS",
            "REQUIRED_NEST_ROOT_KEYS", "REQUIRED_EMBED_KEYS");

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
                .isEqualTo(new TreeSet<>(union(union(DERIVED, REQUIRED_DERIVED), NOT_RECORD_BACKED)));
    }
}
