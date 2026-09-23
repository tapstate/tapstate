package io.tapstate.core.schema;

import io.tapstate.core.model.Doc;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.YamlFlatten;
import io.tapstate.core.model.YamlForm;
import io.tapstate.core.model.YamlScalarOrList;
import io.tapstate.core.model.YamlType;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Generates the {@code tapstate/v1} JSON Schema from the resource model and its {@code @Doc}
 * metadata. Build-time only (reflection runs on the JVM during the build); the runtime loads
 * the generated artifact instead of reflecting.
 *
 * <p>The whole grammar is reachable from {@link Resource}: each permitted subtype, every record
 * component type, every enum, transitively. Walking that graph is what makes the schema
 * same-source with the model — a new field appears in the schema with no hand edit here.
 */
final class SchemaGenerator {

    private static final String DIALECT = "https://json-schema.org/draft/2020-12/schema";
    private static final String ID = "https://tapstate.io/schema/tapstate/v1.json";

    private final Map<String, Json> defs = new TreeMap<>();
    private final Set<String> seen = new HashSet<>();
    private final List<Class<?>> registeredTypes = new ArrayList<>();

    /** The schema as a structured tree (asserted in tests); {@link #generate()} serializes it. */
    Json.Obj generateTree() {
        defs.clear();
        seen.clear();
        registeredTypes.clear();
        register(Resource.class);

        List<Json.Entry> defEntries = new ArrayList<>();
        defs.forEach((name, schema) -> defEntries.add(new Json.Entry(name, schema)));
        return new Json.Obj(List.of(
                new Json.Entry("$schema", new Json.Str(DIALECT)),
                new Json.Entry("$id", new Json.Str(ID)),
                new Json.Entry("$ref", new Json.Str("#/$defs/Resource")),
                new Json.Entry("$defs", new Json.Obj(defEntries))
        ));
    }

    String generate() {
        return new JsonWriter().write(generateTree());
    }

    /**
     * Grammar elements reachable from {@link Resource} that lack an {@code @Doc} — every record
     * component emitted as a property, every enum constant, and every emitted type. Empty means
     * the schema is fully self-describing; a non-empty result fails the build. Scalar
     * ({@code @YamlForm}) types and their components are exempt — they are inlined, not emitted
     * as objects, so they expose no field to document.
     */
    List<String> undocumented() {
        generateTree();
        List<String> missing = new ArrayList<>();
        for (Class<?> type : registeredTypes) {
            String name = defName(type);
            if (docValue(type.getAnnotation(Doc.class)) == null) {
                missing.add(name);
            }
            if (type.isRecord()) {
                for (RecordComponent component : type.getRecordComponents()) {
                    if (component.isAnnotationPresent(YamlFlatten.class)) {
                        continue; // flattened away, never emitted as a property
                    }
                    if (docValue(component.getAnnotation(Doc.class)) == null) {
                        missing.add(name + "." + component.getName());
                    }
                }
            } else if (type.isEnum()) {
                for (Object constant : type.getEnumConstants()) {
                    if (constantDoc(type, (Enum<?>) constant) == null) {
                        missing.add(name + "." + ((Enum<?>) constant).name());
                    }
                }
            }
        }
        Collections.sort(missing);
        return missing;
    }

    /** Adds {@code type} to {@code $defs} (records, enums, sealed unions) once. */
    private void register(Class<?> type) {
        if (!seen.add(defName(type))) {
            return;
        }
        registeredTypes.add(type);
        Json schema;
        if (type.isEnum()) {
            schema = enumSchema(type);
        } else if (type.isRecord()) {
            schema = recordSchema(type);
        } else if (type.isSealed()) {
            schema = unionSchema(type);
        } else {
            throw new IllegalStateException("not a schema definition type: " + type);
        }
        defs.put(defName(type), schema);
    }

    private Json unionSchema(Class<?> sealed) {
        // Each subtype contributes its reference-site schema: a scalar form for @YamlForm
        // subtypes (inlined, no $def of their own), a $ref for object subtypes. Structurally
        // identical branches collapse — two string forms (literal + regex) become one — so the
        // oneOf stays valid (a plain string can't match two branches at once).
        Json anyValue = new Json.Obj(List.of());
        List<Json> branches = new ArrayList<>();
        boolean anyForm = false;
        for (Class<?> subtype : sealed.getPermittedSubclasses()) {
            Json branch = valueSchemaFor(subtype);
            if (branch.equals(anyValue)) {
                anyForm = true;
            } else if (!branches.contains(branch)) {
                branches.add(branch);
            }
        }
        List<Json.Entry> entries = new ArrayList<>();
        String desc = docValue(sealed.getAnnotation(Doc.class));
        if (desc != null) {
            entries.add(new Json.Entry("description", new Json.Str(desc)));
        }
        if (anyForm) {
            // An "any literal" branch subsumes every other: the value is unconstrained and the
            // forms are spelled out in the description. A oneOf would be invalid here — a string
            // would match both the string branch and the any branch.
            return new Json.Obj(entries);
        }
        if (branches.size() == 1) {
            entries.addAll(((Json.Obj) branches.get(0)).entries());
        } else {
            entries.add(new Json.Entry("oneOf", new Json.Arr(branches)));
        }
        return new Json.Obj(entries);
    }

    private Json recordSchema(Class<?> record) {
        YamlType variant = record.getAnnotation(YamlType.class);
        RecordComponent flattened = flattenedComponent(record);
        boolean resource = Resource.class.isAssignableFrom(record);

        List<Json.Entry> props = new ArrayList<>();
        List<Json> required = new ArrayList<>();
        // A flattened-union variant leads with its `type:` discriminator constant.
        if (variant != null) {
            props.add(new Json.Entry("type",
                    constProperty("Transform type discriminator.", variant.value())));
            required.add(new Json.Str("type"));
        }
        // version/kind are contract constants, not record components — synthesize them so the
        // schema describes the full document, in canonical key order (version, kind, ...).
        if (resource) {
            props.add(new Json.Entry("version",
                    constProperty("The grammar version; always \"tapstate/v1\".", Resource.VERSION)));
            props.add(new Json.Entry("kind",
                    constProperty("Resource kind discriminator.", kindOf(record))));
            required.add(new Json.Str("version"));
            required.add(new Json.Str("kind"));
        }
        for (RecordComponent component : record.getRecordComponents()) {
            if (component.isAnnotationPresent(YamlFlatten.class)) {
                continue; // merged in via allOf below, not a nested property
            }
            props.add(new Json.Entry(yamlKey(component), propertySchema(component)));
            Doc doc = component.getAnnotation(Doc.class);
            if (doc != null && doc.required()) {
                required.add(new Json.Str(yamlKey(component)));
            }
        }
        List<Json.Entry> entries = new ArrayList<>();
        entries.add(new Json.Entry("type", new Json.Str("object")));
        String desc = docValue(record.getAnnotation(Doc.class));
        if (desc != null) {
            entries.add(new Json.Entry("description", new Json.Str(desc)));
        }
        entries.add(new Json.Entry("properties", new Json.Obj(props)));
        if (!required.isEmpty()) {
            entries.add(new Json.Entry("required", new Json.Arr(required)));
        }
        if (record == Embed.class) {
            entries.add(new Json.Entry("allOf", new Json.Arr(List.of(flatEmbedShape()))));
        }
        if (flattened != null) {
            // The discriminated union's variant fields merge into this object: allOf brings them
            // in, and unevaluatedProperties (not additionalProperties) closes the merged whole —
            // additionalProperties:false would reject the variant's fields it can't see.
            entries.add(new Json.Entry("allOf",
                    new Json.Arr(List.of(valueSchemaFor(flattened.getType())))));
            entries.add(new Json.Entry("unevaluatedProperties", new Json.Bool(false)));
        } else if (variant == null) {
            // §11 strict mode: a document may not carry fields outside the schema. The escape
            // hatch is the open `experimental` property, not arbitrary keys here. Variants stay
            // open (their fields close at the enclosing object's unevaluatedProperties).
            entries.add(new Json.Entry("additionalProperties", new Json.Bool(false)));
        }
        return new Json.Obj(entries);
    }

    /**
     * A flat embed merges into its parent and therefore has no target path or array identity. The other
     * two shapes still require a path. This conditional belongs beside the generated record rather than
     * in the checked-in artifact, so the schema remains reproducible from the model contract.
     */
    private static Json flatEmbedShape() {
        Json flat = new Json.Obj(List.of(
                new Json.Entry("properties", new Json.Obj(List.of(
                        new Json.Entry("as", new Json.Obj(List.of(
                                new Json.Entry("const", new Json.Str("flat")))))))),
                new Json.Entry("required", new Json.Arr(List.of(new Json.Str("as"))))));
        Json noPath = new Json.Obj(List.of(new Json.Entry("not", new Json.Obj(List.of(
                new Json.Entry("required", new Json.Arr(List.of(new Json.Str("path")))))))));
        Json noArrayKey = new Json.Obj(List.of(new Json.Entry("not", new Json.Obj(List.of(
                new Json.Entry("required", new Json.Arr(List.of(new Json.Str("arrayKey")))))))));
        Json thenShape = new Json.Obj(List.of(new Json.Entry("allOf", new Json.Arr(List.of(noPath, noArrayKey)))));
        Json elseShape = new Json.Obj(List.of(new Json.Entry("required",
                new Json.Arr(List.of(new Json.Str("path"))))));
        return new Json.Obj(List.of(
                new Json.Entry("if", flat),
                new Json.Entry("then", thenShape),
                new Json.Entry("else", elseShape)));
    }

    private static RecordComponent flattenedComponent(Class<?> record) {
        for (RecordComponent component : record.getRecordComponents()) {
            if (component.isAnnotationPresent(YamlFlatten.class)) {
                return component;
            }
        }
        return null;
    }

    private static Json constProperty(String description, String value) {
        return new Json.Obj(List.of(
                new Json.Entry("description", new Json.Str(description)),
                new Json.Entry("const", new Json.Str(value))));
    }

    private static String kindOf(Class<?> resource) {
        String name = resource.getSimpleName();
        String suffix = "Resource";
        String stem = name.endsWith(suffix) ? name.substring(0, name.length() - suffix.length()) : name;
        return stem.toLowerCase();
    }

    private Json enumSchema(Class<?> enumType) {
        List<Json.Entry> entries = new ArrayList<>();
        entries.add(new Json.Entry("type", new Json.Str("string")));
        String desc = docValue(enumType.getAnnotation(Doc.class));
        if (desc != null) {
            entries.add(new Json.Entry("description", new Json.Str(desc)));
        }
        // When every constant is documented, a oneOf of const+description carries per-value docs
        // (better for explain). Otherwise fall back to a plain enum list.
        List<Json> values = new ArrayList<>();
        List<Json> documented = new ArrayList<>();
        boolean allDocumented = true;
        for (Object constant : enumType.getEnumConstants()) {
            String yaml = yamlValue(constant);
            values.add(new Json.Str(yaml));
            String constantDesc = constantDoc(enumType, (Enum<?>) constant);
            if (constantDesc == null) {
                allDocumented = false;
            } else {
                documented.add(new Json.Obj(List.of(
                        new Json.Entry("const", new Json.Str(yaml)),
                        new Json.Entry("description", new Json.Str(constantDesc)))));
            }
        }
        if (allDocumented && !values.isEmpty()) {
            entries.add(new Json.Entry("oneOf", new Json.Arr(documented)));
        } else {
            entries.add(new Json.Entry("enum", new Json.Arr(values)));
        }
        return new Json.Obj(entries);
    }

    private static String constantDoc(Class<?> enumType, Enum<?> constant) {
        try {
            return docValue(enumType.getField(constant.name()).getAnnotation(Doc.class));
        } catch (NoSuchFieldException e) {
            return null;
        }
    }

    /** A property's schema: its type constraints, with the component's {@code @Doc} woven in. */
    private Json propertySchema(RecordComponent component) {
        Json.Obj type = componentTypeSchema(component);
        Doc doc = component.getAnnotation(Doc.class);
        String desc = docValue(doc);
        if (desc == null && (doc == null || doc.def().isEmpty())) {
            return type;
        }
        List<Json.Entry> entries = new ArrayList<>();
        if (desc != null) {
            entries.add(new Json.Entry("description", new Json.Str(desc)));
        }
        entries.addAll(type.entries());
        if (doc != null && !doc.def().isEmpty()) {
            entries.add(new Json.Entry("default", defaultValue(component.getType(), doc.def())));
        }
        return new Json.Obj(entries);
    }

    /** A list component whose single-element YAML form is a bare scalar renders as a oneOf. */
    private Json.Obj componentTypeSchema(RecordComponent component) {
        if (component.isAnnotationPresent(YamlScalarOrList.class)) {
            Type itemType = ((ParameterizedType) component.getGenericType()).getActualTypeArguments()[0];
            Json.Obj item = typeSchema(itemType);
            Json.Obj array = new Json.Obj(List.of(
                    new Json.Entry("type", new Json.Str("array")),
                    new Json.Entry("items", item)));
            return new Json.Obj(List.of(new Json.Entry("oneOf", new Json.Arr(List.of(item, array)))));
        }
        return typeSchema(component.getGenericType());
    }

    /** Coerces a documented default to the JSON type implied by the component's Java type. */
    private static Json defaultValue(Class<?> type, String raw) {
        if (type == Integer.class || type == int.class) {
            // A non-numeric default would emit invalid JSON (an unquoted token) — fail the build.
            Long.parseLong(raw);
            return new Json.Num(raw);
        }
        if (type == Boolean.class || type == boolean.class) {
            return new Json.Bool(Boolean.parseBoolean(raw));
        }
        return new Json.Str(raw);
    }

    private Json.Obj typeSchema(Type type) {
        if (type instanceof Class<?> raw) {
            if (raw == String.class) {
                return scalar("string");
            }
            if (raw == Boolean.class || raw == boolean.class) {
                return scalar("boolean");
            }
            if (raw == Integer.class || raw == int.class) {
                return scalar("integer");
            }
            if (raw == Object.class) {
                return new Json.Obj(List.of());
            }
            if (raw.isEnum() || raw.isRecord() || raw.isSealed()) {
                return valueSchemaFor(raw);
            }
            return new Json.Obj(List.of());
        }
        if (type instanceof ParameterizedType parameterized) {
            Class<?> raw = (Class<?>) parameterized.getRawType();
            Type[] args = parameterized.getActualTypeArguments();
            if (List.class.isAssignableFrom(raw)) {
                return new Json.Obj(List.of(
                        new Json.Entry("type", new Json.Str("array")),
                        new Json.Entry("items", typeSchema(args[0]))));
            }
            if (Map.class.isAssignableFrom(raw)) {
                Json additional = args[1] == Object.class
                        ? new Json.Bool(true) : typeSchema(args[1]);
                return new Json.Obj(List.of(
                        new Json.Entry("type", new Json.Str("object")),
                        new Json.Entry("additionalProperties", additional)));
            }
        }
        return new Json.Obj(List.of());
    }

    /** Schema to use at a reference site: an inlined surface form, or a {@code $ref} to a $def. */
    private Json.Obj valueSchemaFor(Class<?> type) {
        YamlForm form = type.getAnnotation(YamlForm.class);
        if (form != null) {
            return formSchema(type, form.value());
        }
        register(type);
        return new Json.Obj(List.of(
                new Json.Entry("$ref", new Json.Str("#/$defs/" + defName(type)))));
    }

    private Json.Obj formSchema(Class<?> type, YamlForm.Form form) {
        return switch (form) {
            // The record wraps a single value; the YAML form is that value's own schema.
            case UNWRAP -> typeSchema(type.getRecordComponents()[0].getGenericType());
            case FALSE -> new Json.Obj(List.of(new Json.Entry("const", new Json.Bool(false))));
        };
    }

    private static Json.Obj scalar(String jsonType) {
        return new Json.Obj(List.of(new Json.Entry("type", new Json.Str(jsonType))));
    }

    private static String docValue(Doc doc) {
        return doc == null || doc.value().isBlank() ? null : doc.value();
    }

    /** A nested type is keyed by its enclosing chain so {@code TableRef.Literal} can't collide. */
    private static String defName(Class<?> type) {
        String name = type.getSimpleName();
        for (Class<?> enclosing = type.getEnclosingClass();
             enclosing != null; enclosing = enclosing.getEnclosingClass()) {
            name = enclosing.getSimpleName() + "." + name;
        }
        return name;
    }

    private static String yamlKey(RecordComponent component) {
        Doc doc = component.getAnnotation(Doc.class);
        if (doc != null && !doc.key().isEmpty()) {
            return doc.key();
        }
        return camelToSnake(component.getName());
    }

    private static String camelToSnake(String name) {
        StringBuilder out = new StringBuilder(name.length() + 4);
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (Character.isUpperCase(c)) {
                out.append('_').append(Character.toLowerCase(c));
            } else {
                out.append(c);
            }
        }
        return out.toString();
    }

    private static String yamlValue(Object enumConstant) {
        try {
            return (String) enumConstant.getClass().getMethod("yaml").invoke(enumConstant);
        } catch (ReflectiveOperationException e) {
            return ((Enum<?>) enumConstant).name().toLowerCase();
        }
    }
}
