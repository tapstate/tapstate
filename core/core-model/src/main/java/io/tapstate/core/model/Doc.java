package io.tapstate.core.model;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Human-facing documentation for a grammar element — the single source the {@code tapstate/v1}
 * JSON Schema is generated from. Every type, record component and enum constant that the schema
 * emits as a documented object/property/value must carry one with a non-blank {@link #value()};
 * the schema generator fails the build otherwise. (Scalar {@link YamlForm} wrappers and flattened
 * {@link YamlFlatten} components are not emitted as documented fields and are exempt.)
 *
 * <p>This is metadata only: it never affects parsing or canonical serialization.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.RECORD_COMPONENT, ElementType.FIELD, ElementType.TYPE})
public @interface Doc {

    /** One-line description of the element. Required, non-blank. */
    String value();

    /** Whether this field must be present in a valid document. */
    boolean required() default false;

    /** YAML key override, when it is not the camelCase-to-snake_case of the component name. */
    String key() default "";

    /** Documented default value (the YAML form), rendered as the schema {@code default}; empty = none. */
    String def() default "";

    /** The least value a number may take, rendered as the schema {@code minimum}; {@code Long.MIN_VALUE} = none. */
    long min() default Long.MIN_VALUE;

    /** The greatest value a number may take, rendered as the schema {@code maximum}; {@code Long.MAX_VALUE} = none. */
    long max() default Long.MAX_VALUE;

    /**
     * A regular expression a text value must contain a match of, rendered as the schema {@code pattern}; empty =
     * none. It constrains the whole value only where it anchors itself at both ends.
     */
    String pattern() default "";
}
