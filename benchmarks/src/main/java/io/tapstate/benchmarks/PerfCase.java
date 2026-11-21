package io.tapstate.benchmarks;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares one versioned performance case for discovery from source.
 * Use a direct import or the fully qualified name, and supply literal attribute values.
 * Discovery does not resolve constants or compile the declaring source.
 */
@Retention(RetentionPolicy.SOURCE)
@Target({ElementType.TYPE, ElementType.METHOD})
@Repeatable(PerfCase.Cases.class)
public @interface PerfCase {
    String id();
    int version();
    String description();
    String group();
    String[] operators();
    String phase();
    String mode();
    String profile();
    int profileVersion();
    String source();
    String target();
    String[] environments();
    String[] purposes() default {};
    String workload();
    String[] dataFeatures() default {};
    String[] tags() default {};
    String execution() default "single-run";

    /** Optional specification path relative to the declaring Java file. */
    String spec() default "";

    @Retention(RetentionPolicy.SOURCE)
    @Target({ElementType.TYPE, ElementType.METHOD})
    @interface Cases {
        PerfCase[] value();
    }
}
