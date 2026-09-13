package io.tapstate.core.common;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * The numeric attributes declared by a source, independent of its connector framework.
 * Null means undeclared, not zero or a default. Exact bounds retain their decimal representation;
 * no destination-specific type selection or range widening belongs in this value.
 */
public record NumericType(Integer bit, Boolean fixed, Boolean unsigned, Boolean zerofill,
                          BigDecimal minValue, BigDecimal maxValue, Integer precision, Integer scale)
        implements Serializable {
}
