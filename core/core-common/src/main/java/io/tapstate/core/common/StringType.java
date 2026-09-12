package io.tapstate.core.common;

import java.io.Serializable;

/** Declared source string attributes, independent of connector and destination SQL type names. */
public record StringType(Long bytes, Boolean fixed, Boolean doubleBytes, Long defaultValue, Integer byteRatio)
        implements Serializable {
}
