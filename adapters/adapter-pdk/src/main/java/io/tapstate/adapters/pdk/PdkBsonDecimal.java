package io.tapstate.adapters.pdk;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;

/** Exact BSON decimal values can arrive from a connector's isolated driver class loader. */
final class PdkBsonDecimal {
    private static final String BSON_TYPE = "org.bson.types.Decimal128";
    private static final ClassValue<Access> ACCESS = new ClassValue<>() {
        @Override
        protected Access computeValue(Class<?> type) {
            try {
                return new Access(type.getMethod("isFinite"), type.getMethod("bigDecimalValue"));
            } catch (NoSuchMethodException e) {
                throw new IllegalStateException("mongodb decimal128 has no exact-value accessor", e);
            }
        }
    };

    private record Access(Method isFinite, Method bigDecimalValue) {}

    private PdkBsonDecimal() {}

    static final String MONGODB = "io.tapdata.mongodb.MongodbConnector";
    static final String ATLAS = "io.tapdata.mongodb.MongodbAtlasConnector";

    static boolean usesBsonModel(String connectorClass) {
        return MONGODB.equals(connectorClass) || ATLAS.equals(connectorClass);
    }

    static boolean isNative(Object value) {
        return value != null && BSON_TYPE.equals(value.getClass().getName());
    }

    /** Recognizes the connector's plain narrowing of this value before restoring its exact form. */
    static BigDecimal fromNarrowed(Object source, Object converted) {
        if (!(converted instanceof Double narrowed) || !isNative(source)) return null;
        BigDecimal exact = decimal128Value(source);
        return exact != null && exact.doubleValue() == narrowed.doubleValue() ? exact : null;
    }

    private static BigDecimal decimal128Value(Object source) {
        Access access = ACCESS.get(source.getClass());
        try {
            if (!(Boolean) access.isFinite().invoke(source)) return null;
            try {
                return (BigDecimal) access.bigDecimalValue().invoke(source);
            } catch (InvocationTargetException e) {
                // Negative zero is finite but BigDecimal cannot preserve its sign.
                if (e.getCause() instanceof ArithmeticException) return null;
                throw e;
            }
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("cannot read mongodb decimal128 exactly", e);
        }
    }
}
