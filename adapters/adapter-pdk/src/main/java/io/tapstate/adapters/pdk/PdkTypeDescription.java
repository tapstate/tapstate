package io.tapstate.adapters.pdk;

import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Reuses column JSON while checking every immutable bean property for changes. */
final class PdkTypeDescription {
    private static final Object[] NO_ARGUMENTS = new Object[0];
    private static final Set<Class<?>> IMMUTABLE = Set.of(String.class, Boolean.class, Byte.class,
            Short.class, Integer.class, Long.class, Float.class, Double.class, Character.class,
            BigDecimal.class, BigInteger.class, Instant.class);
    private static final ClassValue<List<Method>> READERS = new ClassValue<>() {
        @Override
        protected List<Method> computeValue(Class<?> type) {
            List<Method> getters = Arrays.stream(type.getMethods())
                    .filter(method -> !Modifier.isStatic(method.getModifiers()) && method.getParameterCount() == 0)
                    .filter(method -> !method.getName().equals("getClass"))
                    .filter(method -> method.getName().startsWith("get") || method.getName().startsWith("is"))
                    .sorted(Comparator.comparing(Method::getName)).toList();
            return getters.stream().allMatch(method -> method.getReturnType().isPrimitive()
                    || IMMUTABLE.contains(method.getReturnType())) ? getters : List.of();
        }
    };

    private final TapType type;
    private final List<Method> readers;
    private Object[] values;
    private String description;

    PdkTypeDescription(TapType type) {
        this.type = type;
        // Custom types and types containing mutable values retain the PDK's full serialization.
        readers = TapType.getTapTypeClass(type.getType()) == type.getClass() ? READERS.get(type.getClass()) : List.of();
    }

    synchronized String json() {
        if (readers.isEmpty()) return describe(type);
        if (values == null || changed()) {
            values = new Object[readers.size()];
            for (int index = 0; index < readers.size(); index++) values[index] = read(readers.get(index));
            description = describe(type);
        }
        return description;
    }

    private boolean changed() {
        for (int index = 0; index < readers.size(); index++) {
            if (!Objects.equals(values[index], read(readers.get(index)))) return true;
        }
        return false;
    }

    private Object read(Method reader) {
        try {
            return reader.invoke(type, NO_ARGUMENTS);
        } catch (ReflectiveOperationException failed) {
            throw new IllegalStateException("cannot read PDK type metadata", failed);
        }
    }

    static String describe(TapType type) {
        return Objects.requireNonNull(InstanceFactory.instance(JsonParser.class), "PDK JSON provider").toJson(type);
    }
}
