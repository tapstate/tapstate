package io.tapstate.tools;

import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** Build-time inspection only; this tool is never included in a product runtime. */
public final class ErrorContractSnapshot {
    public static void main(String[] args) throws Exception {
        Path root = Path.of(args[0]);
        List<String> rows = new ArrayList<>();
        try (var files = Files.walk(root.resolve("io/tapstate"))) {
            for (Path path : files.filter(p -> p.toString().endsWith(".class")).sorted().toList()) {
                String name = root.relativize(path).toString().replace(java.io.File.separatorChar, '.');
                var owner = Class.forName(name.substring(0, name.length() - 6), false,
                        ErrorContractSnapshot.class.getClassLoader());
                if (args.length > 1 && args[1].equals("typed")) {
                    var contract = Class.forName("io.tapstate.core.common.TapstateErrorCode");
                    if (owner.isEnum() && contract.isAssignableFrom(owner)) {
                        for (Object value : owner.getEnumConstants()) {
                            String code = (String) contract.getMethod("code").invoke(value);
                            Object severity = contract.getMethod("severity").invoke(value);
                            Set<?> parameters = (Set<?>) contract.getMethod("placeholders").invoke(value);
                            String names = String.join(",", parameters.stream().map(Object::toString).sorted().toList());
                            String constant = ((Enum<?>) value).name();
                            rows.add(String.join("\t", code, owner.getName() + "#" + constant,
                                    severity.toString(), names,
                                    owner.getField(constant).isAnnotationPresent(Deprecated.class) ? "deprecated" : "active"));
                        }
                    }
                    continue;
                }
                for (var field : owner.getDeclaredFields()) {
                    if (!field.getType().getName().equals("io.tapstate.core.common.ProductErrorDefinition")
                            || !Modifier.isStatic(field.getModifiers()) || !Modifier.isPublic(field.getModifiers())) continue;
                    Object value = field.get(null);
                    Class<?> definition = value.getClass();
                    String code = (String) definition.getMethod("code").invoke(value);
                    Object severity = definition.getMethod("severity").invoke(value);
                    Set<?> parameters = (Set<?>) definition.getMethod("parameters").invoke(value);
                    String names = String.join(",", parameters.stream().map(Object::toString).sorted().toList());
                    rows.add(String.join("\t", code, owner.getName() + "#" + field.getName(),
                            severity.toString(), names, field.isAnnotationPresent(Deprecated.class) ? "deprecated" : "active"));
                }
            }
        }
        rows.stream().sorted().forEach(System.out::println);
    }
}
