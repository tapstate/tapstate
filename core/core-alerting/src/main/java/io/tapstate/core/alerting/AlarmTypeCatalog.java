package io.tapstate.core.alerting;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Static type descriptions and explicit host capabilities; registration alone does not enable a provider. */
public final class AlarmTypeCatalog {
    public static final int RECORD_FORMAT_VERSION = 1;
    public enum Kind { STATEFUL, EVENT }
    public enum ParameterKind { DURATION_SECONDS, DECIMAL }
    public enum Availability { AVAILABLE, UNSUPPORTED_RECORD_VERSION, UNKNOWN_TYPE,
        UNSUPPORTED_TYPE_VERSION, INPUT_UNAVAILABLE, EVALUATOR_UNAVAILABLE }
    public record Parameter(String name, ParameterKind kind, String unit, BigDecimal minimum,
            BigDecimal maximum, BigDecimal defaultValue) {
        public Parameter {
            name = id(name, "parameterName");
            Objects.requireNonNull(kind, "kind");
            unit = AlertValidation.text(unit, "parameterUnit", 32);
            Objects.requireNonNull(minimum, "minimum");
            Objects.requireNonNull(maximum, "maximum");
            Objects.requireNonNull(defaultValue, "defaultValue");
            AlertValidation.require(minimum.compareTo(maximum) <= 0 && defaultValue.compareTo(minimum) >= 0
                    && defaultValue.compareTo(maximum) <= 0, "parameterRange", "invalid-default-or-range");
            if (kind == ParameterKind.DURATION_SECONDS) {
                AlertValidation.require(minimum.signum() >= 0 && maximum.compareTo(BigDecimal.valueOf(86400)) <= 0
                        && integral(minimum) && integral(maximum) && integral(defaultValue), "durationParameter", "invalid-seconds");
            }
        }
        public void validate(BigDecimal value) {
            AlertValidation.require(value != null && value.compareTo(minimum) >= 0 && value.compareTo(maximum) <= 0
                    && (kind != ParameterKind.DURATION_SECONDS || integral(value)), name, "outside-parameter-range");
        }
        private static boolean integral(BigDecimal value) { return value.stripTrailingZeros().scale() <= 0; }
    }
    /** Text keys are rendered by the host's fixed locale catalog, never evaluated as expressions. */
    public record Type(String id, int version, Kind kind, Set<String> resourceKinds, String providerId,
            String evaluatorId, List<Parameter> parameters, AlarmSeverity defaultSeverity, boolean enabledByDefault,
            String conditionTextKey, String recoveryTextKey, String guidanceTextKey) {
        public Type {
            id = AlarmTypeCatalog.id(id, "typeId");
            AlertValidation.require(version > 0, "typeVersion", "must-be-positive");
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(defaultSeverity, "defaultSeverity");
            resourceKinds = Set.copyOf(resourceKinds);
            AlertValidation.require(!resourceKinds.isEmpty() && resourceKinds.size() <= 8, "resourceKinds", "outside-1-8");
            resourceKinds.forEach(resource -> AlarmTypeCatalog.id(resource, "resourceKind"));
            providerId = AlarmTypeCatalog.id(providerId, "providerId");
            evaluatorId = AlarmTypeCatalog.id(evaluatorId, "evaluatorId");
            parameters = List.copyOf(parameters);
            AlertValidation.require(parameters.size() <= 16, "parameters", "too-many");
            AlertValidation.require(parameters.stream().map(Parameter::name).distinct().count() == parameters.size(), "parameters", "duplicate-name");
            conditionTextKey = AlarmTypeCatalog.id(conditionTextKey, "conditionTextKey");
            guidanceTextKey = AlarmTypeCatalog.id(guidanceTextKey, "guidanceTextKey");
            if (kind == Kind.STATEFUL) { recoveryTextKey = AlarmTypeCatalog.id(recoveryTextKey, "recoveryTextKey"); }
            else { AlertValidation.require(recoveryTextKey == null, "recoveryTextKey", "events-have-no-recovery"); }
        }
        public Map<String, BigDecimal> validatedParameters(Map<String, BigDecimal> supplied) {
            Map<String, BigDecimal> result = new LinkedHashMap<>();
            AlertValidation.require(supplied.size() <= parameters.size(), "parameters", "unknown-field");
            for (Parameter parameter : parameters) {
                BigDecimal value = supplied.getOrDefault(parameter.name(), parameter.defaultValue());
                parameter.validate(value);
                result.put(parameter.name(), value);
            }
            AlertValidation.require(result.keySet().containsAll(supplied.keySet()), "parameters", "unknown-field");
            return Map.copyOf(result);
        }
    }
    /** Fixed metric coordinates and the two existing bounded numeric controls used by rule authoring. */
    public record MetricPreset(MetricAlertEvaluator.Rule template,String thresholdParameter,String recoveryParameter) {
        public MetricPreset {Objects.requireNonNull(template);id(thresholdParameter,"thresholdParameter");id(recoveryParameter,"recoveryParameter");
            AlertValidation.require(!thresholdParameter.equals(recoveryParameter),"metricParameters","duplicate-name");}
        public MetricAlertEvaluator.Rule rule(Map<String,BigDecimal> parameters) {
            return new MetricAlertEvaluator.Rule(template.name(),template.type(),template.unit(),template.attributes(),template.direction(),
                    parameters.get(thresholdParameter),parameters.get(recoveryParameter));
        }
    }
    private final Map<String, Type> types;
    private final Map<String,MetricPreset> metrics;
    public AlarmTypeCatalog(Collection<Type> descriptors) {this(descriptors,Map.of());}
    public AlarmTypeCatalog(Collection<Type> descriptors,Map<String,MetricPreset> metrics) {
        AlertValidation.require(descriptors.size() <= 128, "types", "too-many");
        Map<String, Type> registered = new LinkedHashMap<>();
        for (Type type : descriptors) {
            AlertValidation.require(registered.putIfAbsent(type.id(), type) == null, "typeId", "duplicate-id");
        }
        types = Map.copyOf(registered);this.metrics=Map.copyOf(metrics);
        metrics.forEach((typeId,preset)->{
            Type type=types.get(typeId);
            AlertValidation.require(type!=null && type.evaluatorId().equals("threshold") && type.kind()==Kind.STATEFUL,"metricPreset","foreign-type");
            var parameters=type.validatedParameters(Map.of());
            AlertValidation.require(parameters.containsKey(preset.thresholdParameter()) && parameters.containsKey(preset.recoveryParameter()),"metricPreset","missing-controls");
            var threshold=type.parameters().stream().filter(parameter->parameter.name().equals(preset.thresholdParameter())).findFirst().orElseThrow();
            var recovery=type.parameters().stream().filter(parameter->parameter.name().equals(preset.recoveryParameter())).findFirst().orElseThrow();
            AlertValidation.require(threshold.unit().equals(preset.template().unit()) && recovery.unit().equals(preset.template().unit()),"metricPreset","parameter-unit-mismatch");
            preset.rule(parameters);
        });
    }
    public MetricAlertEvaluator.Rule metric(String typeId,Map<String,BigDecimal> parameters) {
        var preset=metrics.get(typeId);return preset==null?null:preset.rule(types.get(typeId).validatedParameters(parameters));
    }
    public MetricPreset metricPreset(String typeId){return metrics.get(typeId);}
    public List<Type> types() { return types.values().stream().sorted(java.util.Comparator.comparing(Type::id)).toList(); }
    public Availability availability(String id, int typeVersion, int recordVersion, Set<String> providers, Set<String> evaluators) {
        if (recordVersion != RECORD_FORMAT_VERSION) { return Availability.UNSUPPORTED_RECORD_VERSION; }
        Type type = types.get(id);
        if (type == null) { return Availability.UNKNOWN_TYPE; }
        if (type.version() != typeVersion) { return Availability.UNSUPPORTED_TYPE_VERSION; }
        if (!providers.contains(type.providerId())) { return Availability.INPUT_UNAVAILABLE; }
        if (!evaluators.contains(type.evaluatorId())) { return Availability.EVALUATOR_UNAVAILABLE; }
        return Availability.AVAILABLE;
    }
    private static String id(String value, String field) {
        AlertValidation.text(value, field, 128);
        AlertValidation.require(value.matches("[a-z][a-z0-9]*([.-][a-z0-9]+)*"), field, "invalid-id");
        return value;
    }
}
