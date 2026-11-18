package io.tapstate.core.common;

import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** Explicit product module allocations; module numbers are never inferred from upstream codes. */
public enum ProductErrorModule {
    DSL("001", DslErrorDefinitions::registered),
    CONNECTOR("006", ConnectorErrorDefinitions::registered),
    CONTROL("021", RequestErrorDefinitions::registered),
    SOURCE("022", SourceErrorDefinitions::registered),
    PIPELINE("023", PipelineErrorDefinitions::registered),
    ARTIFACT("024", ArtifactErrorDefinitions::registered),
    PIPELINE_DRAFT("025", PipelineDraftErrorDefinitions::registered),
    MCP("026", McpErrorDefinitions::registered),
    SCHEMA("027", SchemaErrorDefinitions::registered),
    LIFECYCLE("028", LifecycleErrorDefinitions::registered),
    START_CHECK("029", StartCheckDefinitions::registered),
    VALIDATION("030", ValidationErrorDefinitions::registered),
    BOOT("031", BootErrorDefinitions::registered),
    ROLE("032", RoleErrorDefinitions::registered),
    CLI("033", CliErrorDefinitions::registered),
    ALERT("034", AlertErrorDefinitions::registered),
    CAPTURE("035", CaptureErrorDefinitions::registered),
    ACTUATION("036", ActuationErrorDefinitions::registered),
    CLUSTER("037", ClusterErrorDefinitions::registered),
    CONNECTOR_CATALOG("038", ConnectorCatalogErrorDefinitions::registered),
    DATA_BROWSER("039", DataBrowserErrorDefinitions::registered),
    MONITOR("040", MonitorErrorDefinitions::registered),
    POSITION("041", PositionErrorDefinitions::registered),
    MIGRATION("042", MigrationErrorDefinitions::registered),
    ROW_VALUE("043", RowValueErrorDefinitions::registered),
    ENGINE("044", EngineErrorDefinitions::registered),
    TRANSFORM("045", TransformErrorDefinitions::registered),
    NEST_SIZING("046", NestSizingErrorDefinitions::registered),
    STORE("047", StoreErrorDefinitions::registered),
    IO("048", IoErrorDefinitions::registered),
    NEST("049", NestErrorDefinitions::registered);

    private final String number;

    private final Supplier<List<ProductErrorDefinition>> definitions;

    ProductErrorModule(String number, Supplier<List<ProductErrorDefinition>> definitions) {
        this.number = number;
        this.definitions = definitions;
    }

    private static final class Registered {
        static final Set<String> CODES = java.util.Arrays.stream(values())
                .flatMap(module -> module.definitions.get().stream())
                .map(ProductErrorDefinition::code)
                .collect(Collectors.toUnmodifiableSet());
    }

    public static boolean isRegistered(String code) {
        return code != null && Registered.CODES.contains(code);
    }

    public String number() { return number; }
}
