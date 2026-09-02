package io.tapstate.archtests;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaModifier;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static com.tngtech.archunit.core.domain.JavaClass.Predicates.assignableTo;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAPackage;
import static com.tngtech.archunit.core.domain.properties.HasName.Predicates.name;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Package-level enforcement of the ring dependency rules (the fine-grained gate; the
 * enforcer bannedDependencies allowlist in the ring parents is the coarse-grained one).
 *
 * <p>Rings that do not exist yet (spi / cli ...) use {@code allowEmptyShould(true)}:
 * the rule idles while the packages are empty and becomes effective automatically as
 * soon as the first class appears - no test change needed.
 */
class RingDependencyRulesTest {

    private static JavaClasses tapstateClasses;

    @BeforeAll
    static void importTapstateClasses() {
        tapstateClasses = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("io.tapstate");
    }

    @Test
    @DisplayName("R1: core ring (except core-dsl / core-sql) depends only on java.., itself, and jackson-annotations")
    void r1_coreRingDependsOnWhitelistOnly() {
        noClasses().that().resideInAPackage("io.tapstate.core..")
                // core-dsl and core-sql each carry their own additional grant (see
                // r1_coreDslAlsoAllowsYamlParserAndCel / r1_coreSqlAlsoAllowsTheSqlFrontEnd);
                // every other core module is held to the zero-framework allowlist
                .and().resideOutsideOfPackage("io.tapstate.core.dsl..")
                .and().resideOutsideOfPackage("io.tapstate.core.sql..")
                .should().dependOnClassesThat().resideOutsideOfPackages(
                        "java..",
                        "io.tapstate.core..",
                        // annotations only, no runtime behavior
                        "com.fasterxml.jackson.annotation.."
                )
                .allowEmptyShould(true)
                .because("the core ring depends on no other ring; third-party dependencies are "
                        + "individually named, anything outside the allowlist is a red light")
                .check(tapstateClasses);
    }

    @Test
    @DisplayName("R1 (core-dsl grant): core-dsl adds the YAML parser and the CEL compiler, nothing more")
    void r1_coreDslAlsoAllowsYamlParserAndCel() {
        noClasses().that().resideInAPackage("io.tapstate.core.dsl..")
                .should().dependOnClassesThat().resideOutsideOfPackages(
                        "java..",
                        "io.tapstate.core..",
                        "com.fasterxml.jackson.annotation..",
                        // R1 named grants, core-dsl-only: the YAML parser (B3) and the CEL
                        // expression compiler (B4). cel-java's other transitive libraries
                        // (protobuf / antlr4) stay out of core-dsl's own bytecode surface.
                        "org.yaml.snakeyaml..",
                        "dev.cel..",
                        // Guava is not an independent grant: building a CEL type is impossible
                        // without it, because cel's own type API takes and returns guava
                        // collections in its signatures. It is confined to method bodies - no
                        // core-dsl type exposes a guava type in its own signature - so widening
                        // this grant does not widen what the rest of the platform can see.
                        "com.google.common.."
                )
                .allowEmptyShould(true)
                .because("the YAML parser and CEL compiler are granted to core-dsl alone; the rest "
                        + "of the core ring still bans them (enforcer pom grant is the coarse twin "
                        + "of this rule)")
                .check(tapstateClasses);
    }

    @Test
    @DisplayName("R1 (core-sql grant): core-sql adds the SQL parser and validator, nothing more")
    void r1_coreSqlAlsoAllowsTheSqlFrontEnd() {
        noClasses().that().resideInAPackage("io.tapstate.core.sql..")
                .should().dependOnClassesThat().resideOutsideOfPackages(
                        "java..",
                        "io.tapstate.core..",
                        "com.fasterxml.jackson.annotation..",
                        // R1 named grant, core-sql-only: the SQL parser, validator and type
                        // deriver. The library's other reachable artifacts (its JDBC driver base
                        // and a fraction arithmetic helper) are pulled in by static initializers
                        // on that path, not referenced by anything this module writes -- so they
                        // are named in the pom grant and deliberately not here.
                        "org.apache.calcite.."
                )
                .allowEmptyShould(true)
                .because("the SQL front end is granted to core-sql alone; the rest of the core "
                        + "ring still bans it (enforcer pom grant is the coarse twin of this rule)")
                .check(tapstateClasses);
    }

    @Test
    @DisplayName("R2: spi ring depends only on core")
    void r2_spiRingOnlyDependsOnCore() {
        classes().that().resideInAPackage("io.tapstate.spi..")
                .should().onlyDependOnClassesThat().resideInAnyPackage(
                        "java..",
                        "io.tapstate.spi..",
                        "io.tapstate.core..")
                .allowEmptyShould(true)
                .because("ports depend one-way on the kernel and on nothing else")
                .check(tapstateClasses);
    }

    @Test
    @DisplayName("R6: cli depends only on the core ring + the shared message catalog")
    void r6_cliOnlyDependsOnCoreRing() {
        classes().that().resideInAPackage("io.tapstate.cli..")
                .should().onlyDependOnClassesThat().resideInAnyPackage(
                        "java..",
                        "io.tapstate.cli..",
                        "io.tapstate.core..",
                        "io.tapstate.control.client..",
                        // the shared error-code message catalog + renderer (presentation layer)
                        "io.tapstate.messages..",
                        // the CLI's own facade libraries
                        "picocli..",
                        "org.jline..")
                .allowEmptyShould(true)
                .because("the CLI talks to services through the framework-free HTTP client only; it "
                        + "must have no dependency on control services or runtime modules; the message catalog is "
                        + "a presentation-layer leaf, not a service ring")
                .check(tapstateClasses);
    }

    @Test
    @DisplayName("control-client depends only on the core ring and the JDK HTTP client")
    void controlClientIsAFrameworkFreeTransportLeaf() {
        classes().that().resideInAPackage("io.tapstate.control.client..")
                .should().onlyDependOnClassesThat().resideInAnyPackage(
                        "java..",
                        "io.tapstate.control.client..",
                        "io.tapstate.core..")
                .because("the shared HTTP client is a framework-free transport leaf usable by the CLI and MCP")
                .check(tapstateClasses);
    }

    @Test
    @DisplayName("R5: mcp-server is a presentation sidecar over control-core and control-client")
    void r5_mcpServerLayering() {
        classes().that().resideInAPackage("io.tapstate.mcp..")
                .should().onlyDependOnClassesThat().resideInAnyPackage(
                        "java..",
                        "io.tapstate.mcp..",
                        "io.tapstate.control.client..",
                        "io.tapstate.control.core..",
                        "io.tapstate.core..",
                        "io.tapstate.messages..",
                        "io.modelcontextprotocol..",
                        "org.springframework..")
                .because("the local MCP presentation sidecar delegates only through the HTTP control contract")
                .check(tapstateClasses);
    }

    @Test
    @DisplayName("messages (shared presentation catalog) depends only on the core ring")
    void messagesModuleDependsOnCoreRingOnly() {
        classes().that().resideInAPackage("io.tapstate.messages..")
                .should().onlyDependOnClassesThat().resideInAnyPackage(
                        "java..",
                        "io.tapstate.messages..",
                        "io.tapstate.core..")
                .allowEmptyShould(true)
                .because("the shared message catalog renders coded errors for every presentation "
                        + "face; it depends only on the error-code contract in the core ring and "
                        + "carries no third-party, so no face inherits a library through it")
                .check(tapstateClasses);
    }

    @Test
    @DisplayName("R1 (tools): the catalog build tools depend only on java.., the core ring and themselves")
    void toolsDependOnTheCoreRingAndNothingElse() {
        // No allowEmptyShould(true) here, and that is the whole point of the assertion above it.
        // Elsewhere in this file the flag lets a rule idle until its ring exists; this prefix has a
        // module today, so an empty scan does not mean "not built yet" - it means catalog-assembler
        // fell off the arch-tests classpath and the rule below is checking nothing while reporting
        // the same green as compliance. ModuleRegistrationTest catches that slip from the pom side;
        // this catches it from the scan side, which is the side that stays silent on its own.
        assertThat(tapstateClasses.that(resideInAPackage("io.tapstate.tools..")))
                .as("the tools prefix must have classes on the scan classpath, or the rule below is "
                        + "green without having checked anything")
                .isNotEmpty();
        classes().that().resideInAPackage("io.tapstate.tools..")
                .should().onlyDependOnClassesThat().resideInAnyPackage(
                        "java..",
                        "io.tapstate.tools..",
                        "io.tapstate.core..")
                .because("the catalog build tools are a framework-free leaf over the kernel: they read "
                        + "spec and capability data and write the bundled catalog, so they carry no "
                        + "third-party library and no ring dependency. Holding them to this allowlist "
                        + "is what keeps the default build PDK-free and native-friendly")
                .check(tapstateClasses);
    }

    @Test
    @DisplayName("R3: adapters never reach up into runtime / control / surface rings")
    void r3_adaptersDoNotDependOnHigherRings() {
        // Adapters depend one-way on the ports and the kernel. Their third-party system
        // dependencies are open-ended (PDK / Mongo driver / future backends) and locked
        // per-module below, so the ring-level rule is expressed as a ban on upward edges
        // rather than a positive package allowlist.
        noClasses().that().resideInAPackage("io.tapstate.adapters..")
                .should().dependOnClassesThat().resideInAnyPackage(
                        "io.tapstate.runtime..",
                        "io.tapstate.control..",
                        "io.tapstate.cli..",
                        "io.tapstate.app..")
                .allowEmptyShould(true)
                .because("adapters depend one-way on the ports and the kernel only; they never "
                        + "reach up into the runtime, control, or surface rings")
                .check(tapstateClasses);
    }

    @Test
    @DisplayName("R3 (PDK lock): only adapter-pdk may import the PDK API")
    void r3_pdkLockedToAdapterPdk() {
        noClasses().that().resideOutsideOfPackage("io.tapstate.adapters.pdk..")
                .should().dependOnClassesThat().resideInAPackage("io.tapdata..")
                .allowEmptyShould(true)
                .because("the PDK API is locked to adapter-pdk; no other module may import it")
                .check(tapstateClasses);
    }

    @Test
    @DisplayName("R3 (catalog pipeline): the PDK-free side of the catalog pipeline holds no PDK reference")
    void theCatalogPipelinesPdkFreeSideHoldsNoPdkReference() {
        // The catalog pipeline is split by PDK exposure on purpose. catalog-derive classloads a
        // connector jar to read its capabilities, so it touches the PDK; it is kept out of the default
        // reactor precisely so that transitive tree never enters `mvn verify`. Registering it here to
        // guard it would trade a discipline problem for a heavy build - so it cannot be scanned, and
        // the boundary is asserted from the other side instead: the side that must stay clean, whose
        // classes are already on this classpath at zero build cost.
        //
        // r3_pdkLockedToAdapterPdk bans io.tapdata everywhere outside adapter-pdk and therefore covers
        // these packages too - but it idles empty-green by design, so if either package fell off the
        // classpath it would report exactly the same green as it does now. The two assertions below
        // are what this test adds: they pin that both halves of the PDK-free side were actually
        // scanned, and each is checked separately because a union is non-empty as soon as either
        // member is.
        assertThat(tapstateClasses.that(resideInAPackage("io.tapstate.core..")))
                .as("the kernel must be on the scan classpath for the ban below to mean anything")
                .isNotEmpty();
        assertThat(tapstateClasses.that(resideInAPackage("io.tapstate.tools.catalog.assembler..")))
                .as("the PDK-free assembler must be on the scan classpath for the ban below to mean "
                        + "anything - it is the half that shares a package prefix with the PDK-touching "
                        + "deriver, so it is the half worth proving was read")
                .isNotEmpty();
        noClasses().that().resideInAnyPackage(
                        "io.tapstate.core..",
                        "io.tapstate.tools.catalog.assembler..")
                .should().dependOnClassesThat().resideInAPackage("io.tapdata..")
                .because("the catalog pipeline is split by PDK exposure: the deriver classloads "
                        + "connector jars, the assembler and the kernel never do. That split is what "
                        + "lets the assembler run in every build while the deriver runs only when "
                        + "connectors are present")
                .check(tapstateClasses);
    }

    @Test
    @DisplayName("R3 (Mongo lock): only adapter-mongo-store may depend on the Mongo driver")
    void r3_mongoDriverLockedToAdapterMongoStore() {
        noClasses().that().resideOutsideOfPackage("io.tapstate.adapters.mongostore..")
                .should().dependOnClassesThat().resideInAnyPackage(
                        "com.mongodb..",
                        "org.bson..")
                .allowEmptyShould(true)
                .because("the Mongo driver is locked to adapter-mongo-store; no other module may "
                        + "depend on it")
                .check(tapstateClasses);
    }

    @Test
    @DisplayName("R4: runtime depends on core + spi (+ Hazelcast) only, never on adapters")
    void r4_runtimeRingLayering() {
        classes().that().resideInAPackage("io.tapstate.runtime..")
                .should().onlyDependOnClassesThat().resideInAnyPackage(
                        "java..",
                        "io.tapstate.runtime..",
                        "io.tapstate.spi..",
                        "io.tapstate.core..",
                        // the runtime's execution substrate (the Hazelcast fork)
                        "com.hazelcast..")
                .allowEmptyShould(true)
                .because("the runtime depends on the ports and the kernel (and Hazelcast) only; "
                        + "adapters are injected by the app assembly root, never compiled in")
                .check(tapstateClasses);
    }

    @Test
    @DisplayName("R5: control-core depends on core + the storage port + the connection-probe seam only (framework-free — no Spring)")
    void r5_controlCoreLayering() {
        classes().that().resideInAPackage("io.tapstate.control.core..")
                .should().onlyDependOnClassesThat().resideInAnyPackage(
                        "java..",
                        "io.tapstate.control.core..",
                        "io.tapstate.core..",
                        // control-core decouples from the runtime through the storage port
                        "io.tapstate.spi.store..",
                        // the synchronous control-to-runtime seam: the probe whitelist (a closed set
                        // of six — connection test, schema discovery, and the four query channels).
                        // Every other control<->runtime interaction stays store-decoupled; this
                        // narrow channel is the one compile reference control-core holds into the
                        // runtime ring
                        "io.tapstate.runtime.probe..")
                .allowEmptyShould(true)
                .because("control-core is the resource-type-agnostic verb layer: pure logic that "
                        + "depends on the kernel, the storage port, and the synchronous probe "
                        + "whitelist only. It stays framework-free — Spring lives in "
                        + "rest-api (the HTTP presentation face), never here — so the apply / registry "
                        + "logic is unit-testable without a container; it reaches the runtime only "
                        + "through the store, save for the probe whitelist (a closed set of six)")
                .check(tapstateClasses);
    }

    @Test
    @DisplayName("R5: rest-api depends on control-core + core + the shared message catalog (not the ports)")
    void r5_restApiLayering() {
        classes().that().resideInAPackage("io.tapstate.control.restapi..")
                .should().onlyDependOnClassesThat().resideInAnyPackage(
                        "java..",
                        "io.tapstate.control.restapi..",
                        "io.tapstate.control.core..",
                        "io.tapstate.core..",
                        // the shared error-code message catalog + renderer (presentation layer): rest-api
                        // renders coded errors the same way the CLI does (its R6 grant), a leaf not a ring
                        "io.tapstate.messages..",
                        // Spring is permitted in the control ring (rest-api is the HTTP layer)
                        "org.springframework..",
                        // Jackson annotations and databind are the JSON substrate used only by the HTTP
                        // projection to enforce request shape and response omission rules.
                        "com.fasterxml.jackson.annotation..",
                        "tools.jackson.databind..",
                        // the Servlet API is the substrate the Web MVC servlet stack runs on; the HTTP
                        // layer's interceptor and controllers read the request through it
                        "jakarta.servlet..")
                .allowEmptyShould(true)
                .because("the HTTP presentation adapter sits on control-core, the kernel, the shared "
                        + "message catalog and the servlet substrate; it does not reach the ports directly")
                .check(tapstateClasses);
    }

    @Test
    @DisplayName("R5 (exactness): the control-to-runtime sync whitelist is exactly the six named probes "
            + "— no further channel")
    void r5_controlToRuntimeSyncWhitelistHasNoFurtherChannel() {
        // A control-to-runtime sync channel is a runtime interface control reaches for. The whitelist is
        // a closed set of exactly six such interfaces: connection test, schema discovery, and the four
        // channels the data browser needs (listing a source's collections, reading its documents,
        // reading its table stats, and following its changes). The probes' value types are storage-port
        // types (the connection config or query request they take, the result they return), carried as
        // payload, not channels of their own. This gate bans a further channel — another probe interface
        // control depends on. Widening the whitelist must change this gate and the sync-whitelist
        // decision, not slip in beside it (a seventh probe interface control reaches for turns this red).
        //
        // The set is spelled out here because the decision is what the names encode; the four query
        // channels are named ahead of the code that implements them, so until then this gate is green
        // without proving anything. What it does from the first day is refuse a seventh.
        DescribedPredicate<JavaClass> aRuntimeSyncChannelOutsideTheWhitelist =
                resideInAPackage("io.tapstate.runtime.probe..")
                        .and(DescribedPredicate.describe("interfaces", JavaClass::isInterface))
                        .and(DescribedPredicate.not(name("io.tapstate.runtime.probe.ConnectionProbe")))
                        .and(DescribedPredicate.not(name("io.tapstate.runtime.probe.SchemaDiscoveryProbe")))
                        .and(DescribedPredicate.not(name("io.tapstate.runtime.probe.DataBrowserCollectionsProbe")))
                        .and(DescribedPredicate.not(name("io.tapstate.runtime.probe.DataBrowserFindProbe")))
                        .and(DescribedPredicate.not(name("io.tapstate.runtime.probe.DataBrowserStatsProbe")))
                        .and(DescribedPredicate.not(name("io.tapstate.runtime.probe.DataBrowserTailProbe")))
                        .as("a control-to-runtime sync channel outside the whitelist");
        noClasses().that().resideInAPackage("io.tapstate.control..")
                .should().dependOnClassesThat(aRuntimeSyncChannelOutsideTheWhitelist)
                .allowEmptyShould(true)
                .because("the control-to-runtime sync whitelist is a closed set of exactly six members "
                        + "— the connection probe, the schema-discovery probe and the four query probes; "
                        + "a further synchronous channel must change this gate and the sync-whitelist "
                        + "decision, not slip in beside it")
                .check(tapstateClasses);
    }

    @Test
    @DisplayName("R5 (exactness): every probe in the whitelist package exposes exactly one operation")
    void r5_everyWhitelistedProbeExposesExactlyOneOperation() {
        // Each whitelist member is a single operation. Counting all abstract methods — including any
        // inherited from a super-interface, not only the probe's own — a second operation (added
        // directly or pulled in through a super-interface) is a further synchronous control-to-runtime
        // call and must change the sync-whitelist decision, not slip in.
        //
        // Derived from the package rather than a list of names, and that is the point. The cheap way
        // around a closed set of channels is a union: one probe carrying several verbs, or one method
        // taking a discriminator. After a union lands, the next verb costs a method or a case and
        // touches nothing any gate watches — the closure is gone for that whole family. Derived, a
        // union is red the moment it is written, rather than when someone remembers to extend a list.
        // package-info compiles to an interface too, and it is not a channel — exclude it by name.
        JavaClasses probes = tapstateClasses.that(resideInAPackage("io.tapstate.runtime.probe..")
                .and(DescribedPredicate.describe("interfaces", JavaClass::isInterface))
                .and(DescribedPredicate.describe(
                        "not package-info", type -> !type.getName().endsWith(".package-info"))));
        assertThat(probes)
                .as("the whitelist package holds the probe interfaces themselves — an empty result "
                        + "would make every assertion below vacuous")
                .isNotEmpty();
        for (JavaClass probe : probes) {
            long operations = probe.getAllMethods().stream()
                    .filter(method -> method.getModifiers().contains(JavaModifier.ABSTRACT))
                    .count();
            assertThat(operations)
                    .as("each whitelisted probe is a closed set of exactly one operation: " + probe.getName())
                    .isEqualTo(1L);
        }
    }

    @Test
    @DisplayName("R5 (exactness): control-core never drives the spi execution ports directly — the probes "
            + "are the only path")
    void r5_controlCoreDoesNotBypassTheProbesIntoTheSpiExecutionPorts() {
        // The ring grant allows control-core to see all of io.tapstate.spi.store (it holds the storage
        // ports), and the spi execution ports — the ports the probes delegate to, the ones that drive a
        // connector — live in that same package. Compiling against them from control-core would silently
        // bypass the runtime seam: legal to the package rule above, but a reversal of the sync-whitelist
        // decision (testing, discovery and data-browser reads run where the connectors run — the runtime
        // side). This gate turns that bypass red instead of leaving it to prose.
        //
        // Identified by the marker interface, not by name. A name list fails in the worst direction:
        // a port spelled differently than the list expects never matches, so the gate stays green while
        // covering nothing, and nothing says so. With the marker, a new execution port joins this gate
        // by implementing it — and the assertion below is what keeps the existing ones from quietly
        // dropping out of coverage if someone removes the marker.
        DescribedPredicate<JavaClass> anSpiExecutionPort =
                assignableTo("io.tapstate.spi.store.ExecutionPort")
                        .as("an spi execution port (a port that drives a connector)");
        List<String> markedPorts = new ArrayList<>();
        for (JavaClass marked : tapstateClasses.that(anSpiExecutionPort)) {
            markedPorts.add(marked.getName());
        }
        assertThat(markedPorts)
                .as("every connector-driving port carries the execution-port marker")
                .contains(
                        "io.tapstate.spi.store.ConnectionTester",
                        "io.tapstate.spi.store.SchemaDiscoverer",
                        "io.tapstate.spi.store.DataBrowser");
        noClasses().that().resideInAPackage("io.tapstate.control..")
                .should().dependOnClassesThat(anSpiExecutionPort)
                .allowEmptyShould(true)
                .because("control drives the connectors only through the whitelisted runtime probes; a "
                        + "direct compile reference to an spi execution port bypasses the seam and "
                        + "reverses the sync-whitelist decision")
                .check(tapstateClasses);
    }

    @Test
    @DisplayName("R9: control and runtime hold no compile reference to each other, save the connection-probe whitelist")
    void r9_controlAndRuntimeDoNotReferenceEachOther() {
        // The control-to-runtime half carries the single R5 exception: control may reach the runtime
        // synchronously only through the probe whitelist (io.tapstate.runtime.probe), a closed set of
        // six. Any other runtime package is still forbidden; the exactness gates above pin that
        // whitelist to exactly six channels of one operation each.
        noClasses().that().resideInAPackage("io.tapstate.control..")
                .should().dependOnClassesThat(
                        JavaClass.Predicates.resideInAPackage("io.tapstate.runtime..")
                                .and(JavaClass.Predicates.resideOutsideOfPackage("io.tapstate.runtime.probe..")))
                .allowEmptyShould(true)
                .because("control writes desired state and the runtime watches and converges; they "
                        + "decouple through the store and hold no reference to each other — the sole "
                        + "exception is the synchronous probe whitelist "
                        + "(io.tapstate.runtime.probe), a closed set of six")
                .check(tapstateClasses);
        // The runtime-to-control half stays a blanket ban: the runtime never reaches up into control.
        noClasses().that().resideInAPackage("io.tapstate.runtime..")
                .should().dependOnClassesThat().resideInAPackage("io.tapstate.control..")
                .allowEmptyShould(true)
                .because("control writes desired state and the runtime watches and converges; the "
                        + "runtime holds no reference back into the control ring")
                .check(tapstateClasses);
    }

    @Test
    @DisplayName("R7: the app assembly root is the only non-adapter module allowed to depend on adapters")
    void r7_onlyAppMayDependOnAdapters() {
        noClasses().that().resideOutsideOfPackages("io.tapstate.app..", "io.tapstate.adapters..")
                .should().dependOnClassesThat().resideInAPackage("io.tapstate.adapters..")
                .allowEmptyShould(true)
                .because("the app assembly root is the single place that wires adapters into the "
                        + "runtime (the conditional --role loading point)")
                .check(tapstateClasses);
    }
}
