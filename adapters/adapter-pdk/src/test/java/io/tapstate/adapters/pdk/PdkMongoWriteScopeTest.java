package io.tapstate.adapters.pdk;

import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.TransactionBeginFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PdkMongoWriteScopeTest {
    @Test
    void disabledInspectorDoesNotClaimAnAcknowledgedScope() {
        PdkMongoWriteScope scope = PdkMongoWriteScope.disabled();
        assertThat(scope.afterWrite(scope.beforeWrite()))
                .isEqualTo("state=DISABLED;reason=DISABLED;concern=UNKNOWN");
    }

    @Test
    void absentConnectorAndForeignTokensRemainUnknown() {
        PdkMongoWriteScope scope = PdkMongoWriteScope.enabled(null);
        assertThat(scope.afterWrite(scope.beforeWrite()))
                .isEqualTo("state=UNKNOWN;reason=CONNECTOR_UNAVAILABLE;concern=UNKNOWN");
        assertThat(scope.afterWrite(PdkMongoWriteScope.disabled().beforeWrite()))
                .isEqualTo("state=UNKNOWN;reason=CALL_TOKEN_MISMATCH;concern=UNKNOWN");
        assertThat(scope.afterWrite(null)).contains("state=UNKNOWN", "CALL_TOKEN_MISMATCH");
    }

    @Test
    void unsupportedConnectorKeepsItsOriginalFunctions(@TempDir Path dir) {
        Path jar = Synthetic.countingSink(dir);
        try (PdkConnector connector = PdkConnector.open("demo",
                new ConnectorRef(List.of(jar), "synthetic.CountingSink", "2.0.8", null), Map.of())) {
            TransactionBeginFunction original = context -> {};
            connector.functions().supportTransactionBeginFunction(original);
            var write = connector.functions().getWriteRecordFunction();
            PdkMongoWriteScope scope = PdkMongoWriteScope.enabled(connector);
            assertThat(scope.afterWrite(scope.beforeWrite()))
                    .isEqualTo("state=UNKNOWN;reason=UNSUPPORTED_CONNECTOR;concern=UNKNOWN");
            assertThat(connector.functions().getTransactionBeginFunction()).isSameAs(original);
            assertThat(connector.functions().getWriteRecordFunction()).isSameAs(write);
            assertThat(PdkMongoWriteScope.enabled(connector)).isSameAs(scope);
        }
    }

    @Test
    void matchingClassNameCannotSubstituteForThePinnedBytes(@TempDir Path dir) {
        String name = "io.tapdata.mongodb.MongodbConnector";
        String source = """
                package io.tapdata.mongodb;
                import io.tapdata.pdk.apis.TapConnector;
                import io.tapdata.pdk.apis.functions.ConnectorFunctions;
                import io.tapdata.entity.codec.TapCodecsRegistry;
                import io.tapdata.pdk.apis.context.TapConnectionContext;
                import io.tapdata.pdk.apis.entity.ConnectionOptions;
                import io.tapdata.pdk.apis.entity.TestItem;
                import io.tapdata.entity.schema.TapTable;
                import java.util.List;
                import java.util.function.Consumer;
                public class MongodbConnector implements TapConnector {
                    public void registerCapabilities(ConnectorFunctions functions, TapCodecsRegistry codecs) {}
                    public void init(TapConnectionContext context) {}
                    public void stop(TapConnectionContext context) {}
                    public void discoverSchema(TapConnectionContext context, List<String> tables, int size,
                            Consumer<List<TapTable>> consumer) {}
                    public ConnectionOptions connectionTest(TapConnectionContext context,
                            Consumer<TestItem> consumer) { return ConnectionOptions.create(); }
                    public int tableCount(TapConnectionContext context) { return 0; }
                }
                """;
        Path jar = SyntheticJar.compileToJar(dir, name, source);
        try (PdkConnector connector = PdkConnector.open("mongodb",
                new ConnectorRef(List.of(jar), name, "2.0.8", null), Map.of())) {
            PdkMongoWriteScope scope = PdkMongoWriteScope.enabled(connector);
            assertThat(scope.afterWrite(scope.beforeWrite()))
                    .isEqualTo("state=UNKNOWN;reason=CONNECTOR_BYTES_UNSUPPORTED;concern=UNKNOWN");
        }
    }

    @Test
    void transactionEntryAndReturnPreserveTheOriginalBehavior() throws Throwable {
        AtomicInteger begun = new AtomicInteger(), rolledBack = new AtomicInteger();
        IllegalStateException originalFailure = new IllegalStateException("fixture failure");
        ConnectorFunctions functions = new ConnectorFunctions()
                .supportTransactionBeginFunction(context -> begun.incrementAndGet())
                .supportTransactionCommitFunction(context -> { throw originalFailure; })
                .supportTransactionRollbackFunction(context -> rolledBack.incrementAndGet());
        PdkMongoWriteScope.Transactions observed = PdkMongoWriteScope.Transactions.install(functions);
        assertThat(observed.rejection()).isNull();
        functions.getTransactionBeginFunction().begin(null);
        assertThatThrownBy(() -> functions.getTransactionCommitFunction().commit(null)).isSameAs(originalFailure);
        functions.getTransactionRollbackFunction().rollback(null);
        assertThat(begun).hasValue(1);
        assertThat(rolledBack).hasValue(1);
        assertThat(observed.rejection()).isEqualTo("TRANSACTION_FUNCTION_INVOKED");
    }

    @Test
    void aTransientSessionCannotDisappearFromTransactionEvidence() throws Throwable {
        AtomicBoolean sessionPresent = new AtomicBoolean();
        ConnectorFunctions functions = new ConnectorFunctions()
                .supportTransactionBeginFunction(context -> sessionPresent.set(true))
                .supportTransactionCommitFunction(context -> sessionPresent.set(false))
                .supportTransactionRollbackFunction(context -> sessionPresent.set(false));
        PdkMongoWriteScope.Transactions observed = PdkMongoWriteScope.Transactions.install(functions);
        assertThat(sessionPresent).isFalse();
        functions.getTransactionBeginFunction().begin(null);
        functions.getTransactionCommitFunction().commit(null);
        assertThat(sessionPresent).isFalse();
        assertThat(observed.rejection()).isEqualTo("TRANSACTION_FUNCTION_INVOKED");
    }

    @Test
    void replacingATransactionWrapperInvalidatesTheObservation() {
        ConnectorFunctions functions = inertTransactions();
        PdkMongoWriteScope.Transactions observed = PdkMongoWriteScope.Transactions.install(functions);
        assertThat(observed.rejection()).isNull();
        functions.supportTransactionBeginFunction(context -> {});
        assertThat(observed.rejection()).isEqualTo("TRANSACTION_WRAPPER_CHANGED");
    }

    @Test
    void anUnobservedTransactionEntryRemainsUnknown() {
        ConnectorFunctions functions = new ConnectorFunctions().supportTransactionBeginFunction(context -> {});
        assertThat(PdkMongoWriteScope.Transactions.install(functions).rejection())
                .isEqualTo("TRANSACTION_OBSERVATION_UNAVAILABLE");
    }

    @Test
    void sharedObserversAreNotWrappedAgainAndKeepPriorTransactionEvidence() throws Throwable {
        ConnectorFunctions functions = inertTransactions();
        PdkMongoWriteScope.Transactions first = PdkMongoWriteScope.Transactions.install(functions);
        var begin = functions.getTransactionBeginFunction();
        functions.getTransactionBeginFunction().begin(null);
        assertThat(PdkMongoWriteScope.Transactions.install(functions)).isSameAs(first);
        assertThat(functions.getTransactionBeginFunction()).isSameAs(begin);
        assertThat(first.rejection()).isEqualTo("TRANSACTION_FUNCTION_INVOKED");
    }

    @Test
    void threadNamesCannotAliasDifferentCallingThreads() {
        PdkMongoWriteScope.ThreadOwners owners = new PdkMongoWriteScope.ThreadOwners();
        assertThat(owners.claim("writer", 1)).isNull();
        assertThat(owners.claim("writer", 1)).isNull();
        assertThat(owners.claim("writer", 2)).isEqualTo("THREAD_NAME_REUSED");
        assertThat(owners.claim("other", 3)).isEqualTo("THREAD_NAME_REUSED");
    }

    @Test
    void threadObservationHasFiniteMemory() {
        PdkMongoWriteScope.ThreadOwners owners = new PdkMongoWriteScope.ThreadOwners();
        for (int index = 0; index < 128; index++) assertThat(owners.claim("writer_" + index, index)).isNull();
        assertThat(owners.claim("overflow", 129)).isEqualTo("THREAD_OWNER_LIMIT");
        assertThat(new PdkMongoWriteScope.ThreadOwners().claim("x".repeat(257), 1))
                .isEqualTo("THREAD_NAME_LIMIT");
    }

    @Test
    void concernEvidencePreservesEqualityWithoutPublishingCustomNames() {
        assertThat(PdkMongoWriteScope.concern(1, null, null)).isEqualTo("w:1,j:DEFAULT,timeoutMs:DEFAULT");
        assertThat(PdkMongoWriteScope.concern("majority", true, 1000))
                .isEqualTo("w:MAJORITY,j:true,timeoutMs:1000");
        String custom = "private-tag\nwith-delimiters;\u00e9";
        String evidence = PdkMongoWriteScope.concern(custom, false, 0);
        assertThat(evidence).matches("w:TAG_SHA256_[0-9a-f]{64},j:false,timeoutMs:0").hasSizeLessThan(150);
        assertThat(evidence).doesNotContain(custom, "private-tag", "\n", "\u00e9");
        assertThat(PdkMongoWriteScope.concern(custom, false, 0)).isEqualTo(evidence);
        assertThat(PdkMongoWriteScope.concern(custom + "x", false, 0)).isNotEqualTo(evidence);
    }

    @Test
    void invalidConcernShapesCannotCreateEvidence() {
        assertThat(PdkMongoWriteScope.concern(new Object(), null, null)).isNull();
        assertThat(PdkMongoWriteScope.concern("x".repeat(4097), null, null)).isNull();
        assertThat(PdkMongoWriteScope.concern(1, null, -1)).isNull();
        assertThat(PdkMongoWriteScope.concern(-1, null, null)).isNull();
    }

    private static ConnectorFunctions inertTransactions() {
        return new ConnectorFunctions().supportTransactionBeginFunction(context -> {})
                .supportTransactionCommitFunction(context -> {}).supportTransactionRollbackFunction(context -> {});
    }
}
