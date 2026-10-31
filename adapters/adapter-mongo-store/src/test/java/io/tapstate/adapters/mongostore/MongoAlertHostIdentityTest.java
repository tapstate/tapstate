package io.tapstate.adapters.mongostore;

import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.StorageSetupScope;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class MongoAlertHostIdentityTest {
    private static final StorageSetupScope ORIGINAL = new StorageSetupScope("cluster", "installation", "operation", 1);
    private static final StorageBinding BINDING = new StorageBinding(ORIGINAL, StorageBinding.Mode.OP, 1, "views",
            new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, "mongodb://fixture/cache", "cache", null));
    private static Document current(long generation) {
        return new Document("clusterId", "cluster").append("installationId", "installation")
                .append("operationId", "operation").append("generation", generation);
    }

    @Test void credentialRotationKeepsOnlyTheOriginalPersistentBindingAdmitted() {
        assertThat(MongoAlertHostIdentity.current(current(2), ORIGINAL, BINDING)).isTrue();
        assertThat(MongoAlertHostIdentity.current(current(2), ORIGINAL, null)).isFalse();
        var newer = new StorageSetupScope("cluster", "installation", "operation", 2);
        assertThat(MongoAlertHostIdentity.current(current(2), newer, BINDING)).isFalse();
        assertThat(MongoAlertHostIdentity.current(current(1), newer, new StorageBinding(newer,
                StorageBinding.Mode.OP, 2, "views", BINDING.cache()))).isFalse();
    }

    @Test void identityLossReplacementOrMalformedGenerationsNeverUseCredentialRotationAsAuthority() {
        assertThat(MongoAlertHostIdentity.current(null, ORIGINAL, BINDING)).isFalse();
        for (String field : new String[]{"clusterId", "installationId", "operationId"}) {
            assertThat(MongoAlertHostIdentity.current(current(2).append(field, "replacement"), ORIGINAL, BINDING)).isFalse();
        }
        for (Object generation : new Object[]{0L, -1L, 2, "2", null}) {
            assertThat(MongoAlertHostIdentity.current(current(2).append("generation", generation), ORIGINAL, BINDING)).isFalse();
        }
    }
}
