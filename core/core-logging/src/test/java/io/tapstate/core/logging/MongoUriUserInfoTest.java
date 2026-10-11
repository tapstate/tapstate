package io.tapstate.core.logging;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MongoUriUserInfoTest {

    @Test
    void redactsOnlyAuthorityUserInfoAndTracksRawAndDecodedPasswords() {
        String uri = "mongodb+srv://alice:pa%40ss@cluster.example/test?appName=tapstate";

        assertThat(MongoUriUserInfo.redact(uri))
                .isEqualTo("mongodb+srv://<redacted>@cluster.example/test?appName=tapstate");
        assertThat(MongoUriUserInfo.secretValues(uri))
                .containsExactly("alice:pa%40ss", "pa%40ss", "pa@ss");
        assertThat(MongoUriUserInfo.isRedactedDisplay(MongoUriUserInfo.redact(uri))).isTrue();
    }

    @Test
    void leavesAConnectionWithoutUserInfoVisible() {
        String uri = "mongodb://db-a.example:27017,db-b.example:27017/test?replicaSet=rs0";

        assertThat(MongoUriUserInfo.redact(uri)).isEqualTo(uri);
        assertThat(MongoUriUserInfo.secretValues(uri)).isEmpty();
        assertThat(MongoUriUserInfo.isRedactedDisplay(uri)).isFalse();
    }

    @Test
    void hidesAnUnparseableCredentialValueAndRejectsItsDisplayMarker() {
        for (String uri : new String[] {
                "alice:secret@cluster.example/test",
                "mongodb+srv://alice:pa?ss@cluster.example/test"}) {
            assertThat(MongoUriUserInfo.redact(uri)).isEqualTo(MongoUriUserInfo.REDACTED);
            assertThat(MongoUriUserInfo.secretValues(uri)).containsExactly(uri);
        }
        assertThat(MongoUriUserInfo.isRedactedDisplay(MongoUriUserInfo.REDACTED)).isTrue();
    }
}
