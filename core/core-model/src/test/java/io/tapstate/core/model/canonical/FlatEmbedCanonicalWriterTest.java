package io.tapstate.core.model.canonical;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.TransformResource;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FlatEmbedCanonicalWriterTest {

    @Test
    void flatEmbedsWriteWithoutAPathOrArrayKey() {
        TransformResource transform = new TransformResource(
                "customer_profile",
                null,
                new TransformBody.Nest(null, null, new NestRoot(
                        "customer",
                        List.of("id"),
                        null,
                        null,
                        List.of(new Embed(
                                "profile",
                                Map.of("customer_id", "id"),
                                EmbedAs.FLAT,
                                null,
                                null,
                                null,
                                null,
                                null)))),
                null);

        assertThat(new CanonicalWriter().write(transform)).isEqualTo("""
                version: tapstate/v1
                kind: transform
                id: customer_profile
                type: nest
                root:
                  from: customer
                  key: [id]
                  embed:
                    - from: profile
                      on:
                        customer_id: id
                      as: flat
                """);
    }
}
