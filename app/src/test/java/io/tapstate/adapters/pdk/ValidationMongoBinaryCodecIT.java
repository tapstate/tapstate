package io.tapstate.adapters.pdk;

import io.tapdata.entity.schema.type.TapBinary;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Real registered connector codec; no database, driver query, native sort or validation lifecycle proof. */
class ValidationMongoBinaryCodecIT {
    @Test void realMongoBinaryCodecRetainsSubtypeAndExactBytesThroughPortableOwnership() throws Throwable {
        String configured = System.getProperty("tapstate.pdk.it.mongodb-jar");
        assumeTrue(configured != null && !configured.isBlank(), "a real MongoDB connector jar is required");
        var classpath = List.of(Path.of(configured));
        var artifact = new ConnectorIntrospector().introspect(classpath);
        var ref = new ConnectorRef(classpath, artifact.className(), artifact.pdkApiVersion(), null, artifact.spec());
        // Opening registers capabilities/codecs. No init or database operation is invoked.
        try (var connector = PdkConnector.open("mongodb", ref, Map.of())) {
            connector.underLoader(() -> {
                Class<?> binaryClass = Class.forName("org.bson.types.Binary", true,
                        Thread.currentThread().getContextClassLoader());
                var decoder = new PdkValidationCodecs("mongodb", "retained-artifact", connector.codecs());
                for (int subtype : List.of(0, 4, 128, 255)) {
                    byte[] bytes = { 0, 1, -1, 64 };
                    Object nativeValue = binaryClass.getConstructor(byte.class, byte[].class)
                            .newInstance((byte)subtype, bytes);
                    var selected = decoder.select("payload", nativeValue, new TapBinary());
                    assertThat(selected.origin()).isEqualTo(PdkValidationCodecs.Origin.CUSTOM);
                    var owned = decoder.binary("payload", selected, 128);
                    assertThat(owned.subtype()).isEqualTo(subtype);
                    assertThat(owned.bytes()).containsExactly((byte)0, (byte)1, (byte)-1, (byte)64);
                    byte[] returned = owned.bytes();
                    returned[0] = 99;
                    assertThat(owned.bytes()[0]).isZero();
                }
                return null;
            });
        }
    }
}
