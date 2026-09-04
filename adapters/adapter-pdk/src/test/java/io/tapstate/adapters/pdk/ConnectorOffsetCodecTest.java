package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The codec that renders a connector's own stream offset as the opaque token a source position carries,
 * and reads that token back into the object the connector issued.
 *
 * <p>Both directions are needed for a resume to exist at all: a position is recorded as a token, outlives
 * the process, and is handed back to a connector that will only accept an offset of its own making.
 */
class ConnectorOffsetCodecTest {

    /** A connector's own offset object, in the shape the real ones have: a serializable bean. */
    static final class BinlogPosition implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String filename;
        private final long position;

        BinlogPosition(String filename, long position) {
            this.filename = filename;
            this.position = position;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof BinlogPosition p
                    && Objects.equals(filename, p.filename) && position == p.position;
        }

        @Override
        public int hashCode() {
            return Objects.hash(filename, position);
        }
    }

    /** An offset object that cannot be written down at all. */
    static final class Unserializable {
    }

    @Test
    void carriesAConnectorOffsetObjectThroughATokenAndBack() {
        BinlogPosition offset = new BinlogPosition("mysql-bin.000003", 4711L);

        String token = ConnectorOffsetCodec.toToken("mysql", offset);
        Object back = ConnectorOffsetCodec.fromToken("mysql", token, getClass().getClassLoader());

        assertThat(back).isEqualTo(offset);
    }

    /** The token is a string, so it survives every boundary a position crosses on the way to storage. */
    @Test
    void rendersTheTokenAsTextThatSurvivesAPersistenceBoundary() {
        String token = ConnectorOffsetCodec.toToken("mysql", new BinlogPosition("mysql-bin.000003", 4711L));

        assertThat(token).isNotBlank().matches("[A-Za-z0-9+/=]+");
    }

    /**
     * A map offset -- the other shape connectors issue -- round-trips as its own type, not as whatever a
     * generic reader would have guessed it to be.
     */
    @Test
    void carriesAMapOffsetBackAsAMap() {
        Map<String, Object> offset = new LinkedHashMap<>();
        offset.put("lsn", 987654321L);
        offset.put("slot", "tapstate_slot");

        Object back = ConnectorOffsetCodec.fromToken(
                "postgres", ConnectorOffsetCodec.toToken("postgres", offset), getClass().getClassLoader());

        assertThat(back).isEqualTo(offset);
    }

    /**
     * The class is resolved through the connector's own loader, never the host's. A connector's offset
     * type lives in the connector jar and is invisible to the host, so a decode that resolved against the
     * host loader would fail on every real connector while passing every test whose fixture the host can
     * see. The witness is a loader that refuses this one class: the decode must fail, because that is the
     * loader it asked.
     */
    @Test
    void resolvesTheOffsetClassThroughTheConnectorsOwnLoader() {
        String token = ConnectorOffsetCodec.toToken("mysql", new BinlogPosition("mysql-bin.000003", 4711L));
        ClassLoader refusesTheOffsetClass = new ClassLoader(null) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                if (name.contains("BinlogPosition")) {
                    throw new ClassNotFoundException(name);
                }
                return ConnectorOffsetCodecTest.class.getClassLoader().loadClass(name);
            }
        };

        assertThatThrownBy(() -> ConnectorOffsetCodec.fromToken("mysql", token, refusesTheOffsetClass))
                .isInstanceOf(TapstateException.class)
                .extracting(e -> ((TapstateException) e).code())
                .isEqualTo(ConnectorError.POSITION_UNREADABLE);
    }

    /**
     * A recorded position this connector can no longer read is a coded refusal naming the connector, not a
     * bare crash and not a silent fall back to reading from the present -- which is the shape that loses
     * every change made since the position was recorded, with nothing thrown and nothing logged.
     */
    @Test
    void refusesAnUnreadableTokenWithACode() {
        assertThatThrownBy(() -> ConnectorOffsetCodec.fromToken(
                "mysql", "not-a-token", getClass().getClassLoader()))
                .isInstanceOf(TapstateException.class)
                .extracting(e -> ((TapstateException) e).code())
                .isEqualTo(ConnectorError.POSITION_UNREADABLE);
    }

    /**
     * An offset the connector will not let anyone write down is a coded refusal too. Reporting no position
     * instead would say "this source has nowhere to resume from", which is a different and untrue claim.
     */
    @Test
    void refusesAnOffsetThatCannotBeWrittenDownWithACode() {
        assertThatThrownBy(() -> ConnectorOffsetCodec.toToken("mysql", new Unserializable()))
                .isInstanceOf(TapstateException.class)
                .extracting(e -> ((TapstateException) e).code())
                .isEqualTo(ConnectorError.POSITION_UNRENDERABLE);
    }
}
