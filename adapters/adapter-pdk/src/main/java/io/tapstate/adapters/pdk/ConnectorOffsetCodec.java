package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.util.Base64;
import java.util.Map;

/**
 * Renders a connector's own stream offset as the opaque token a source position carries, and reads a
 * token back into the object the connector issued.
 *
 * <p>A connector states where a stream is as an object of its own making — a binlog coordinate, a log
 * sequence number, a resume token — and will only accept one of those back. A recorded position, though,
 * has to outlive the process that read it: it is written to the coordination store as text and handed
 * back to a connector that may be running a later build, in a later run. This is the only place the two
 * shapes meet.
 *
 * <p>The token is the offset's serialized bytes in base64. Two properties matter more than the encoding:
 * it needs no knowledge of any particular connector, and it never invents a position — a connector that
 * will not let its offset be written down, or a token this connector can no longer read, is a coded
 * refusal naming the connector. Falling back to reading from the present would be the silent form of the
 * same failure, and would drop every change made since the position was recorded.
 *
 * <p>The class is resolved through the connector's own loader. An offset type lives in the connector jar
 * and is invisible to the host, so resolving against the host loader would fail for every real connector.
 *
 * <p>A token is readable only by a build of the connector whose offset class still matches the one that
 * wrote it; a class that changes shape between builds makes the recorded position unreadable, and that is
 * reported rather than worked around. Tolerating the mismatch would mean handing a connector a position
 * assembled from fields that no longer mean what they did.
 */
final class ConnectorOffsetCodec {

    private ConnectorOffsetCodec() {
    }

    /**
     * The token for {@code offset}, an object the connector issued. Refuses with a code when the offset
     * cannot be written down at all — reporting no position instead would claim this source has nowhere
     * to resume from, which is a different and untrue statement.
     */
    static String toToken(String connectorId, Object offset) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
                out.writeObject(offset);
            }
            return Base64.getEncoder().encodeToString(bytes.toByteArray());
        } catch (IOException | RuntimeException e) {
            throw new TapstateException(ConnectorError.POSITION_UNRENDERABLE,
                    Map.of("connector", connectorId, "detail", detail(e)), e);
        }
    }

    /**
     * The offset object {@code token} was made from, with its classes resolved through {@code loader} —
     * the connector's own. Refuses with a code when the token cannot be read back.
     */
    static Object fromToken(String connectorId, String token, ClassLoader loader) {
        try {
            byte[] bytes = Base64.getDecoder().decode(token);
            try (ObjectInputStream in = new ConnectorLoaderInputStream(new ByteArrayInputStream(bytes), loader)) {
                return in.readObject();
            }
        } catch (IOException | ClassNotFoundException | RuntimeException e) {
            throw new TapstateException(ConnectorError.POSITION_UNREADABLE,
                    Map.of("connector", connectorId, "detail", detail(e)), e);
        }
    }

    /**
     * An object stream that resolves every class through the connector's loader rather than through
     * whichever loader happens to be deepest on the calling stack — which for a read driven from the host
     * is the host's, and cannot see a connector's own types at all.
     */
    private static final class ConnectorLoaderInputStream extends ObjectInputStream {

        private final ClassLoader loader;

        ConnectorLoaderInputStream(ByteArrayInputStream in, ClassLoader loader) throws IOException {
            super(in);
            this.loader = loader;
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass descriptor) throws ClassNotFoundException {
            // No fallback to the default resolution. It resolves against the deepest loader on the calling
            // stack, which for a read driven from the host is the host's -- so a connector type the
            // connector's own loader cannot supply would still be found whenever the host happened to
            // carry one of the same name, and the position would be rebuilt from a different class than
            // the one that wrote it. Array and primitive descriptors are spelled in the form this already
            // takes, so nothing needs the fallback anyway.
            return Class.forName(descriptor.getName(), false, loader);
        }
    }

    private static String detail(Throwable t) {
        if (t instanceof InvalidClassException) {
            return "the connector's position type has changed shape since the position was recorded: "
                    + t.getMessage();
        }
        return t.getMessage() != null ? t.getMessage() : t.getClass().getSimpleName();
    }
}
