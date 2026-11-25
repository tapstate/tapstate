package io.tapstate.spi.alerting;

import java.net.URI;
import java.util.Arrays;

/** Caller-owned private input or resolved material; never return this value from configuration queries. */
public final class ReceiverSecretMaterial implements AutoCloseable {
    private byte[] authentication;
    private byte[] privateCa;
    private URI webhookEndpoint;
    private boolean closed;

    public ReceiverSecretMaterial(byte[] authentication, byte[] privateCa, URI webhookEndpoint) {
        this.authentication = authentication == null ? null : authentication.clone();
        this.privateCa = privateCa == null ? null : privateCa.clone();
        this.webhookEndpoint = webhookEndpoint;
    }
    public byte[] authentication() { requireOpen(); return authentication == null ? null : authentication.clone(); }
    public byte[] privateCa() { requireOpen(); return privateCa == null ? null : privateCa.clone(); }
    public URI webhookEndpoint() { requireOpen(); return webhookEndpoint; }
    @Override public String toString() { return "ReceiverSecretMaterial[redacted]"; }
    @Override public void close() {
        if (authentication != null) { Arrays.fill(authentication, (byte) 0); authentication = null; }
        if (privateCa != null) { Arrays.fill(privateCa, (byte) 0); privateCa = null; }
        webhookEndpoint = null; closed = true;
    }
    private void requireOpen() { if (closed) { throw new IllegalStateException("receiver secret material is closed"); } }
}
