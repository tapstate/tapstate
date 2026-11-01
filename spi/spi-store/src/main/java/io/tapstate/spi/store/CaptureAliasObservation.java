package io.tapstate.spi.store;

/** Connector-decoded resource identity over an original store-issued note snapshot. */
public record CaptureAliasObservation(CaptureResourceNotes notes,String resourceName) {
    public CaptureAliasObservation {
        if(notes==null || resourceName==null || resourceName.isBlank())throw new IllegalArgumentException("invalid capture alias observation");
    }
}
