package io.tapstate.spi.store;

import java.util.List;

/** A read-only exact note observation, decoded only by the connector adapter outside transactions. */
public interface CaptureResourceNotes {
    RequiredNoteScope scope();
    RequiredNoteOrigin origin();
    RequiredNoteValue slot();
    boolean mayCarrySlot();
    List<RequiredNoteScope> carries();
    boolean resumable();
    static String nameHash(String name) {
        if(name==null)return "UNKNOWN";
        return io.tapstate.core.model.canonical.CanonicalHash.ofText(io.tapstate.core.common.JsonWriter.write(List.of("postgres-slot-name",name)));
    }
}
