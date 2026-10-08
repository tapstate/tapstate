package io.tapstate.core.catalog;

import java.util.List;
import java.util.Set;

import io.tapstate.core.model.SourceMode;

/**
 * Merges a connector's structural facts ({@link NormalizedSpec}) with its derived capability bitmap
 * into a {@link ConnectorCatalogEntry}: resolves modes (overlay, then upstream declaration, then
 * derived defaults), sink capability and write semantics (overlay, then derived, then withdrawn for a
 * source-only connector), the refined group, the discovery axis and the push-out flag,
 * then stamps provenance. This is the shared merge — the same rules the runtime server-register path
 * will reuse — so it lives in the core ring and depends on no build tooling.
 */
public final class CatalogEntryAssembler {

    private CatalogEntryAssembler() {
    }

    // Mode sources merge here, highest first. Both callers come through this one method, so a source
    // wired into only one of them cannot happen:
    //
    //     upstream spec.json "modes"   ---.
    //     our own overlay declaration  ---+--> [ overlay > upstream > derived ] --> entry.modes()
    //     capability bitmap (derived)  ---'                                          + modeSource
    //
    //     build-time caller  -> checked-in snapshot row
    //     runtime  caller    -> registered catalog row
    //
    // The overlay is a required argument rather than an optional one on purpose: a caller that has no
    // overlay to give has to say so, instead of quietly assembling rows that skip it.
    public static ConnectorCatalogEntry assemble(NormalizedSpec spec,
                                                 Set<String> derivedCapabilityIds,
                                                 ConnectorOverlay overlay,
                                                 String specPath,
                                                 String specContentHash) {
        Set<DerivedCapability> capabilities = DerivedCapability.fromCapabilityIds(derivedCapabilityIds);
        ModeResolution modeResolution = ModeResolver.resolve(
                capabilities, spec.declaredModes(), overlay.modesFor(spec.id()));
        List<SourceMode> modes = List.copyOf(modeResolution.modes());

        // Same precedence as modes, for the same reason: a connector this repository cannot build
        // derives no capabilities, and a sink read off an absent write_record says "not a target"
        // rather than "could not tell". Absent declaration means derive, so adding the block to one
        // connector leaves every other one exactly as it was.
        SinkCapability declaredSink = overlay.sinkFor(spec.id());
        SinkCapability sink = declaredSink != null ? declaredSink : SinkRules.derive(
                capabilities.contains(DerivedCapability.WRITE_RECORD),
                spec.dmlInsertAlternatives(),
                spec.hasDmlUpdatePolicy());
        // Last, and over a declaration too: a connector this release supports as a source only is not
        // a target, whatever its jar can write. Applied here because both callers come through here,
        // so the bundled row and the row a registration derives cannot disagree about it.
        if (OfficialConnectors.isSourceOnly(spec.id())) {
            sink = new SinkCapability(false, List.of());
        }

        ConnectorGroup group = GroupRules.refine(spec.tagGroup(), modeResolution.modes(), spec.id());
        Discovery discovery = DiscoveryRules.fromGroup(group);
        // A message-queue connector is the one kind that can be a push (event-stream) target.
        boolean pushOut = group == ConnectorGroup.MQ;

        // No revision is stamped here. It is a property of a whole catalog rather than of one entry,
        // so it lives in the index head and is filled in when the catalog is read; the runtime caller
        // assembles a single row with no head at all, and null is the honest answer for it.
        Provenance provenance = new Provenance(null, null, specPath, specContentHash,
                null, null, modeResolution.bySource());

        return new ConnectorCatalogEntry(spec.id(), spec.name(), spec.displayName(), spec.icon(),
                group, modes, discovery, sink, pushOut, spec.config(), provenance);
    }
}
