/**
 * Source capture port: the read-side extraction contract. A pure interface over the standard event
 * envelope — bounded snapshot reads, an unbounded CDC stream, a connection test and schema
 * discovery. Which phases run is driven by the pipeline read mode as a {@link
 * io.tapstate.spi.capture.CapturePlan}; the {@link io.tapstate.spi.capture.CapturePhase} of an event
 * follows its op. Position is on the contract at both ends: a cdc stream is started at a {@link
 * io.tapstate.spi.capture.CaptureStart}, and a snapshot batch reports the opaque {@link
 * io.tapstate.spi.capture.SourcePosition} seam its change tail resumes from. Rule R2: this module
 * depends only on the core ring.
 */
package io.tapstate.spi.capture;
