package io.tapstate.runtime.engine;

import com.hazelcast.jet.core.Processor;

/**
 * A transparent local envelope whose processing callbacks only check bounded local authority and
 * delegate. Its one private final Processor owns business work and timing. It must not expose that
 * delegate, buffer business rows, perform I/O or create another timed unit. Architecture checks verify
 * the delegation shape; behavior checks preserve authority, lifecycle and delegate timing.
 */
public interface TransparentProcessor extends Processor {}
