package io.tapstate.control.core;

import io.tapstate.spi.store.CloudSessionContext;

/** Back-channel C1 redemption result; the JWT remains transient and is never persisted. */
public record CloudCodeExchangeResult(String jwt, CloudSessionContext context) { }
