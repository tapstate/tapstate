package io.tapstate.control.core;

/** The history resolution a caller may request. */
public enum HistoryResolution {
    AUTO,
    RAW,
    PT5M,
    PT30M,
    PT1H,
    PT3H,
    PT6H
}
