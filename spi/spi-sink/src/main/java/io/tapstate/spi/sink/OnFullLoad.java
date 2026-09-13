package io.tapstate.spi.sink;

/** What to do with an existing target before a fresh full load. */
public enum OnFullLoad {
    CLEAR, APPEND, FAIL
}
