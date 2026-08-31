package io.tapstate.cli;

/** Immutable terminal dimensions captured by the UI event loop. */
record TuiViewport(int width, int height) {

    static final TuiViewport DEFAULT = new TuiViewport(100, 24);

    TuiViewport {
        width = Math.max(1, width);
        height = Math.max(1, height);
    }
}
