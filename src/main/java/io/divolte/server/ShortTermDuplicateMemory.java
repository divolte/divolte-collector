package io.divolte.server;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.NotThreadSafe;

@ParametersAreNonnullByDefault
@NotThreadSafe
final class ShortTermDuplicateMemory {
    final int[] memory;

    public ShortTermDuplicateMemory(final int size) {
        memory = new int[size];
    }

    public boolean observeAndReturnDuplicity(int observation) {
        final boolean result = memory[(observation & Integer.MAX_VALUE) % memory.length] == observation;
        memory[(observation & Integer.MAX_VALUE) % memory.length] = observation;

        return result;
    }
}
