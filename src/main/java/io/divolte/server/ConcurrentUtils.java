package io.divolte.server;

import java.io.IOException;

import javax.annotation.ParametersAreNonnullByDefault;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ParametersAreNonnullByDefault
public final class ConcurrentUtils {
    private static final Logger logger = LoggerFactory.getLogger(ConcurrentUtils.class);

    private ConcurrentUtils() {
        throw new UnsupportedOperationException("Singleton; do not instantiate.");
    }

    @FunctionalInterface
    public interface IOExceptionThrower {
        public abstract void run() throws IOException;
    }

    public static boolean throwsIoException(final IOExceptionThrower r) {
        try {
            r.run();
            return false;
        } catch (final IOException ioe) {
            return true;
        }
    }
}
