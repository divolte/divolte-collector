package io.divolte.server;

import java.security.SecureRandom;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import com.google.common.base.Splitter;

/**
 * A generator for cookies that encode a timestamp in their value.
 */
@ParametersAreNonnullByDefault
public final class CookieValues {
    private final static char VERSION = '0';
    private final static String VERSION_STRING = "" + VERSION;
    private static final char SEPARATOR_CHAR = ':';

    private final static Splitter splitter = Splitter.on(SEPARATOR_CHAR).limit(4);

    // Some sources mention it's a good idea to avoid contention on SecureRandom instances...
    private final static ThreadLocal<SecureRandom> localRandom = new ThreadLocal<SecureRandom> () {
        protected SecureRandom initialValue() {
            return new SecureRandom();
        }
    };

    private CookieValues() {
        // Prevent external instantiation.
    }

    public static CookieValue generate(final long ts) {
        final SecureRandom random = localRandom.get();

        final byte[] randomBytes = new byte[24];
        random.nextBytes(randomBytes);
        final String id = Base64.getUrlEncoder().encodeToString(randomBytes);

        return new CookieValue(ts, id);
    }

    public static CookieValue generate() {
        return generate(System.currentTimeMillis());
    }

    public static Optional<CookieValue> tryParse(String input) {
        Objects.requireNonNull(input);
        try {
            List<String> parts = splitter.splitToList(input);
            return
                    parts.size() != 3 ||
                    parts.get(0).charAt(0) != VERSION ?
                            Optional.empty() :
                            Optional.of(new CookieValue(
                                    Long.parseLong(parts.get(1), 36),
                                    parts.get(2)));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    public final static class CookieValue {
        @Nonnull
        public final String value;
        public final long timestamp;
        public final char version;

        private CookieValue(final long timestamp, final String id) {
            this.version = VERSION;
            this.timestamp = timestamp;
            this.value = VERSION_STRING + SEPARATOR_CHAR + Long.toString(timestamp, 36) + SEPARATOR_CHAR + id;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }

        @Override
        public boolean equals(final Object other) {
            return this == other ||
                   null != other && getClass() == other.getClass() && value.equals(((CookieValue) other).value);
        }
    }
}
