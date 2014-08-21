package io.divolte.server;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

/**
 * A generator for cookies that encode a timestamp in their value.
 */
public final class CookieValues {
    // Some sources mention it's a good idea to avoid contention on SecureRandom instances...
    private final static ThreadLocal<SecureRandom> localRandom = new ThreadLocal<SecureRandom> () {
        protected SecureRandom initialValue() {
            return new SecureRandom();
        }
    };

    private CookieValues() {
    }

    public static CookieValue generate(final long ts) {
        final byte[] randomBytes = new byte[8];
        final byte[] valueBytes = new byte[16];

        final ByteBuffer buf = ByteBuffer.wrap(valueBytes);
        buf.putLong(ts);

        localRandom.get().nextBytes(randomBytes);
        buf.put(randomBytes);

        return new CookieValue(valueBytes, ts);
    }

    public static CookieValue generate() {
        return generate(System.currentTimeMillis());
    }

    public static Optional<CookieValue> tryParse(String input) {
        if (input.length() != 32) {
            return Optional.empty();
        }
        try {
            byte[] valueBytes = Hex.decodeHex(input.toCharArray());
            long ts = ByteBuffer.wrap(valueBytes).getLong();
            return Optional.of(new CookieValue(valueBytes, ts));
        } catch (DecoderException e) {
            return Optional.empty();
        }
    }

    public final static class CookieValue {
        @Nonnull
        public final String value;
        public final long timestamp;

        private CookieValue(@Nonnull final byte[] valueBytes, final long timestamp) {
            this.value = Hex.encodeHexString(Objects.requireNonNull(valueBytes));
            this.timestamp = timestamp;
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
