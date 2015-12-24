package io.divolte.server.processing;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public final class Item<E> {
    public final int sourceId;
    public final int affinityHash;
    public final E payload;

    private static final HashFunction HASHER = Hashing.murmur3_32(42);

    private Item(final int sourceId, final int affinityHash, final E payload) {
        this.sourceId = sourceId;
        this.affinityHash = affinityHash;
        this.payload = Objects.requireNonNull(payload);
    }

    public static <E> Item<E> of(final int sourceId, final String key, final E payload) {
        return new Item<>(
                sourceId,
                // making sure the hash result is non-negative by masking with max int
                HASHER.hashString(key, StandardCharsets.UTF_8).asInt() & Integer.MAX_VALUE,
                payload);
    }

    public static <E> Item<E> withCopiedAffinity(final int sourceId, final Item<?> affinitySource, final E payload) {
        return new Item<>(sourceId, affinitySource.affinityHash, payload);
    }
}
