/*
 * Copyright 2018 GoDataDriven B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
