/*
 * Copyright 2017 GoDataDriven B.V.
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

package io.divolte.server;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.NotThreadSafe;
import java.nio.charset.StandardCharsets;

/**
 * Probabilistic detection of duplicate events in a stream with fixed memory overhead.
 * <p>
 * This class is used to detect duplicates in an event stream. An event
 * is identified by an array of strings that represent characteristics of the
 * event. (The same values indicate the same logical event.) Invoking
 * {@link #isProbableDuplicate(String...)} not only returns whether the event
 * is probably a duplicate or not, but also updates the internal state such
 * that the event has been 'seen'. (A second immediate invocation with the same
 * parameter will always return <code>true</code>.)
 * <p>
 * Because this class is probabilistic it can return both false positives
 * (a unique event is considered to be a duplicate) and false negatives (an event
 * previously seen is not flagged as a duplicate).
 * <p>
 * This class maintains a number of slots as internal state, each of which
 * can store an event signature. The number of slots is specified as a
 * constructor parameter. Duplicate detection works by hashing the event
 * properties to a specific slot, and checking whether the signature stored
 * in that slot matches the event. The signature is independent of the hash
 * used to choose a slot.
 * <p>
 * Duplicate events are missed (false negatives) when multiple different events
 * hash to the same slot. The signature of the each such event will replace the
 * signature of the previous such event. When a prior event is repeated its
 * signature is no longer present at the slot location and it is not recognized
 * as a duplicate. For a fixed number of events the proportion of false negatives
 * is:
 * <ul>
 *   <li>Inversely proportional to the number of slots that are configured.
 *     More slots means fewer false negatives.</li>
 *   <li>Proportional to the interval between duplicate events. The further
 *     apart duplicate events occur, the less likely they are to be recognized.</li>
 * </ul>
 * <p>
 * Unique events are incorrectly categorized as duplicates (false positives) when
 * multiple different events hash to the same slot <em>and</em> have the same
 * signature, without an intervening event hashing to the same slot. For a fixed
 * number of events the proportion of false positives is:
 * <ul>
 *   <li>Inversely proportional to the number of slots that are configured.
 *     More slots means fewer false positives.</li>
 *   <li>Inversely proportional to the probability of multiple events in the same
 *     slot having the same signature. Signatures are 64-bits in length, meaning
 *     that the probability of two events having the same signature is 1/(2^32).</li>
 * </ul>
 */
/* TODO: These calculations need revising.
 *
 * The probability of a false positive is equal to the probability of a hash
 * collision with any observation currently in the filter memory. So, if the
 * memory size is 10 million, the chance of a false positive is equal to the
 * chance of the hash code of a new event colliding with one of 10 million
 * arbitrary other hash codes (probability of a hash collision times the
 * probability of the colliding hash code being drawn when drawing 10 million
 * hash codes from the entire space of hash codes).
 *
 * The probability of a false negative is a function of the memory size, the
 * amount of time that passes between two incarnations of the duplicate event
 * and the number of events that occur during that time. A false negative will
 * occur when to equal observations are interleaved with a different observation
 * that maps to the same position in the memory. Given a uniform distribution of
 * event hash codes and a resulting uniform distribution of position
 * assignments, the probability of a false negative for duplicates that occur a
 * given amount of time apart, can be calculated as:
 * time between duplicates / (memory size / throughput rate) / 2.
 * For example:
 * 600 seconds / (10 million / 1 kilohertz) / 2 = 600 seconds / 10000 seconds / 2 = 0.03.
 * This means that at a rate of 1000 events per second, there is a 3% chance of
 * a position in the memory being overwritten during the last 10 minutes.
 *
 * If 99% of duplicate events occur within 2 minutes from each other, we expect
 * to see 120 seconds / (10 million / 1 kilohertz) / 2 = 0.6% of the positions
 * to be overwritten in the last 2 minutes. At a true positive rate of 0.5% for
 * those 99% of events, we expect to see 0.99 * 0.005 * 0.006 = 0.00297% false
 * positives at a rate of 1000 events / second.
 */
@ParametersAreNonnullByDefault
@NotThreadSafe
final class ShortTermDuplicateMemory {
    private static final HashFunction HASHING_FUNCTION = Hashing.murmur3_128();

    private final long[] memory;

    /**
     * Construct an instance with a specific number of slots.
     * <p>
     * More slots lowers the probability of events being categorized
     * incorrectly, at the expense of more memory.
     *
     * @param slotCount the number of slots to use for detecting duplicate events.
     */
    public ShortTermDuplicateMemory(final int slotCount) {
        memory = new long[slotCount];
    }

    /**
     * Query whether an event has been seen before or not, based on event properties.
     * @param eventProperties   An array of values that are specific to the event.
     * @return <code>true</code> if we have probably seen this event previously, or
     *  false otherwise.
     */
    public boolean isProbableDuplicate(final String... eventProperties) {
        final Hasher hasher = HASHING_FUNCTION.newHasher();
        for (final String eventProperty : eventProperties) {
            hasher.putString(eventProperty, StandardCharsets.UTF_8);
        }
        return isProbablyDuplicate(hasher.hash());
    }

    private boolean isProbablyDuplicate(final HashCode eventDigest) {
        // Our hashing algorithm produces 8 bytes:
        //  0: slot[0]
        //  1: slot[1]
        //  2: slot[2]
        //  3: slot[3]
        //  4:
        //  5:
        //  6:
        //  7:
        //  8: signature[0]
        //  9:  ..
        // 10:  ..
        // 11:  ..
        // 12:  ..
        // 13:  ..
        // 14:  ..
        // 15: signature[7]
        final byte[] hashBytes = eventDigest.asBytes();

        // We use the low int for the slot.
        final int slotSelector = Ints.fromBytes(hashBytes[0],
                                                hashBytes[1],
                                                hashBytes[2],
                                                hashBytes[3]);
        // We use the high long for the signature.
        final long signature = Longs.fromBytes(hashBytes[8],
                                               hashBytes[9],
                                               hashBytes[10],
                                               hashBytes[11],
                                               hashBytes[12],
                                               hashBytes[13],
                                               hashBytes[14],
                                               hashBytes[15]);

        final int slot = (slotSelector & Integer.MAX_VALUE) % memory.length;
        final boolean result = memory[slot] == signature;
        memory[slot] = signature;
        return result;
    }
}
