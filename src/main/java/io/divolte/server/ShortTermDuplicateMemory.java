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
 * This class is used to detect duplicates in the event stream. Its single
 * method is to be called with the hash code of an event or a subset of the
 * uniquely identifying fields of an event as argument. This method both returns
 * whether the event is thought to be a duplicate and updates the internal state
 * of the duplicates memory. Hence, calling this method twice with the same
 * argument will always return true for the second invocation. This class can
 * under some circumstances return false negatives (the observation is though to
 * be unique, but was in fact duplicate) or false positives (the observation is
 * thought to be duplicate, but was in fact unique), although the latter case is
 * far less likely to occur than the former.
 *
 * Duplicate detection works by assigning an incoming observation to a position
 * in a finite amount of memory using the observation modulo the memory size.
 * This means that different observations will map to the same position in the
 * memory. An observation is kept in memory until it is overwritten by another
 * observation that maps to the same position. Before writing an observation
 * into the memory, the current value for the observation's position is compared
 * to the observation itself. If these values are equal, the observation is
 * believed to be duplicate, if not the observation is believed to be unique.
 *
 * Because the event stream is infinite, at some point, all positions in memory
 * will be occupied by some observation and each subsequent observation will
 * always overwrite an existing observation. At this point it is possible that
 * the observation of the first occurrence of a duplicate event is overwritten
 * by another observation that maps to the same position before the second
 * occurrence of the duplicate event is observed, leading to a false negative.
 * It is also possible that two events have colliding hash codes, making them
 * identical as far as this filter is concerned, thus resulting in a false
 * positive.
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

    public ShortTermDuplicateMemory(final int size) {
        memory = new long[size];
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
        //  0: bucket[0]
        //  1: bucket[1]
        //  2: bucket[2]
        //  3: bucket[3]
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

        // We use the low int for the bucket.
        final int bucketSelector = Ints.fromBytes(hashBytes[0],
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

        final int bucket = (bucketSelector & Integer.MAX_VALUE) % memory.length;
        final boolean result = memory[bucket] == signature;
        memory[bucket] = signature;
        return result;
    }
}
