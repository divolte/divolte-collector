package io.divolte.server;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.NotThreadSafe;

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
    private final int[] memory;

    public ShortTermDuplicateMemory(final int size) {
        memory = new int[size];
    }

    public boolean observeAndReturnDuplicity(int observation) {
        final int bucket = (observation & Integer.MAX_VALUE) % memory.length;
        final boolean result = memory[bucket] == observation;
        memory[bucket] = observation;

        return result;
    }
}
