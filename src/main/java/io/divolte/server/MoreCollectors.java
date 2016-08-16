package io.divolte.server;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.stream.Collector;

public final class MoreCollectors {
    private MoreCollectors() {
        // Prevent external instantiation.
    }

    public static <T> Collector<T, ImmutableList.Builder<T>, ImmutableList<T>> toImmutableList() {
        return Collector.of(ImmutableList.Builder<T>::new,
                            ImmutableList.Builder<T>::add,
                            (l, r) -> l.addAll(r.build()),
                            ImmutableList.Builder::build);
    }

    public static <T> Collector<T, ImmutableSet.Builder<T>, ImmutableSet<T>> toImmutableSet() {
        return Collector.of(ImmutableSet.Builder<T>::new,
                ImmutableSet.Builder<T>::add,
                (l, r) -> l.addAll(r.build()),
                ImmutableSet.Builder::build);
    }

    public static <K,V> Collector<Map.Entry<K,V>, ImmutableMap.Builder<K,V>, ImmutableMap<K,V>> toImmutableMap() {
        return Collector.of(ImmutableMap.Builder<K,V>::new,
                            ImmutableMap.Builder<K,V>::put,
                            (l, r) -> l.putAll(r.build()),
                            ImmutableMap.Builder::build);
    }

    public static <K,V> Collector<Map.Entry<K,V>, ImmutableMultimap.Builder<K,V>, ImmutableMultimap<K,V>> toImmutableMultimap() {
        return Collector.of(ImmutableMultimap.Builder<K,V>::new,
                            ImmutableMultimap.Builder<K,V>::put,
                            (l, r) -> l.putAll(r.build()),
                            ImmutableMultimap.Builder::build);
    }
}
