package io.divolte.server;

import java.io.IOException;
import java.util.function.Function;

import javax.annotation.ParametersAreNonnullByDefault;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;

@ParametersAreNonnullByDefault
public final class ConcurrentUtils {
    private static final Logger logger = LoggerFactory.getLogger(ConcurrentUtils.class);

    private ConcurrentUtils() {
        throw new UnsupportedOperationException("Singleton; do not instantiate.");
    }

    public static <K,V> Cache<K, V> buildSizeBoundCacheFromLoadingFunction(Function<K, V> loader, int size) {
        return CacheBuilder
                .newBuilder()
                .maximumSize(size)
                .initialCapacity(size)
                .build(new CacheLoader<K, V>() {
                    @Override
                    public V load(K key) throws Exception {
                        return loader.apply(key);
                    }
                });
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
