package io.divolte.server;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sf.uadetector.ReadableUserAgent;
import net.sf.uadetector.UserAgentStringParser;
import net.sf.uadetector.service.UADetectorServiceFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.typesafe.config.Config;

final class UserAgentParserAndCache {
    private final static Logger logger = LoggerFactory.getLogger(UserAgentParserAndCache.class);

    private final LoadingCache<String,ReadableUserAgent> cache;

    public UserAgentParserAndCache(final Config config) {
        final UserAgentStringParser parser = parserBasedOnTypeConfig(config.getString("divolte.tracking.ua_parser.type"));
        this.cache = sizeBoundCacheFromLoadingFunction(parser::parse, config.getInt("divolte.tracking.ua_parser.cache_size"));
        logger.info("User agent parser data version: {}", parser.getDataVersion());
    }

    public Optional<ReadableUserAgent> tryParse(final String userAgentString) {
        try {
            return Optional.of(cache.get(userAgentString));
        } catch (final ExecutionException e) {
            logger.debug("Failed to parse user agent string for: " + userAgentString);
            return Optional.empty();
        }
    }

    private static UserAgentStringParser parserBasedOnTypeConfig(String type) {
        switch (type) {
        case "caching_and_updating":
            logger.info("Using caching and updating user agent parser.");
            return UADetectorServiceFactory.getCachingAndUpdatingParser();
        case "online_updating":
            logger.info("Using online updating user agent parser.");
            return UADetectorServiceFactory.getOnlineUpdatingParser();
        case "non_updating":
            logger.info("Using non-updating (resource module based) user agent parser.");
            return UADetectorServiceFactory.getResourceModuleParser();
        default:
            throw new RuntimeException("Invalid user agent parser type. Valid values are: caching_and_updating, online_updating, non_updating.");
        }
    }

    private static <K,V> LoadingCache<K, V> sizeBoundCacheFromLoadingFunction(Function<K, V> loader, int size) {
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
}
