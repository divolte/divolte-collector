package io.divolte.server.recordmapping;

import io.divolte.server.config.UserAgentParserConfiguration;
import io.divolte.server.config.ValidatedConfiguration;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import net.sf.uadetector.ReadableUserAgent;
import net.sf.uadetector.UserAgentStringParser;
import net.sf.uadetector.service.UADetectorServiceFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public final class UserAgentParserAndCache {
    private final static Logger logger = LoggerFactory.getLogger(UserAgentParserAndCache.class);

    private final LoadingCache<String,ReadableUserAgent> cache;

    public UserAgentParserAndCache(final ValidatedConfiguration vc) {
        final UserAgentStringParser parser = parserBasedOnTypeConfig(vc.configuration().global.mapper.userAgentParser.type);
        this.cache = sizeBoundCacheFromLoadingFunction(parser::parse, vc.configuration().global.mapper.userAgentParser.cacheSize);
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

    private static UserAgentStringParser parserBasedOnTypeConfig(UserAgentParserConfiguration.ParserType type) {
        switch (type) {
        case CACHING_AND_UPDATING:
            logger.info("Using caching and updating user agent parser.");
            return UADetectorServiceFactory.getCachingAndUpdatingParser();
        case ONLINE_UPDATING:
            logger.info("Using online updating user agent parser.");
            return UADetectorServiceFactory.getOnlineUpdatingParser();
        case NON_UPDATING:
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
