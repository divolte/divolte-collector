package io.divolte.server;

import io.divolte.record.IncomingRequestRecord;
import io.divolte.record.IncomingRequestRecord.Builder;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import java.util.Deque;
import java.util.Optional;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

@ParametersAreNonnullByDefault
final class RecordUtil {
    private static final String PARTY_ID_COOKIE;
    private static final String SESSION_ID_COOKIE;
    private static final String PAGE_VIEW_ID_COOKIE;

    static {
        final Config cfg = ConfigFactory.load();
        PARTY_ID_COOKIE = cfg.getString("divolte.tracking.party_cookie");
        SESSION_ID_COOKIE = cfg.getString("divolte.tracking.session_cookie");
        PAGE_VIEW_ID_COOKIE = cfg.getString("divolte.tracking.page_view_cookie");
    }

    private static void markIncompleteIfAbsent(final IncomingRequestRecord.Builder builder,
                                               final Optional<?> optionalValue) {
        if (!optionalValue.isPresent()) {
            builder.setCompleteRequest(false);
        }
    }

    @Nullable
    private static String getQueryParamOrMarkIncompleteIfAbsent(final HttpServerExchange exchange,
                                                                final String paramName,
                                                                final IncomingRequestRecord.Builder builder) {
        final Optional<String> value =
            Optional.ofNullable(exchange.getQueryParameters().get(paramName))
                    .map(Deque::getFirst);
        markIncompleteIfAbsent(builder, value);
        return value.orElse(null);
    }

    @Nullable
    private static String getRequestHeaderOrMarkIncompleteIfAbsent(final HttpServerExchange exchange,
                                                                   final HttpString headerName,
                                                                   final IncomingRequestRecord.Builder builder) {
        final Optional<String> value = Optional.ofNullable(exchange.getRequestHeaders().getFirst(headerName));
        markIncompleteIfAbsent(builder, value);
        return value.orElse(null);
    }

    private static Integer parseIntIfParseable(@Nullable String number) {
        try {
            return Integer.valueOf(number);
        } catch (NumberFormatException nfe) {
            return null;
        }
    }

    public static IncomingRequestRecord recordFromExchange(HttpServerExchange exchange) {
        final Builder builder = IncomingRequestRecord.newBuilder();
        builder.setCompleteRequest(true);

        final long timeStamp = exchange.getRequestStartTime();
        final boolean firstInSession = !exchange.getRequestCookies().containsKey(SESSION_ID_COOKIE);
        final String partyId = exchange.getResponseCookies().get(PARTY_ID_COOKIE).getValue();
        final String sessionId = exchange.getResponseCookies().get(SESSION_ID_COOKIE).getValue();
        final String pageViewId = exchange.getResponseCookies().get(PAGE_VIEW_ID_COOKIE).getValue();
        final String referer = Optional.ofNullable(exchange.getQueryParameters().get("r")).map(Deque::getFirst).orElse(null);
        final String location = getQueryParamOrMarkIncompleteIfAbsent(exchange, "l", builder);
        final String remoteHost = exchange.getDestinationAddress().getHostString();
        final String userAgent = getRequestHeaderOrMarkIncompleteIfAbsent(exchange, Headers.USER_AGENT, builder);
        final Integer viewportWidth = parseIntIfParseable(getQueryParamOrMarkIncompleteIfAbsent(exchange, "w", builder));
        final Integer viewportHeight = parseIntIfParseable(getQueryParamOrMarkIncompleteIfAbsent(exchange, "h", builder));
        final Integer screenWidth = parseIntIfParseable(getQueryParamOrMarkIncompleteIfAbsent(exchange, "i", builder));
        final Integer screenHeight = parseIntIfParseable(getQueryParamOrMarkIncompleteIfAbsent(exchange, "j", builder));

        return builder
        .setTimestamp(timeStamp)
        .setFirstInSession(firstInSession)
        .setPartyId(partyId)
        .setSessionId(sessionId)
        .setPageViewId(pageViewId)
        .setLocation(location)
        .setReferer(referer)
        .setRemoteHost(remoteHost)
        .setUserAgent(userAgent)
        .setViewportWidth(viewportWidth)
        .setViewportHeight(viewportHeight)
        .setScreenWidth(screenWidth)
        .setScreenHeight(screenHeight)
        .build();
    }
}
