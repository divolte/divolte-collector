package io.divolte.server;

import java.util.ArrayDeque;
import java.util.Deque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.divolte.record.IncomingRequestRecord;
import io.divolte.record.IncomingRequestRecord.Builder;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

final class RecordUtil {
    private static final Logger logger = LoggerFactory.getLogger(RecordUtil.class);

    private static final Deque<String> emtpyDeque = new ArrayDeque<String>();

    private static final String PARTY_ID_COOKIE;
    private static final String SESSION_ID_COOKIE;

    static {
        Config cfg = ConfigFactory.load();
        PARTY_ID_COOKIE = cfg.getString("divolte.tracking.party_cookie");
        SESSION_ID_COOKIE = cfg.getString("divolte.tracking.session_cookie");
    }

    private static String getQueryParamOrMarkIncompleteIfAbsent(HttpServerExchange exchange, String paramName, IncomingRequestRecord.Builder builder) {
        final String result = exchange.getQueryParameters().getOrDefault(paramName, RecordUtil.emtpyDeque).peekFirst();
        if (result == null) {
            builder.setCompleteRequest(false);
        }
        return result;
    }

    private static String getRequestHeaderOrMarkIncompleteIfAbsent(HttpServerExchange exchange, HttpString headerName, IncomingRequestRecord.Builder builder) {
        final String result = exchange.getRequestHeaders().getFirst(headerName);
        if (result == null) {
            builder.setCompleteRequest(false);
        }
        return result;
    }

    private static Integer parseIntIfParseable(String number) {
        try {
            return Integer.parseInt(number);
        } catch (NumberFormatException nfe) {
            return null;
        }
    }

    public static IncomingRequestRecord recordFromExchange(HttpServerExchange exchange) {
        final Builder builder = IncomingRequestRecord.newBuilder();
        builder.setCompleteRequest(true);

        final long timeStamp = exchange.getRequestStartTime();
        logger.debug("Request cookies: {}", exchange.getRequestCookies());
        final boolean firstInSession = !exchange.getRequestCookies().containsKey(SESSION_ID_COOKIE);
        final String partyId = exchange.getResponseCookies().get(PARTY_ID_COOKIE).getValue();
        final String sessionId = exchange.getResponseCookies().get(SESSION_ID_COOKIE).getValue();
        final String pageViewId = getQueryParamOrMarkIncompleteIfAbsent(exchange, "p", builder);
        final String referer = exchange.getQueryParameters().getOrDefault("r", RecordUtil.emtpyDeque).peekFirst();
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
