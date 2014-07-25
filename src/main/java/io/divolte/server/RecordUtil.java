package io.divolte.server;

import io.divolte.record.IncomingRequestRecord;
import io.divolte.record.IncomingRequestRecord.Builder;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;

import org.apache.avro.specific.SpecificRecord;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

final class RecordUtil {
    private static final String PARTY_ID_COOKIE;
    private static final String SESSION_ID_COOKIE;
    
    static {
        Config cfg = ConfigFactory.load();
        PARTY_ID_COOKIE = cfg.getString("divolte.tracking.party_cookie");
        SESSION_ID_COOKIE = cfg.getString("divolte.tracking.session_cookie");
    }

    private static String getQueryParamOrMarkIncompleteIfAbsent(HttpServerExchange exchange, String paramName, IncomingRequestRecord.Builder builder) {
        final String result = exchange.getQueryParameters().get(paramName).peekFirst();
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
        
        final boolean firstInSession = exchange.getRequestCookies().containsKey(SESSION_ID_COOKIE);
        final String partyId = exchange.getResponseCookies().get(PARTY_ID_COOKIE).getValue();
        final String sessionId = exchange.getResponseCookies().get(SESSION_ID_COOKIE).getValue();
        final String pageViewId = getQueryParamOrMarkIncompleteIfAbsent(exchange, "p", builder);
        final String referer = getQueryParamOrMarkIncompleteIfAbsent(exchange, "r", builder);
        final String location = getRequestHeaderOrMarkIncompleteIfAbsent(exchange, Headers.REFERER, builder);
        final String remoteHost = exchange.getDestinationAddress().getHostString();
        final String userAgent = getRequestHeaderOrMarkIncompleteIfAbsent(exchange, Headers.USER_AGENT, builder);
        final Integer viewportWidth = parseIntIfParseable(getQueryParamOrMarkIncompleteIfAbsent(exchange, "w", builder));
        final Integer viewportHeight = parseIntIfParseable(getQueryParamOrMarkIncompleteIfAbsent(exchange, "h", builder));
        final Integer screenWidth = parseIntIfParseable(getQueryParamOrMarkIncompleteIfAbsent(exchange, "i", builder));
        final Integer screenHeight = parseIntIfParseable(getQueryParamOrMarkIncompleteIfAbsent(exchange, "j", builder));
        
        return builder
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
