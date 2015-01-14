/*
 * Copyright 2015 GoDataDriven B.V.
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

/**
 * Registry of query parameters in use, to prevent conflicts.
 *
 * This interface is simply used to accumulate the query parameter names that we use
 * in various places. Although they could be more narrowly scoped, they are gathered
 * here to forestall accidental conflicts.
 */
interface QueryParameterNames {
    // These are only supplied when client-side cookie handling is in use.
    static final String PARTY_ID_QUERY_PARAM = "p";
    static final String NEW_PARTY_ID_QUERY_PARAM = "n";
    static final String SESSION_ID_QUERY_PARAM = "s";
    static final String FIRST_IN_SESSION_QUERY_PARAM = "f";
    static final String EVENT_ID_QUERY_PARAM = "e";
    static final String CLIENT_TIMESTAMP_QUERY_PARAM = "c"; // chronos
    static final String CHECKSUM_QUERY_PARAM = "x";

    // This is always supplied for client-side cookie handling, and optional
    // for server-side handling.
    static final String PAGE_VIEW_ID_QUERY_PARAM = "v";

    // These are query parameters that supplied irrespective of whether client-
    // or server-side cookie handling is in use.
    static final String EVENT_TYPE_QUERY_PARAM = "t";
    static final String LOCATION_QUERY_PARAM = "l";
    static final String REFERER_QUERY_PARAM = "r";
    static final String VIEWPORT_PIXEL_WIDTH_QUERY_PARAM = "w";
    static final String VIEWPORT_PIXEL_HEIGHT_QUERY_PARAM = "h";
    static final String SCREEN_PIXEL_WIDTH_QUERY_PARAM = "i";
    static final String SCREEN_PIXEL_HEIGHT_QUERY_PARAM = "j";
    static final String DEVICE_PIXEL_RATIO_QUERY_PARAM = "k";
}
