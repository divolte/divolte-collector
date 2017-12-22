/*
 * Copyright 2017 GoDataDriven B.V.
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

package io.divolte.server.recordmapping;

import static java.net.URLDecoder.*;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@ParametersAreNonnullByDefault
@ThreadSafe
public final class QueryStringParser {
    public static Map<String,List<String>> parseQueryString(@Nullable final String string) {
        if (string == null) {
            return Collections.emptyMap();
        }

        Map<String,List<String>> result = Maps.newHashMapWithExpectedSize(10);
        try {
            int stringStart = 0;
            String attrName = null;
            for (int i = 0; i < string.length(); ++i) {
                char c = string.charAt(i);
                if (c == '=' && attrName == null) {
                    attrName = string.substring(stringStart, i);
                    stringStart = i + 1;
                } else if (c == '&') {
                    if (attrName != null) {
                        final String key = decode(attrName, StandardCharsets.UTF_8.name());
                        final String value = decode(string.substring(stringStart, i), StandardCharsets.UTF_8.name());
                        mergeIntoMap(result, key, value);
                    } else {
                        final String key = decode(string.substring(stringStart, i), StandardCharsets.UTF_8.name());
                        final String value = "";
                        mergeIntoMap(result, key, value);
                    }
                    stringStart = i + 1;
                    attrName = null;
                }
            }
            if (attrName != null) {
                final String key = decode(attrName, StandardCharsets.UTF_8.name());
                final String value = decode(string.substring(stringStart, string.length()), StandardCharsets.UTF_8.name());
                mergeIntoMap(result, key, value);
            } else if (string.length() != stringStart) {
                final String key = decode(string.substring(stringStart, string.length()), StandardCharsets.UTF_8.name());
                final String value = "";
                mergeIntoMap(result, key, value);
            }
        } catch (UnsupportedEncodingException e) {
            return Collections.emptyMap();
        }

        return result;
    }

    private static void mergeIntoMap(final Map<String,List<String>> map, final String key, final String value) {
        map.compute(key, (ignored,existing) -> {
            if (existing != null) {
                existing.add(value);
                return existing;
            }
            return Lists.newArrayList(value);
         });
    }
}
