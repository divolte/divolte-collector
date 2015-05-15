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

package io.divolte.server.recordmapping;

import com.fasterxml.jackson.core.TreeNode;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;

import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public final class JsonPathSupport {
    private static final Configuration JSON_PATH_CONFIGURATION =
            Configuration.builder()
                         .options(Option.SUPPRESS_EXCEPTIONS)
                         .jsonProvider(new JacksonJsonNodeJsonProvider())
                         .build();

    public static DocumentContext asDocumentContext(final TreeNode jsonDocument) {
        return JsonPath.parse(jsonDocument, JSON_PATH_CONFIGURATION);
    }

    private JsonPathSupport() {
        // Prevent external instantiation.
    }
}
