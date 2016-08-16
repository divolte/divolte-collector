/*
 * Copyright 2014 GoDataDriven B.V.
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

package io.divolte.server.hdfs;

import io.divolte.server.AvroRecordBuffer;

import javax.annotation.ParametersAreNonnullByDefault;

/*
 * Used by the HdfsFlusher to actually flush events to HDFS. Different implementation
 * can use different strategies for assigning events to files and opening and closing files.
 *
 * Clients are expected to use append(...) whenever events are available and periodically call
 * heartbeat() when no events are available. When either append(...) or heartbeat return FAILURE,
 * clients MUST NOT call append(...) any more, until a call to heartbeat() has returned SUCCESS.
 */
@ParametersAreNonnullByDefault
interface FileCreateAndSyncStrategy {
    HdfsOperationResult setup();
    HdfsOperationResult heartbeat();
    HdfsOperationResult append(final AvroRecordBuffer record);
    void cleanup();

    enum HdfsOperationResult {
        SUCCESS,
        FAILURE
    }
}
