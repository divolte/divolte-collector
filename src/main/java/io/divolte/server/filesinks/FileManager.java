/*
 * Copyright 2018 GoDataDriven B.V.
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

package io.divolte.server.filesinks;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import io.divolte.server.AvroRecordBuffer;

/**
 * Abstraction for file based sinks that allows to cater for different types of (remote) file
 * systems.
 */
@NotThreadSafe
public interface FileManager {
    /**
     * Create a new file in inflight status. Throwing IOException will cause the client to go into a
     * retry cycle, by discarding the current file(s) and attempting to open a new one every so
     * often.
     *
     * @param name Name of the file to be created, excluding directory.
     * @return A {@link DivolteFile} instance.
     * @throws IOException In case of errors or unavailability of the remote file system.
     */
    DivolteFile createFile(final String name) throws IOException;

    interface DivolteFile {
        /**
         * Append a single record to the file. May or may not buffer depending on implementation.
         * Throwing IOException will cause the client to go into a retry cycle, by discarding the
         * current file(s) and attempting to open a new one every so often.
         *
         * @param buffer The record to append to the open file.
         * @throws IOException In case of errors or unavailability of the remote file system.
         */
        void append(AvroRecordBuffer buffer) throws IOException;

        /**
         * Close and publish the file. For most implementations this will imply a move / rename to a
         * different directory. Calling append(...) after this should throw IllegalStateException.
         *
         * @throws IOException In case of errors or unavailability of the remote file system.
         */
        void closeAndPublish() throws IOException;

        /**
         * Sync the underlying file.
         *
         * @throws IOException In case of errors or unavailability of the remote file system.
         */
        void sync() throws IOException;

        /**
         * Remove this file. Should close and attempt to delete it. Calling other methods
         * afterwards should throw IllegalStateException.
         *
         * @throws IOException In case of errors or unavailability of the remote file system.
         */
        void discard() throws IOException;
    }

    interface FileManagerFactory {
        /**
         * Attempt to use the loaded configuration to make a file system connection. Implementations
         * should verify that the configuration holds all necessary information to successfully
         * connect to the file system and can additionally verify matters like appropriate
         * credentials and existence of configured directories. This method SHOULD NOT fail in the
         * case that a remote file system is temporarily unavailable, as such situations are
         * expected to recover over time.
         */
        void verifyFileSystemConfiguration();

        /**
         * Create an instance of FileManager that connects to the remote file system. In the event
         * of unavailability of the remote file system, this method should still return a
         * FileManager as recovering the underlying connection is expected to happen as part of a
         * retry cycle.
         *
         * @return A {@link FileManager} instance.
         */
        FileManager create();
    }
}
