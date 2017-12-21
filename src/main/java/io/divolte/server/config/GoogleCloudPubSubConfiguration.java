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

package io.divolte.server.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.cloud.ServiceOptions;
import com.google.common.base.MoreObjects;
import io.divolte.server.config.constraint.GoogleCloudProjectIdRequiredForPubSub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Optional;

@ParametersAreNonnullByDefault
@GoogleCloudProjectIdRequiredForPubSub
public class GoogleCloudPubSubConfiguration extends SinkTypeConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(GoogleCloudPubSubConfiguration.class);

    public final Optional<String> projectId;

    @JsonCreator
    GoogleCloudPubSubConfiguration(final int bufferSize,
                                   final int threads,
                                   final boolean enabled,
                                   @Nullable final String projectId) {
        super(bufferSize, threads, enabled);
        this.projectId = null != projectId ? Optional.of(projectId) : getDefaultProjectId();
    }

    private static Optional<String> getDefaultProjectId() {
        final Optional<String> projectId = Optional.ofNullable(ServiceOptions.getDefaultProjectId());
        if (projectId.isPresent()) {
            logger.info("Discovered default Google Cloud project: {}", projectId.get());
        } else {
            logger.debug("No default Google Cloud project available.");
        }
        return projectId;
    }

    @Override
    protected MoreObjects.ToStringHelper toStringHelper() {
        return super.toStringHelper()
            .add("projectId", projectId);
    }
}
