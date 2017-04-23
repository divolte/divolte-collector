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

import java.util.Optional;

import com.google.common.base.MoreObjects.ToStringHelper;

public abstract class FileSinkConfiguration extends SinkConfiguration {
    public final FileStrategyConfiguration fileStrategy;

    public FileSinkConfiguration(final FileStrategyConfiguration fileStrategy) {
        super();
        this.fileStrategy = Optional.ofNullable(fileStrategy).orElse(FileStrategyConfiguration.DEFAULT_FILE_STRATEGY_CONFIGURATION);
    }

    public abstract String getReadableType();

    @Override
    protected ToStringHelper toStringHelper() {
        return super.toStringHelper().add("file strategy", fileStrategy);
    }
}
