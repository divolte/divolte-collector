/*
 * Copyright 2019 GoDataDriven B.V.
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

package io.divolte.server.js;

import com.google.common.collect.ImmutableList;
import com.google.javascript.jscomp.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.function.Consumer;

@ParametersAreNonnullByDefault
public class Slf4jErrorReportGenerator implements SortingErrorManager.ErrorReportGenerator {
    private static final Logger logger = LoggerFactory.getLogger(Slf4jErrorReportGenerator.class);

    private final MessageFormatter formatter;

    public Slf4jErrorReportGenerator(final SourceExcerptProvider source) {
        this.formatter = new LightweightMessageFormatter(source);
    }

    @Override
    public void generateReport(final SortingErrorManager manager) {
        logErrors(logger::error, CheckLevel.ERROR, manager.getErrors());
        logErrors(logger::warn, CheckLevel.WARNING, manager.getWarnings());

        final LogMethod logMethod = (0 == manager.getErrorCount() + manager.getWarningCount())
            ? logger::info
            : logger::warn;
        logMethod.log("{} error(s), {} warning(s), {}% typed",
                      manager.getErrorCount(), manager.getWarningCount(), manager.getTypedPercent());
    }

    private void logErrors(final Consumer<String> logMethod, final CheckLevel level, final ImmutableList<JSError> errors) {
        for (final JSError error : errors) {
            logMethod.accept(error.format(level, formatter));
        }
    }

    @FunctionalInterface
    private interface LogMethod {
        void log(String message, Object... arguments);
    }
}
