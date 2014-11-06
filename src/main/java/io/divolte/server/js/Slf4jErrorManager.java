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

package io.divolte.server.js;

import javax.annotation.ParametersAreNonnullByDefault;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.javascript.jscomp.BasicErrorManager;
import com.google.javascript.jscomp.CheckLevel;
import com.google.javascript.jscomp.JSError;
import com.google.javascript.jscomp.LightweightMessageFormatter;
import com.google.javascript.jscomp.MessageFormatter;
import com.google.javascript.jscomp.SourceExcerptProvider;

@ParametersAreNonnullByDefault
class Slf4jErrorManager extends BasicErrorManager {
    private static final Logger logger = LoggerFactory.getLogger(Slf4jErrorManager.class);

    private final MessageFormatter formatter;

    public Slf4jErrorManager(final SourceExcerptProvider source) {
        this.formatter = new LightweightMessageFormatter(source);
    }

    @Override
    public void println(final CheckLevel level, final JSError error) {
        final String message = error.format(level, formatter);
        switch (level) {
            case WARNING:
                logger.warn(message);
                break;
            case ERROR:
                logger.error(message);
                break;
            case OFF:
                break;
        }
    }

    @Override
    protected void printSummary() {
        final LogMethod logMethod = (getErrorCount() + getWarningCount() == 0) ?
                                    logger::info : logger::warn;
        if (getTypedPercent() > 0.0) {
            logMethod.log("{} error(s), {} warning(s), {}% typed",
                          getErrorCount(), getWarningCount(), getTypedPercent());
        } else {
            if (getErrorCount() + getWarningCount() > 0) {
                logMethod.log("{} error(s), {} warning(s)",
                              getErrorCount(), getWarningCount());
            }
        }
    }

    @FunctionalInterface
    private interface LogMethod {
        void log(String message, Object... arguments);
    }
}
