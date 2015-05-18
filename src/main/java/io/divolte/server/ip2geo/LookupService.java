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

package io.divolte.server.ip2geo;

import java.net.InetAddress;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import com.maxmind.geoip2.model.CityResponse;

@ParametersAreNonnullByDefault
public interface LookupService extends AutoCloseable {
    Optional<CityResponse> lookup(InetAddress address) throws ClosedServiceException;

    @ParametersAreNonnullByDefault
    class ClosedServiceException extends Exception {
        private static final long serialVersionUID = 1L;

        private final LookupService service;

        public ClosedServiceException(final LookupService service) {
            this(service, null);
        }

        public ClosedServiceException(final LookupService service, @Nullable final Throwable cause) {
            super("The lookup service has been closed.", cause);
            this.service = Objects.requireNonNull(service);
        }

        public LookupService getService() {
            return service;
        }
    }
}
