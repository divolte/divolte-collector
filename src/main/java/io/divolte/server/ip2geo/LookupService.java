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
    public class ClosedServiceException extends Exception {
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
