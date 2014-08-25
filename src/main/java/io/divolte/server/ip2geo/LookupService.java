package io.divolte.server.ip2geo;

import java.net.InetAddress;
import java.util.Optional;

import javax.annotation.ParametersAreNonnullByDefault;

import com.maxmind.geoip2.model.CityResponse;

@ParametersAreNonnullByDefault
public interface LookupService extends AutoCloseable {

    Optional<CityResponse> lookup(InetAddress address);
}
