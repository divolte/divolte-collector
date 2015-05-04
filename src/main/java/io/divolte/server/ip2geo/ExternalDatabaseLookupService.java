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

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.ParametersAreNonnullByDefault;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.maxmind.geoip2.model.CityResponse;

@ParametersAreNonnullByDefault
public class ExternalDatabaseLookupService implements LookupService {
    private static final Logger logger = LoggerFactory.getLogger(ExternalDatabaseLookupService.class);

    private final ExecutorService backgroundWatcher = Executors.newSingleThreadExecutor();
    private final WatchService watcher;
    private final Path location;

    // The reference may be null if we don't have a delegate service yet.
    private final AtomicReference<DatabaseLookupService> databaseLookupService;

    public ExternalDatabaseLookupService(final Path location) throws IOException {
        final Path absoluteLocation = location.toAbsolutePath();
        final Path locationParent = absoluteLocation.getParent();
        if (null == locationParent) {
            throw new IllegalArgumentException("Could not determine parent directory of GeoIP2 database: " + absoluteLocation);
        }
        this.location = absoluteLocation;
        // Do this first, so that if it fails we don't need to clean up resources.
        databaseLookupService = new AtomicReference<>(new DatabaseLookupService(absoluteLocation));

        // Set things up so that we can reload the database if it changes.
        watcher = FileSystems.getDefault().newWatchService();
        logger.debug("Monitoring {} for changes affecting {}.", locationParent, location);
        locationParent.register(watcher, StandardWatchEventKinds.ENTRY_MODIFY);
        // The database will be loaded in the background.
        backgroundWatcher.execute(this::processWatchEvents);
    }

    private void processWatchEvents() {
        final Thread thisThread = Thread.currentThread();
        try {
            logger.debug("Starting to wait for watch events");
            while (!thisThread.isInterrupted()) {
                // Block for the key to be signaled.
                logger.debug("Awaiting next file notification...");
                final WatchKey key = watcher.take();
                try {
                    final Path parentDirectory = (Path) key.watchable();
                    for (final WatchEvent<?> event : key.pollEvents()) {
                        processWatchEvent(parentDirectory, event);
                    }
                } finally {
                    // Ensure that no matter what happens we will receive subsequent notifications.
                    key.reset();
                }
            }
            logger.debug("Stopped processing watch events due to interruption.");
        } catch (final InterruptedException e) {
            logger.debug("Interrupted while waiting for a watch event; stopping.");
            // Preserve the interrupt flag.
            thisThread.interrupt();
        } catch (final ClosedWatchServiceException e) {
            logger.debug("Stopped processing watch events; watcher has been closed.");
        }
    }

    private void processWatchEvent(final Path parentDirectory, final WatchEvent<?> event) {
        final WatchEvent.Kind<?> kind = event.kind();
        if (kind == StandardWatchEventKinds.OVERFLOW) {
            // Ignore these for now; we could treat this as a potential change.
            logger.warn("File notification overflow; may have missed database update.");
        } else if (kind == StandardWatchEventKinds.ENTRY_MODIFY) {
            final Path modifiedRelativePath = (Path) event.context();
            final Path modifiedAbsolutePath = parentDirectory.resolve(modifiedRelativePath);
            if (location.equals(modifiedAbsolutePath)) {
                logger.debug("Database has been updated; attempting reload: {}", modifiedAbsolutePath);
                reloadDatabase();
            } else {
                logger.debug("Ignoring file modified event: {}", modifiedAbsolutePath);
            }
        } else {
            logger.debug("Ignoring file notification: {}", event);
        }
    }

    private void reloadDatabase() {
        try {
            final DatabaseLookupService newService = new DatabaseLookupService(location);
            final DatabaseLookupService oldService = databaseLookupService.getAndSet(newService);
            logger.info("Reloaded database: {}", location);
            try {
                oldService.close();
            } catch (final IOException e) {
                logger.warn("Could not close previous database: " + location, e);
            }
        } catch (final IOException e) {
            logger.warn("Database modified but could not reload: " + location, e);
        }
    }

    @Override
    public Optional<CityResponse> lookup(final InetAddress address) throws ClosedServiceException {
        final DatabaseLookupService service = databaseLookupService.get();
        if (null == service) {
            throw new ClosedServiceException(this);
        }
        try {
            return service.lookup(address);
        } catch (final ClosedServiceException e) {
            // We accidentally performed a lookup using an underlying service that was closed
            // between us fetching it and using it. Retry via recursion.
            return lookup(address);
        }
    }

    @Override
    public void close() throws IOException {
        backgroundWatcher.shutdown();
        watcher.close();
        final DatabaseLookupService service = databaseLookupService.getAndSet(null);
        if (null != service) {
            service.close();
        }
    }
}
