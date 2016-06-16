/*
 * Copyright 2015 GoDataDriven B.V.
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

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;

import org.hibernate.validator.HibernateValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonMappingException.Reference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.jasonclawson.jackson.dataformat.hocon.HoconTreeTraversingParser;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;

/**
 * Container for a validated configuration loaded from a {@code Config}
 * instance. This container allows access to the underlying configuration values
 * through a {@link DivolteConfiguration} instance which can be obtained from
 * calling {@link #configuration()}, only if the configuration is valid. This
 * method throws an exception otherwise. checking the validity of the
 * configuration must first be done through the {@link #isValid()} method. When
 * the configuration is not valid, a list of {@code ConfigException} instances
 * that were thrown during configuration parsing / loading is available by
 * calling {@link #errors()}.
 */
@ParametersAreNonnullByDefault
public final class ValidatedConfiguration {
    private final static Logger logger = LoggerFactory.getLogger(ValidatedConfiguration.class);

    private final static Joiner DOT_JOINER = Joiner.on('.');

    private final ImmutableList<String> configurationErrors;
    private final Optional<DivolteConfiguration> divolteConfiguration;

    /**
     * Creates an instance of a validated configuration. The underlying
     * {@code Config} object is passed through a supplier, instead of directly.
     * The constructor will catch any {@code ConfigException} thrown from the
     * supplier's getter.
     *
     * @param configLoader
     *            Supplier of the underlying {@code Config} instance.
     */
    public ValidatedConfiguration(final Supplier<Config> configLoader) {
        final ImmutableList.Builder<String> configurationErrors = ImmutableList.builder();

        DivolteConfiguration divolteConfiguration;
        try {
            /*
             * We first load the config using the provided loading method.
             * Then, we validate using bean validation and add any validation
             * errors to the resulting list of error messages.
             */
            final Config config = configLoader.get();
            divolteConfiguration = mapped(config.getConfig("divolte").resolve());
            configurationErrors.addAll(validate(divolteConfiguration));
        } catch(final ConfigException e) {
            logger.debug("Configuration error caught during validation.", e);
            configurationErrors.add(e.getMessage());
            divolteConfiguration = null;
        } catch (final UnrecognizedPropertyException e) {
            // Add a special case for unknown property as we add the list of available properties to the message.
            logger.debug("Configuration error. Exception while mapping.", e);
            final String message = messageForUnrecognizedPropertyException(e);
            configurationErrors.add(message);
            divolteConfiguration = null;
        } catch (final JsonMappingException e) {
            logger.debug("Configuration error. Exception while mapping.", e);
            final String message = messageForMappingException(e);
            configurationErrors.add(message);
            divolteConfiguration = null;
        } catch (final IOException e) {
            logger.error("Error while reading configuration!", e);
            throw new RuntimeException("Error while reading configuration.", e);
        }

        this.configurationErrors = configurationErrors.build();
        this.divolteConfiguration = Optional.ofNullable(divolteConfiguration);
    }

    private String messageForMappingException(final JsonMappingException e) {
        final String pathToError = e.getPath().stream()
                   .map(Reference::getFieldName)
                   .collect(Collectors.joining("."));
        return String.format(
                "%s.%n\tLocation: %s.%n\tConfiguration path to error: '%s'",
                e.getOriginalMessage(),
                Optional.ofNullable(e.getLocation()).map(JsonLocation::getSourceRef).orElse("<unknown source>"),
                "".equals(pathToError) ? "<unknown path>" : pathToError);
    }

    private static String messageForUnrecognizedPropertyException(final UnrecognizedPropertyException e) {
        return String.format(
                "%s.%n\tLocation: %s.%n\tConfiguration path to error: '%s'%n\tAvailable properties: %s.",
                e.getOriginalMessage(),
                e.getLocation().getSourceRef(),
                e.getPath().stream()
                           .map(Reference::getFieldName)
                           .collect(Collectors.joining(".")),
                e.getKnownPropertyIds().stream()
                                       .map(Object::toString).map(s -> "'" + s + "'")
                                       .collect(Collectors.joining(", ")));
    }

    private List<String> validate(final DivolteConfiguration divolteConfiguration) {
        final Validator validator = Validation
                .byProvider(HibernateValidator.class)
                .configure()
                .buildValidatorFactory()
                .getValidator();

        final Set<ConstraintViolation<DivolteConfiguration>> validationErrors = validator.validate(divolteConfiguration);

        return validationErrors
                .stream()
                .map(
                        (e) -> String.format(
                                "Property '%s' %s. Found: '%s'.",
                                DOT_JOINER.join("divolte", e.getPropertyPath()),
                                e.getMessage(),
                                e.getInvalidValue()))
                .collect(Collectors.toList());
    }

    private static DivolteConfiguration mapped(final Config input) throws IOException {
        final Config resolved = input.resolve();
        final ObjectMapper mapper = new ObjectMapper();

        // snake_casing
        mapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);

        // Ignore unknown stuff in the config
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);

        // Deserialization for Duration
        final SimpleModule module = new SimpleModule("Configuration Deserializers");
        module.addDeserializer(Duration.class, new DurationDeserializer());
        module.addDeserializer(Properties.class, new PropertiesDeserializer());

        mapper.registerModules(
                new Jdk8Module(),                   // JDK8 types (Optional, etc.)
                new GuavaModule(),                  // Guava types (immutable collections)
                new ParameterNamesModule(),         // Support JDK8 parameter name discovery
                module                              // Register custom deserializers module
                );

        return mapper.readValue(new HoconTreeTraversingParser(resolved.root()), DivolteConfiguration.class);
    }

    /**
     * Returns the validated configuration object tree. This is only returned
     * when no validation errors exist. The method throws
     * {@code IllegalStateException} otherwise.
     *
     * @return The validated configuration.
     * @throws IllegalStateException
     *             When validation errors exist.
     */
    public DivolteConfiguration configuration() {
        Preconditions.checkState(configurationErrors.isEmpty(),
                                 "Attempt to access invalid configuration.");
        return divolteConfiguration.orElseThrow(() -> new IllegalStateException("Configuration not available."));
    }

    /**
     * Returns a list of {@code ConfigException} that were thrown during
     * configuration validation.
     *
     * @return A list of {@code ConfigException} that were thrown during
     *         configuration validation.
     */
    public List<String> errors() {
        return configurationErrors;
    }

    /**
     * Returns false if validation errors exist, true otherwise.
     *
     * @return false if validation errors exist, true otherwise.
     */
    public boolean isValid() {
        return configurationErrors.isEmpty();
    }
}
