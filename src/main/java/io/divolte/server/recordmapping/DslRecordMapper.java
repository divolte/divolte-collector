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

package io.divolte.server.recordmapping;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.io.Files;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import groovy.lang.Script;
import io.divolte.server.DivolteEvent;
import io.divolte.server.config.ValidatedConfiguration;
import io.divolte.server.ip2geo.LookupService;
import io.divolte.server.recordmapping.DslRecordMapping.MappingAction;
import io.divolte.server.recordmapping.DslRecordMapping.MappingAction.MappingResult;

@ParametersAreNonnullByDefault
@NotThreadSafe
public class DslRecordMapper {
    private final static Logger logger = LoggerFactory.getLogger(DslRecordMapper.class);

    private final Schema schema;
    private final List<DslRecordMapping.MappingAction> actions;

    public DslRecordMapper(final ValidatedConfiguration vc, final String groovyFile, final Schema schema, final Optional<LookupService> geoipService) {
        this.schema = Objects.requireNonNull(schema);

        logger.info("Using mapping from script file: {}", groovyFile);

        try {
            final DslRecordMapping mapping = new DslRecordMapping(schema, new UserAgentParserAndCache(vc), geoipService);

            final String groovyScript = Files.toString(new File(groovyFile), StandardCharsets.UTF_8);

            final CompilerConfiguration compilerConfig = new CompilerConfiguration();
            compilerConfig.setScriptBaseClass("io.divolte.groovyscript.MappingBase");

            final Binding binding = new Binding();
            binding.setProperty("mapping", mapping);

            final GroovyShell shell = new GroovyShell(binding, compilerConfig);
            final Script script = shell.parse(groovyScript);

            script.run();

            actions = mapping.actions();
        } catch (final IOException e) {
            throw new RuntimeException("Could not load mapping script file: " + groovyFile, e);
        }
    }

    public DslRecordMapper(final Schema schema, final DslRecordMapping mapping) {
        this.schema = schema;
        actions = mapping.actions();
    }

    public GenericRecord newRecordFromExchange(final DivolteEvent event) {
        final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        final Map<String,Optional<?>> context = Maps.newHashMapWithExpectedSize(20);

        for (final Iterator<MappingAction> itr = actions.iterator();
             itr.hasNext() && itr.next().perform(event, context, builder) == MappingResult.CONTINUE;) {
            // Nothing needed in here.
        }

        return builder.build();
    }
}
