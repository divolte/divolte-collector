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

import io.undertow.util.ETag;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.List;
import java.util.Objects;

import javax.annotation.ParametersAreNonnullByDefault;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.javascript.jscomp.CommandLineRunner;
import com.google.javascript.jscomp.CompilationLevel;
import com.google.javascript.jscomp.Compiler;
import com.google.javascript.jscomp.CompilerOptions;
import com.google.javascript.jscomp.ErrorManager;
import com.google.javascript.jscomp.Result;
import com.google.javascript.jscomp.SourceFile;
import com.google.javascript.jscomp.WarningLevel;

import static com.google.javascript.jscomp.CompilerOptions.LanguageMode.*;

@ParametersAreNonnullByDefault
public class JavaScriptResource {
    private static final Logger logger = LoggerFactory.getLogger(JavaScriptResource.class);

    private static final CompilationLevel COMPILATION_LEVEL = CompilationLevel.ADVANCED_OPTIMIZATIONS;

    private final String resourceName;
    private final ImmutableMap<String, Object> scriptConstants;
    private final GzippableHttpBody entityBody;

    public JavaScriptResource(final String resourceName,
                              final ImmutableMap<String, Object> scriptConstants,
                              final boolean debugMode) throws IOException {
        this.resourceName = Objects.requireNonNull(resourceName);
        this.scriptConstants = Objects.requireNonNull(scriptConstants);
        logger.debug("Compiling JavaScript resource: {}", resourceName);
        final Compiler compiler;
        try (final InputStream is = getClass().getClassLoader().getResourceAsStream(resourceName)) {
            compiler = compile(resourceName, is, scriptConstants, debugMode);
        }
        logger.info("Pre-compiled JavaScript source: {}", resourceName);
        final Result result = compiler.getResult();
        if (!result.success || 0 < result.warnings.length) {
            throw new IllegalArgumentException("Javascript resource contains warnings and/or errors: " + resourceName);
        }
        final byte[] entityBytes = compiler.toSource().getBytes(StandardCharsets.UTF_8);
        entityBody = new GzippableHttpBody(ByteBuffer.wrap(entityBytes), generateETag(entityBytes));
    }

    public String getResourceName() {
        return resourceName;
    }

    protected ImmutableMap<String, Object> getScriptConstants() {
        return scriptConstants;
    }

    public GzippableHttpBody getEntityBody() {
        return entityBody;
    }

    private static Compiler compile(final String filename,
                                    final InputStream javascript,
                                    final ImmutableMap<String,Object> scriptConstants,
                                    final boolean debugMode) throws IOException {
        final CompilerOptions options = new CompilerOptions();
        COMPILATION_LEVEL.setOptionsForCompilationLevel(options);
        COMPILATION_LEVEL.setTypeBasedOptimizationOptions(options);
        options.setEnvironment(CompilerOptions.Environment.BROWSER);
        options.setLanguageIn(ECMASCRIPT5_STRICT);
        options.setLanguageOut(ECMASCRIPT5_STRICT);
        WarningLevel.VERBOSE.setOptionsForWarningLevel(options);

        // Enable pseudo-compatible mode for debugging where the output is compiled but
        // can be related more easily to the original JavaScript source.
        if (debugMode) {
            options.setPrettyPrint(true);
            COMPILATION_LEVEL.setDebugOptionsForCompilationLevel(options);
        }

        options.setDefineReplacements(scriptConstants);

        final SourceFile source = SourceFile.fromInputStream(filename, javascript, StandardCharsets.UTF_8);
        final Compiler compiler = new Compiler();
        final ErrorManager errorManager = new Slf4jErrorManager(compiler);
        compiler.setErrorManager(errorManager);
        // TODO: Use an explicit list of externs instead of the default browser set, to control compatibility.
        final List<SourceFile> externs = CommandLineRunner.getBuiltinExterns(options.getEnvironment());
        compiler.compile(externs, ImmutableList.of(source), options);
        return compiler;
    }

    private static ETag generateETag(final byte[] entityBytes) {
        final MessageDigest digester = createDigester();
        final byte[] digest = digester.digest(entityBytes);
        return new ETag(false, Base64.getEncoder().encodeToString(digest));
    }

    private static MessageDigest createDigester() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (final NoSuchAlgorithmException e) {
            throw new RuntimeException("JRE missing mandatory digest: SHA-256", e);
        }
    }
}
