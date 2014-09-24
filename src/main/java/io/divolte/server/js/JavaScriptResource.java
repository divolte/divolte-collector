package io.divolte.server.js;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
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
import com.google.javascript.jscomp.SourceFile;

import static com.google.javascript.jscomp.CompilerOptions.LanguageMode.*;

@ParametersAreNonnullByDefault
public class JavaScriptResource {
    private static final Logger logger = LoggerFactory.getLogger(JavaScriptResource.class);

    private static final CompilationLevel COMPILATION_LEVEL = CompilationLevel.ADVANCED_OPTIMIZATIONS;

    private final String resourceName;
    private final ByteBuffer entityBody;
    private final String eTag;

    public JavaScriptResource(final String resourceName,
                              final ImmutableMap<String, Object> scriptConstants,
                              final boolean debugMode) throws IOException {
        this.resourceName = Objects.requireNonNull(resourceName);
        logger.debug("Compiling JavaScript resource: {}", resourceName);
        final Compiler compiler;
        try (final InputStream is = getClass().getResourceAsStream(resourceName)) {
            compiler = compile(resourceName, is, scriptConstants, debugMode);
        }
        logger.info("Finished compiling JavaScript source: {}", resourceName);
        if (!compiler.getResult().success) {
            throw new IllegalArgumentException("Javascript resource contains errors: " + resourceName);
        }
        final byte[] entityBytes = compiler.toSource().getBytes(StandardCharsets.UTF_8);
        entityBody = ByteBuffer.wrap(entityBytes).asReadOnlyBuffer();
        eTag = generateETag(entityBytes);
    }

    public String getResourceName() {
        return resourceName;
    }

    public ByteBuffer getEntityBody() {
        return entityBody.duplicate();
    }

    public String getETag() {
        return eTag;
    }

    private static Compiler compile(final String filename,
                                    final InputStream javascript,
                                    final ImmutableMap<String,Object> scriptConstants,
                                    final boolean debugMode) throws IOException {
        final CompilerOptions options = new CompilerOptions();
        COMPILATION_LEVEL.setOptionsForCompilationLevel(options);
        options.setLanguageIn(ECMASCRIPT5_STRICT);
        options.setLanguageOut(ECMASCRIPT5_STRICT);

        // Enable pseudo-compatible mode for debugging where the output is compiled but
        // can be related more easily to the original JavaScript source.
        if (debugMode) {
            options.setPrettyPrint(true);
            COMPILATION_LEVEL.setDebugOptionsForCompilationLevel(options);
        }

        options.setDefineReplacements(scriptConstants);

        final SourceFile source = SourceFile.fromInputStream(filename, javascript);
        final Compiler compiler = new Compiler();
        final ErrorManager errorManager = new Slf4jErrorManager(compiler);
        compiler.setErrorManager(errorManager);
        // TODO: Use an explicit list of externs instead of the default set, to control compatibility.
        compiler.compile(CommandLineRunner.getDefaultExterns(),
                         ImmutableList.of(source),
                         options);
        return compiler;
    }

    private static String generateETag(final byte[] entityBytes) {
        final MessageDigest digester = createDigester();
        final byte[] digest = digester.digest(entityBytes);
        return '"' + Base64.getEncoder().encodeToString(digest) + '"';
    }

    private static MessageDigest createDigester() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (final NoSuchAlgorithmException e) {
            throw new RuntimeException("JRE missing mandatory digest: SHA-256", e);
        }
    }
}