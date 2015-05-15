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

package io.divolte.server.mincode;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.core.format.InputAccessor;
import com.fasterxml.jackson.core.format.MatchStrength;
import com.fasterxml.jackson.core.io.IOContext;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;

@ParametersAreNonnullByDefault
public class MincodeFactory extends JsonFactory {
    private static final long serialVersionUID = 1L;

    public final static String FORMAT_NAME_MINCODING = "Mincoding";
    public final static Version FORMAT_VERSION = Version.unknownVersion();

    public MincodeFactory() {
        this(null);
    }

    public MincodeFactory(@Nullable final ObjectCodec oc) {
        super(oc);
    }

    public MincodeFactory(final MincodeFactory src, @Nullable final ObjectCodec codec) {
        super(src, codec);
    }

    @Override
    public JsonFactory copy() {
        return new MincodeFactory(this, null);
    }

    @Override
    protected Object readResolve() {
        // Force deserialization to go via constructor.
        return new MincodeFactory(this, _objectCodec);
    }

    @Override
    public String getFormatName() {
        return FORMAT_NAME_MINCODING;
    }

    @Override
    public Version version() {
        return FORMAT_VERSION;
    }

    @Override
    public MatchStrength hasFormat(final InputAccessor acc) throws IOException {
        final MatchStrength matchStrength;
        if (acc.hasMoreBytes()) {
            final byte b = acc.nextByte();
            // First byte has to be one of the record types, possibly capital denoting a root object.
            // (Only '.' cannot appear at the start of Mincode.)
            switch (b) {
                case '(':
                case 'a':
                case 's':
                case 't':
                case 'f':
                case 'n':
                case 'd':
                case 'j':
                    matchStrength = MatchStrength.SOLID_MATCH;
                    break;
                default:
                    matchStrength = MatchStrength.NO_MATCH;
            }
        } else {
            // Zero-length isn't supported.
            matchStrength = MatchStrength.NO_MATCH;
        }
        return matchStrength;
    }

    @Override
    public MincodeParser createParser(final File f) throws IOException {
        final IOContext ctxt = _createContext(f, true);
        return _createParser(_decorate(new FileInputStream(f), ctxt), ctxt);
    }

    @Override
    public MincodeParser createParser(final URL url) throws IOException {
        final IOContext ctxt = _createContext(url, true);
        return _createParser(_decorate(_optimizedStreamFromURL(url), ctxt), ctxt);
    }

    @Override
    public MincodeParser createParser(final InputStream in) throws IOException {
        final IOContext ctxt = _createContext(in, false);
        return _createParser(_decorate(in, ctxt), ctxt);
    }

    @Override
    public MincodeParser createParser(final Reader r) throws IOException {
        final IOContext ctxt = _createContext(r, false);
        return _createParser(_decorate(r, ctxt), ctxt);
    }

    @Override
    public MincodeParser createParser(final byte[] data) throws IOException {
        return createParser(data, 0, data.length);
    }

    @Override
    public MincodeParser createParser(byte[] data, int offset, int len) throws IOException {
        final IOContext ctxt = _createContext(data, true);
        if (null != _inputDecorator) {
            final InputStream in = _inputDecorator.decorate(ctxt, data, offset, len);
            if (null != in) {
                return _createParser(in, ctxt);
            }
        }
        return _createParser(data, offset, len, ctxt);
    }

    @Override
    public MincodeParser createParser(final String content) throws IOException {
        return createParser(new StringReader(content));
    }

    @Override
    public MincodeParser createParser(final char[] content) throws IOException {
        return createParser(content, 0, content.length);
    }

    @Override
    public MincodeParser createParser(final char[] content,
                                      final int offset,
                                      final int len) throws IOException {
        return null != _inputDecorator
                ? createParser(new CharArrayReader(content, offset, len))
                : _createParser(content, offset, len, _createContext(content, true), false);
    }

    @Override
    protected MincodeParser _createParser(final InputStream in,
                                          final IOContext ctxt) throws IOException {
        return _createParser(new InputStreamReader(in, StandardCharsets.UTF_8), ctxt);
    }

    @Override
    protected MincodeParser _createParser(final Reader r,
                                          final IOContext ctxt) throws IOException {
        return new MincodeParser(ctxt, _parserFeatures, _objectCodec, r);
    }

    @Override
    protected MincodeParser _createParser(final char[] data,
                                          final int offset,
                                          final int len,
                                          final IOContext ctxt,
                                          final boolean recyclable) throws IOException {
        return _createParser(new CharArrayReader(data, offset, len), ctxt);
    }

    @Override
    protected MincodeParser _createParser(final byte[] data,
                                          final int offset,
                                          final int len,
                                          final IOContext ctxt) throws IOException {
        final ByteArrayInputStream in = new ByteArrayInputStream(data, offset, len);
        return _createParser(new InputStreamReader(in, StandardCharsets.UTF_8), ctxt);
    }
}
