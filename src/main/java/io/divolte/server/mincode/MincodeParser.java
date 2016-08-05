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

import com.fasterxml.jackson.core.Base64Variant;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.base.ParserBase;
import com.fasterxml.jackson.core.io.IOContext;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Objects;
import java.util.Optional;

@ParametersAreNonnullByDefault
public class MincodeParser extends ParserBase {

    private static final Optional<Integer> PENDING_END_OBJECT = Optional.of((int)'.');

    private final Reader reader;
    private char[] inputBuffer;
    private final boolean bufferRecyclable;

    @Nullable
    private ObjectCodec objectCodec;

    private Optional<Integer> pendingRecordType = Optional.empty();

    MincodeParser(final IOContext ctxt,
                  final int parserFeatures,
                  @Nullable final ObjectCodec objectCodec,
                  final Reader reader) {
        super(ctxt, parserFeatures);
        this.reader = Objects.requireNonNull(reader);
        this.inputBuffer = ctxt.allocTokenBuffer();
        this._inputPtr = 0;
        this._inputEnd = 0;
        this.bufferRecyclable = true;
        this.objectCodec = objectCodec;
    }

    private boolean _loadMore() throws IOException {
        _currInputProcessed += _inputEnd;
        _currInputRowStart -= _inputEnd;
        final boolean loadedMore;
        if (null != reader) {
            final int count = reader.read(inputBuffer, 0, inputBuffer.length);
            if (loadedMore = 0 < count) {
                _inputPtr = 0;
                _inputEnd = count;
            } else {
                _closeInput();
            }
        } else {
            loadedMore = false;
        }
        return loadedMore;
    }

    private int nextChar() throws IOException {
        return _inputPtr < _inputEnd || _loadMore() ? inputBuffer[_inputPtr++] : -1;
    }

    private char nextChar(final String eofMsg) throws IOException {
        final int nextChar = nextChar();
        if (-1 == nextChar) {
            _reportInvalidEOF(eofMsg, _currToken);
        }
        return (char)nextChar;
    }

    @Override
    protected void _finishString() throws IOException {
        // Step 1: Scan until we reach either an escape sequence or the
        //         end of the input buffer.
        final int inputLen = _inputEnd;
        int ptr = _inputPtr;
        loop:
        while (ptr < inputLen) {
            final int c = inputBuffer[ptr];
            switch (c) {
                case '!':
                    // Found the end of the string. No escapes necessary.
                    _textBuffer.resetWithShared(inputBuffer, _inputPtr, ptr - _inputPtr);
                    _inputPtr = ptr + 1;
                    // EARLY RETURN
                    return;
                case '~':
                    // Escape sequence encountered; more work will be required.
                    break loop;
                default:
                    // Nothing to do yet; proceed to next character.
            }
            ++ptr;
        }

        // We either ran out of buffer, or hit an escape sequence.

        // Step 2: Make a copy of what we scanned past so far.
        _textBuffer.resetWithCopy(inputBuffer, _inputPtr, ptr - _inputPtr);
        _inputPtr = ptr;

        // Step 3: Get the current segment so we can start filling it in.
        char[] outBuf = _textBuffer.getCurrentSegment();
        int outPtr = _textBuffer.getCurrentSegmentSize();

        // Step 4: Scan over the remaining data, filling in the current segment
        //         and rolling over to new ones as necessary.
        loop:
        for (;;) {
            if (_inputPtr >= _inputEnd && !_loadMore()) {
                _reportInvalidEOF(": was expecting end of string value", _currToken);
            }
            char c = inputBuffer[_inputPtr++];
            switch (c) {
                case '!':
                    // End of string.
                    break loop;
                case '~':
                    // Escape sequence. Next character is the real thing.
                    c = nextChar(" in character escape sequence");
                    break;
                default:
            }
            if (outPtr >= outBuf.length) {
                outBuf = _textBuffer.finishCurrentSegment();
                outPtr = 0;
            }
            outBuf[outPtr++] = c;
        }
        _textBuffer.setCurrentLength(outPtr);
    }

    @Override
    protected void _closeInput() throws IOException {
        if (_ioContext.isResourceManaged() || isEnabled(Feature.AUTO_CLOSE_SOURCE)) {
            reader.close();
        }
    }

    @Override
    protected void _releaseBuffers() throws IOException {
        super._releaseBuffers();
        if (bufferRecyclable) {
            final char[] buf = inputBuffer;
            if (null != inputBuffer) {
                inputBuffer = null;
                _ioContext.releaseTokenBuffer(buf);
            }
        }
    }

    @Override
    @Nullable
    public ObjectCodec getCodec() {
        return objectCodec;
    }

    @Override
    public void setCodec(@Nullable ObjectCodec objectCodec) {
        this.objectCodec = objectCodec;
    }

    @Override
    public int releaseBuffered(final Writer w) throws IOException {
        final int count = _inputEnd - _inputPtr;
        if (0 < count) {
            w.write(inputBuffer, _inputPtr, count);
        }
        return count;
    }

    @Override
    public Object getInputSource() {
        return reader;
    }

    @Override
    @Nullable
    public JsonToken nextToken() throws IOException {
        final JsonToken nextToken;
        if (_closed) {
            nextToken = null;
        } else {
            _tokenInputTotal = _currInputProcessed + _inputPtr - 1;
            _tokenInputRow = _currInputRow;
            _tokenInputCol = _inputPtr - _currInputRowStart - 1;

            // Cursor is positioned:
            //  - At the start of a record.
            //  - At the start of the name of a field if:
            //      - The current token is START_OBJECT
            //      - The pending type isn't '.'
            //  - At the start of the value of a field if:
            //      - The current token is FIELD_NAME

            // We may be in the middle of processing a record.
            // If so, the type has been preserved as the pending record type.
            final int recordType;
            if (pendingRecordType.isPresent()) {
                recordType = pendingRecordType.get();
                pendingRecordType = Optional.empty();
            } else {
                recordType = nextChar();
            }

            // If we just finished a field name, invalidate the
            // buffer containing the text for the field name.
            if (JsonToken.FIELD_NAME == _currToken) {
                _nameCopied = false;
            }

            // Special handling for end-of-object.
            // (This is the only record type while in an object that doesn't
            //  have a field name.)
            if (')' == recordType) {
                if (!_parsingContext.inObject()) {
                    _reportError("Unexpected end of object while not in object.");
                }
                nextToken = JsonToken.END_OBJECT;
                _parsingContext = _parsingContext.getParent();
            } else if (_currToken != JsonToken.FIELD_NAME
                        && _parsingContext.inObject()) {
                // If we're in an object but didn't just deliver the field name,
                // that means we've just started a new record and are positioned
                // over the name of the field.
                nextToken = JsonToken.FIELD_NAME;
                _finishString();
                _parsingContext.setCurrentName(_textBuffer.contentsAsString());
                pendingRecordType = Optional.of(recordType);
            } else {
                // We're now positioned on the payload for the record, if it has any.
                switch (recordType) {
                    case -1:
                        // End-of-file.
                        _handleEOF();
                        close();
                        nextToken = null;
                        break;
                    case 'a':
                        // Start of an array.
                        nextToken = JsonToken.START_ARRAY;
                        _parsingContext = _parsingContext.createChildArrayContext(_tokenInputRow, _tokenInputCol);
                        break;
                    case '.':
                        if (!_parsingContext.inArray()) {
                            _reportError("Unexpected end of array while not in array.");
                        }
                        nextToken = JsonToken.END_ARRAY;
                        _parsingContext = _parsingContext.getParent();
                        break;
                    case '(':
                        // Start of an object.
                        nextToken = JsonToken.START_OBJECT;
                        _parsingContext = _parsingContext.createChildObjectContext(_tokenInputRow, _tokenInputCol);
                        break;
                    case 'o':
                        // An empty object.
                        nextToken = JsonToken.START_OBJECT;
                        _parsingContext = _parsingContext.createChildObjectContext(_tokenInputRow, _tokenInputCol);
                        pendingRecordType = PENDING_END_OBJECT;
                        break;
                    case 's':
                        nextToken = JsonToken.VALUE_STRING;
                        _finishString();
                        break;
                    case 't':
                        // Boolean true.
                        nextToken = JsonToken.VALUE_TRUE;
                        break;
                    case 'f':
                        // Boolean false.
                        nextToken = JsonToken.VALUE_FALSE;
                        break;
                    case 'n':
                        // Null.
                        nextToken = JsonToken.VALUE_NULL;
                        break;
                    case 'd':
                        // Base-36 integer.
                        nextToken = JsonToken.VALUE_NUMBER_INT;
                        nextIntegerRecord();
                        break;
                    case 'j':
                        // JSON-formatted number.
                        nextToken = nextNumberRecord();
                        break;
                    default:
                        // Unknown record type.
                        throw _constructError("Unknown record type encountered: " + (char)recordType);
                }
            }
        }
        _currToken = nextToken;
        return nextToken;
    }

    private void nextIntegerRecord() throws IOException {
        // First process the body of the record, as a string.
        _finishString();
        try {
            // Next parse it and store as BigInteger.
            setNumberValue(new BigInteger(_textBuffer.contentsAsString(), 36));
        } catch (final NumberFormatException e) {
            _wrapError("Invalid integer record", e);
        }
    }

    @SuppressWarnings("PMD.EmptyCatchBlock")
    private void setNumberValue(final BigInteger value) {
        _numTypesValid = NR_BIGINT;
        _numberBigInt = value;
        // Jackson expects all smaller types to be filled in,
        // so do this until they don't fit.
        try {
            _numberLong = value.longValueExact();
            _numTypesValid |= NR_LONG;
            _numberInt = value.intValueExact();
            _numTypesValid |= NR_INT;
        } catch (final ArithmeticException e) {
            // Harmless; means we reached a type into which it won't fit.
        }
    }

    private JsonToken nextNumberRecord() throws IOException {
        // First process the body of the record, as a string,
        // and convert to a BigDecimal.
        _finishString();
        JsonToken token;
        try {
            final BigDecimal number = _textBuffer.contentsAsDecimal();
            // We have a number, but don't know yet if it's an integer or floating point.
            // Jackson uses floating point to mean a decimal and/or exponent is present.
            // Our best heuristic for this is to check if the scale is 0.
            if (0 == number.scale()) {
                token = JsonToken.VALUE_NUMBER_INT;
                setNumberValue(number.unscaledValue());
            } else {
                token = JsonToken.VALUE_NUMBER_FLOAT;
                _numTypesValid = NR_BIGDECIMAL;
                _numberBigDecimal = number;
                // Unlike integer types, Jackson will convert to double/float on demand.
            }
        } catch (final NumberFormatException e) {
            _wrapError("Invalid number record", e);
            // Unreachable.
            token = null;
        }
        return token;
    }

    @Override
    @Nullable
    public String getText() throws IOException {
        final String text;
        final JsonToken t = _currToken;
        if (null != t) {
            switch (t) {
                case VALUE_STRING:
                case VALUE_NUMBER_INT:
                case VALUE_NUMBER_FLOAT:
                    text = _textBuffer.contentsAsString();
                    break;
                case FIELD_NAME:
                    text = _parsingContext.getCurrentName();
                    break;
                default:
                    text = t.asString();
            }
        } else {
            text = null;
        }
        return text;
    }

    @Override
    @Nullable
    public char[] getTextCharacters() throws IOException {
        final JsonToken t = _currToken;
        final char[] textCharacters;
        if (null != _currToken) {
            switch (_currToken) {
                case FIELD_NAME:
                    if (!_nameCopied) {
                        final String name = _parsingContext.getCurrentName();
                        final int nameLen = name.length();
                        if (_nameCopyBuffer == null) {
                            _nameCopyBuffer = _ioContext.allocNameCopyBuffer(nameLen);
                        } else if (_nameCopyBuffer.length < nameLen) {
                            _nameCopyBuffer = new char[nameLen];
                        }
                        name.getChars(0, nameLen, _nameCopyBuffer, 0);
                        _nameCopied = true;
                    }
                    textCharacters = _nameCopyBuffer;
                    break;
                case VALUE_STRING:
                case VALUE_NUMBER_INT:
                case VALUE_NUMBER_FLOAT:
                    textCharacters = _textBuffer.getTextBuffer();
                    break;
                default:
                    textCharacters = t.asCharArray();
            }
        } else {
            textCharacters = null;
        }
        return textCharacters;
    }

    @Override
    public int getTextLength() throws IOException {
        final int textLength;
        final JsonToken t = _currToken;
        if (null != t) {
            switch (t) {
                case FIELD_NAME:
                    textLength = _parsingContext.getCurrentName().length();
                    break;
                case VALUE_STRING:
                case VALUE_NUMBER_INT:
                case VALUE_NUMBER_FLOAT:
                    textLength = _textBuffer.size();
                    break;
                default:
                    textLength = t.asCharArray().length;
            }
        } else {
            textLength = 0;
        }
        return textLength;
    }

    @Override
    public int getTextOffset() throws IOException {
        final int textOffset;
        final JsonToken t = _currToken;
        if (null != t) {
            switch (t) {
                case VALUE_STRING:
                case VALUE_NUMBER_INT:
                case VALUE_NUMBER_FLOAT:
                    textOffset = _textBuffer.getTextOffset();
                    break;
                case FIELD_NAME:
                default:
                    textOffset = 0;
            }
        } else {
            textOffset = 0;
        }
        return textOffset;
    }

    @Override
    public byte[] getBinaryValue(final Base64Variant b64variant) throws IOException {
        return b64variant.decode(_textBuffer.contentsAsString());
    }
}
