package io.divolte.server.recordmapping;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@ParametersAreNonnullByDefault
class HeaderValueParser {
    private final String headerValue;

    private HeaderValueParser(final String headerValue) {
        this.headerValue = Objects.requireNonNull(headerValue);
    }

    public static Stream<String> values(final String headerValue) {
        return new HeaderValueParser(headerValue).values();
    }

    private Stream<String> values() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new HeaderValueIterator(headerValue),
                                                                        Spliterator.ORDERED | Spliterator.NONNULL | Spliterator.IMMUTABLE),
                                                                        false);
    }

    private static class HeaderValueIterator implements Iterator<String> {
        private final String headerValue;
        private final StringBuilder builder;

        // Index of the next character the scanner will examine.
        private int cursor;

        // The next value to return.
        private Optional<String> nextValue = Optional.empty();

        public HeaderValueIterator(final String headerValue) {
            this.headerValue = Objects.requireNonNull(headerValue);
            this.builder = new StringBuilder(headerValue.length());
        }

        @Override
        public boolean hasNext() {
            final boolean haveNextValue;
            if (nextValue.isPresent()) {
                haveNextValue = true;
            } else {
                nextValue = parseNextValue();
                haveNextValue = nextValue.isPresent();
            }
            return haveNextValue;
        }

        @Override
        public String next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            final String value = nextValue.orElseThrow(IllegalStateException::new);
            nextValue = Optional.empty();
            return value;
        }

        private static boolean isWS(final char ch) {
            switch (ch) {
                case ' ':
                    return true;
                case '\t':
                    return true;
                default:
                    return false;
            }
        }

        private void skipOptionalWhitespace() {
            // Grammar rule: *(SP|HT)
            while (cursor < headerValue.length() && isWS(headerValue.charAt(cursor))) {
                ++cursor;
            }
        }

        private void scanToEndOfQuotedString() {
            // Cursor starts on character after initial quote.
            // We look for the end-quote, taking into account \-style escape sequences
            // along the way.
            int startSegment = cursor;
            while (cursor < headerValue.length()) {
                switch (headerValue.charAt(cursor)) {
                    case '"':
                        // Reached the end. Flush the segment and return.
                        builder.append(headerValue, startSegment, cursor++);
                        return;
                    case '\\':
                        // Found an escape sequence. Flush segment and start a new one.
                        builder.append(headerValue, startSegment, cursor++);
                        startSegment = cursor;
                        // Advance the cursor past the escaped character if possible, to ensure
                        // we don't terminate on it.
                        if (cursor < headerValue.length()) {
                            ++cursor;
                        }
                        break;
                    default:
                        // Normal character. Keep looking.
                        ++cursor;
                }
            }
            // Ran off the end of the string. Just flush the segment.
            builder.append(headerValue, startSegment, cursor);
        }

        private void scanToEndOfToken() {
            // Cursor starts on the first character of the token.
            // We will scan until end-of-string or non-token.
            final int startSegment = cursor++;
            loop:
            for (;;) {
                if (cursor >= headerValue.length()) {
                    // Walked off the end of the string. Flush the segment.
                    builder.append(headerValue, startSegment, cursor);
                    return;
                }
                final char c = headerValue.charAt(cursor);
                switch (c) {
                    case ',':
                    case '"':
                    case ' ':
                    case '\t':
                        break loop;
                    default:
                }
                ++cursor;
            }
            // Flush the token. Cursor remains positioned on whatever triggered the end.
            builder.append(headerValue, startSegment, cursor);
        }

        private void scanToNextComma() {
            boolean whitespaceSkipped = false;
            loop:
            while (cursor < headerValue.length()) {
                // On entry we know we're not on LWS. We're on one of:
                //  - start of token.
                //  - start of quoted string.
                //  - comma
                final char c = headerValue.charAt(cursor);
                switch (c) {
                    case ',':
                        ++cursor;
                        break loop;
                    case '"':
                        ++cursor;
                        if (whitespaceSkipped) {
                            builder.append(' ');
                        }
                        scanToEndOfQuotedString();
                        break;
                    default:
                        if (whitespaceSkipped) {
                            builder.append(' ');
                        }
                        scanToEndOfToken();
                        break;
                }
                if (cursor < headerValue.length() && isWS(headerValue.charAt(cursor))) {
                    whitespaceSkipped = true;
                    skipOptionalWhitespace();
                }
            }
        }

        private Optional<String> parseNextValue() {
            // Steps are:
            //  1. Skip any optional whitespace.
            //  2. Scan to the next comma.
            //      - While scanning for the next comma, track the last non-LWS character.

            // Reset the accumulator.
            builder.setLength(0);

            // Loop until we have a non-empty value.
            do {
                // Step 1: Skip the optional whitespace.
                skipOptionalWhitespace();

                // Step 2: Scan to the next comma.
                scanToNextComma();
            } while (cursor < headerValue.length() && 0 == builder.length());

            // Yield the accumulated value if non-empty.
            return 0 < builder.length() ? Optional.of(builder.toString()) : Optional.empty();
        }
    }
}
