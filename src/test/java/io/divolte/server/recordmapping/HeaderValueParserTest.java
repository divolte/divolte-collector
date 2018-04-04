package io.divolte.server.recordmapping;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class HeaderValueParserTest {

    private static void assertParserProduces(final String headerValue, final String... expectedValues) {
        final String[] values = HeaderValueParser.values(headerValue).toArray(String[]::new);
        assertArrayEquals(expectedValues, values);
    }

    @Test
    public void parserShouldSplitOnCommas() {
        assertParserProduces("a,b,c", "a", "b", "c");
        assertParserProduces("foo,bar,baz", "foo", "bar", "baz");
    }

    @Test
    public void parserShouldSupportSingleValue() {
        assertParserProduces("foobar", "foobar");
    }

    @Test
    public void parserShouldOmitEmptyValues() {
        // This is invalid, but if it were allowed it would be skipped.
        assertParserProduces("");

        // Next check that empty values are skipped at the beginning, in the middle or at the end.
        assertParserProduces(",a", "a");
        assertParserProduces("a,", "a");
        assertParserProduces("a,,b", "a", "b");
    }

    @Test
    public void parserShouldStripLeadingOrTrailingLinearWhitespace() {
        // The normal situation with space after the comma.
        assertParserProduces("a, b, c", "a", "b", "c");
        // Other cases.
        assertParserProduces(" a , b , c ", "a", "b", "c");

        // Tabs are also allowed.
        assertParserProduces("a,\tb,\tc", "a", "b", "c");

        // Mixtures of tabs and spaces.
        assertParserProduces("a,\t \tb, \t c", "a", "b", "c");

        // Values that are only LWS: these are empty, and should therefore be skipped.
        assertParserProduces("   ");
        assertParserProduces("  ,  ,  ");
    }

    @Test
    public void parserShouldNormalizeLWSInValues() {
        // LWS in the middle of a value should be normalized to a single space.
        assertParserProduces("foo bar", "foo bar");
        assertParserProduces("foo    bar", "foo bar");
        assertParserProduces("foo\tbar", "foo bar");
        assertParserProduces("foo \t bar", "foo bar");
        assertParserProduces("foo\t \tbar", "foo bar");
    }

    @Test
    public void parserShouldSupportSimpleQuotedValue() {
        // An entire value can be quoted.
        assertParserProduces("\"foobar\"", "foobar");
    }

    @Test
    public void parserShouldSupportEmptyQuotedValue() {
        // An simple empty value. (This is skipped.)
        assertParserProduces("\"\"");

        // Check that empty quoted values are skipped at the beginning, in the middle or at the end.
        assertParserProduces("\"\",a", "a");
        assertParserProduces("a,\"\"", "a");
        assertParserProduces("a,\"\",b", "a", "b");
    }

    @Test
    public void parserShouldSupportMixedQuotedContent() {
        // Mixed tokens and quoted strings with LWS in the middle.
        assertParserProduces("\"foo\" bar", "foo bar");
        assertParserProduces("foo \"bar\" baz", "foo bar baz");

        // Mixed tokens and quoted strings without LWS in the middle.
        assertParserProduces("\"foo\"bar", "foobar");
        assertParserProduces("foo\"bar\"baz", "foobarbaz");
    }

    @Test
    public void parserShouldSupportQuotedContentWithCommas() {
        // Quoted strings shouldn't be split.
        assertParserProduces("\"foo,bar\"", "foo,bar");
    }

    @Test
    public void parserShouldSupportPreserveQuotedContentWhitespace() {
        // Quoted strings should have their whitespace left alone.
        assertParserProduces("\"  foo \t bar  \"", "  foo \t bar  ");
    }

    @Test
    public void parserShouldSupportQuotesInQuotedStrings() {
        // Quoted strings are allowed to have embedded quotes.
        assertParserProduces("\"foo\\\"bar\"", "foo\"bar");

        // The escaping character itself needs to be escaped.
        assertParserProduces("\"foo\\\\bar\"", "foo\\bar");
    }

    @Test
    public void parserShouldHandleMalformedStrings() {
        // Various malformed strings. Check that:
        //  - They don't trigger an exception.
        //  - They yield the best thing possible.

        // A string with an unterminated escape sequence.
        assertParserProduces("\"foo\\", "foo");
        // A quoted string missing the closing quote.
        assertParserProduces("\"foo", "foo");
    }
}
