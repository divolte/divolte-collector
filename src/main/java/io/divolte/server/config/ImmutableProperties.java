package io.divolte.server.config;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Map;
import java.util.Properties;

public class ImmutableProperties extends Properties {
    private static final long serialVersionUID = 1333087762733134653L;

    public static ImmutableProperties fromSource(final Properties source) {
        final ImmutableProperties result = new ImmutableProperties();
        source.forEach(result::set);
        return result;
    }

    private void set(final Object key, final Object value) {
        super.put(key, value);
    }

    @Override
    public synchronized void load(final InputStream inStream) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void load(final Reader reader) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void loadFromXML(final InputStream in) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized Object setProperty(final String key, final String value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized Object put(final Object key, final Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void putAll(final Map<?,?> t) {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized Object remove(final Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void clear() {
        throw new UnsupportedOperationException();
    }
}
