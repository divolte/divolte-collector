package io.divolte.server.hdfs;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FSDataOutputStream;

final class HdfsFileUtils {
    private HdfsFileUtils() {}

    @SuppressWarnings("resource")
    static DataFileWriter<GenericRecord> setupAvroWriter(FSDataOutputStream stream, Schema schema) throws IOException {
        DataFileWriter<GenericRecord> writer = new DataFileWriter<GenericRecord>(new GenericDatumWriter<>(schema)).create(schema, stream);
        writer.setSyncInterval(1 << 30);
        writer.setFlushOnEveryBlock(true);

        return writer;
    }

    @FunctionalInterface
    interface IOExceptionThrower {
        public abstract void run() throws IOException;
    }

    static Optional<IOException> throwsIoException(final IOExceptionThrower r) {
        try {
            r.run();
            return Optional.empty();
        } catch (final IOException ioe) {
            return Optional.of(ioe);
        }
    }

    public static String findLocalHostName() {
        // we should use the bind address from the divolte.server config to figure out the actual hostname we are listening on
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "localhost";
        }
    }
}
