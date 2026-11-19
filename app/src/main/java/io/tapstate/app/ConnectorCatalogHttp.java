package io.tapstate.app;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.jar.Manifest;
import java.util.zip.ZipInputStream;

final class ConnectorCatalogHttp implements ConnectorReleaseCatalog.Fetch {
    private static final int MANIFEST_PREFIX_BYTES = 16 * 1024;
    private static final int MAX_METADATA_BYTES = 1024 * 1024;
    private static final int TIMEOUT_MILLIS = 10_000;

    @Override
    public byte[] read(URI uri) throws IOException {
        var connection = open(uri);
        try {
            int status = connection.getResponseCode();
            if (status == 404) throw new ConnectorReleaseCatalog.MissingResource(uri);
            verify(status);
            try (var body = connection.getInputStream()) {
                byte[] bytes = body.readNBytes(MAX_METADATA_BYTES + 1);
                if (bytes.length > MAX_METADATA_BYTES) throw new IOException("Connector catalog response exceeds its size limit");
                return bytes;
            }
        } finally {
            connection.disconnect();
        }
    }

    @Override
    public boolean available(URI uri) throws IOException {
        var connection = open(uri);
        connection.setRequestMethod("HEAD");
        try {
            int status = connection.getResponseCode();
            if (status == 404) return false;
            verify(status);
            return true;
        } finally {
            connection.disconnect();
        }
    }

    @Override
    public String authentication(URI uri) throws IOException {
        var connection = open(uri);
        connection.setRequestProperty("Range", "bytes=0-" + (MANIFEST_PREFIX_BYTES - 1));
        try {
            int status = connection.getResponseCode();
            if (status != HttpURLConnection.HTTP_PARTIAL) verify(status);
            try (var body = connection.getInputStream()) {
                return authentication(body.readNBytes(MANIFEST_PREFIX_BYTES));
            }
        } finally {
            connection.disconnect();
        }
    }

    // Published JARs put the manifest first, optionally preceded by directory entries.
    static String authentication(byte[] prefix) throws IOException {
        try (var zip = new ZipInputStream(new ByteArrayInputStream(prefix))) {
            var entry = zip.getNextEntry();
            while (entry != null && entry.isDirectory()) entry = zip.getNextEntry();
            if (entry == null || !"META-INF/MANIFEST.MF".equalsIgnoreCase(entry.getName())) return null;
            byte[] manifest = zip.readNBytes(MANIFEST_PREFIX_BYTES + 1);
            if (manifest.length > MANIFEST_PREFIX_BYTES) throw new IOException("Connector manifest exceeds its size limit");
            return new Manifest(new ByteArrayInputStream(manifest)).getMainAttributes().getValue("Authentication");
        }
    }

    record Transfer(java.io.InputStream body, long length, HttpURLConnection connection) implements AutoCloseable {
        @Override public void close() throws IOException {
            try {
                body.close();
            } finally {
                connection.disconnect();
            }
        }
    }

    Transfer transfer(URI uri) throws IOException {
        var connection = open(uri);
        try {
            verify(connection.getResponseCode());
            return new Transfer(connection.getInputStream(), connection.getContentLengthLong(), connection);
        } catch (IOException failure) {
            connection.disconnect();
            throw failure;
        }
    }

    private static void verify(int status) throws IOException {
        if (status != 200) throw new IOException("Connector catalog request returned HTTP " + status);
    }

    private static HttpURLConnection open(URI uri) throws IOException {
        var connection = (HttpURLConnection) uri.toURL().openConnection();
        connection.setInstanceFollowRedirects(false);
        connection.setConnectTimeout(TIMEOUT_MILLIS);
        connection.setReadTimeout(TIMEOUT_MILLIS);
        return connection;
    }
}
