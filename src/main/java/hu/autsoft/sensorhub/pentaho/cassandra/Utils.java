package hu.autsoft.sensorhub.pentaho.cassandra;

import com.datastax.driver.core.DataType;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.concurrent.ExecutionException;


public class Utils {
    private static final LoadingCache<Map<String, String>, CassandraConnection> cache = CacheBuilder.newBuilder()
            .build(new CacheLoader<Map<String, String>, CassandraConnection>() {
                @Override
                public CassandraConnection load(Map<String, String> config) throws Exception {
                    return new CassandraConnection(config, Utils.cache);
                }
            });

    public static CassandraConnection connect(String nodes, String port, String username, String password, String keyspace, boolean withSSL, String truststoreFilePath, String truststorePass, ConnectionCompression compression)
            throws CassandraConnectionException {
        Map<String, String> config = buildConfig(nodes, port, username, password, keyspace, withSSL, truststoreFilePath, truststorePass, compression);
        try {
            CassandraConnection connection;
            do {
                connection = cache.get(config);
            }
            while (!connection.acquire());
            return connection;
        } catch (ExecutionException e) {
            throw new CassandraConnectionException("Failed to get a connection", e);
        }
    }

    private static Map<String, String> buildConfig(String nodes, String port, String username, String password, String keyspace, boolean withSSL, String truststoreFilePath, String truststorePass, ConnectionCompression compression) {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.<String, String>builder()
                .put("nodes", nodes)
                .put("port", port)
                .put("SslEnabled", Boolean.toString(withSSL));
        if (username != null) builder.put("username", username);
        if (password != null) builder.put("password", password);
        if (keyspace != null) builder.put("keyspace", keyspace);
        if (truststoreFilePath != null) builder.put("truststoreFilePath", truststoreFilePath);
        if (truststorePass != null) builder.put("truststorePass", truststorePass);
        if (compression != null) builder.put("compression", compression.toString());
        return builder.build();
    }

    public static int convertDataType(DataType type) {
        switch (type.getName()) {
            case BIGINT:
            case COUNTER:
            case INT:
                return 5;
            case ASCII:
            case INET:
            case TEXT:
            case VARCHAR:
                return 2;
            case BOOLEAN:
                return 4;
            case DECIMAL:
            case FLOAT:
            case DOUBLE:
                return 1;
            case CUSTOM:
                return 6;
            case LIST:
                return 3;
            case TIMESTAMP:
                return 2;
        }

        return 8;
    }
}
