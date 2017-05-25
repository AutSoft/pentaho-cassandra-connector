package hu.autsoft.sensorhub.pentaho.cassandra;

import lombok.Getter;
import lombok.Setter;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;


@Getter
@Setter
public abstract class AbstractCassandraMeta extends BaseStepMeta implements StepMetaInterface {
    protected static final String CASSANDRA_NODES = "cassandra_nodes";
    protected static final String CASSANDRA_PORT = "cassandra_port";
    protected static final String USERNAME = "username";
    protected static final String PASSWORD = "password";
    protected static final String CASSANDRA_KEYSPACE = "cassandra_keyspace";
    protected static final String CASSANDRA_WITH_SSL = "cassandra_withSSL";
    protected static final String CASSANDRA_TRUSTSTORE_FILE_PATH = "cassandra_trustStoreFilePath";
    protected static final String CASSANDRA_TRUSTSTORE_PASS = "cassandra_trustStorePass";
    protected static final String COMPRESSION = "compression";
    protected static final String CASSANDRA_COLUMN_FAMILY = "cassandra_columnFamily";

    protected static final String FOUR_SPACES_FOR_XML = "    ";

    protected String cassandraNodes;
    protected String cassandraPort;
    protected String username;
    protected String password;
    protected String keyspace;
    protected String columnfamily;
    protected Boolean SslEnabled;
    protected String trustStoreFilePath;
    protected String trustStorePass;
    protected ConnectionCompression compression;

    @Override
    public void setDefault() {
        this.cassandraNodes = "localhost";
        this.cassandraPort = "9042";
        this.username = "";
        this.password = "";
        this.keyspace = "";
        this.columnfamily = "";
        this.SslEnabled = Boolean.FALSE;
        this.trustStoreFilePath = "";
        this.trustStorePass = "";
        this.compression = ConnectionCompression.SNAPPY;
    }

    protected void checkNulls() {
        if (this.cassandraNodes == null) {
            this.cassandraNodes = "";
        }
        if (this.cassandraPort == null) {
            this.cassandraPort = "";
        }
        if (this.username == null) {
            this.username = "";
        }
        if (this.password == null) {
            this.password = "";
        }
        if (this.keyspace == null) {
            this.keyspace = "";
        }
        if (this.columnfamily == null) {
            this.columnfamily = "";
        }
        if (this.SslEnabled == null) {
            this.SslEnabled = Boolean.FALSE;
        }
        if (this.trustStoreFilePath == null) {
            this.trustStoreFilePath = "";
        }
        if (this.trustStorePass == null) {
            this.trustStorePass = "";
        }
    }
}
