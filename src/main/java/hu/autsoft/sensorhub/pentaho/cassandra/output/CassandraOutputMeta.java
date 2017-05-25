package hu.autsoft.sensorhub.pentaho.cassandra.output;

import hu.autsoft.sensorhub.pentaho.cassandra.AbstractCassandraMeta;
import hu.autsoft.sensorhub.pentaho.cassandra.ConnectionCompression;

import java.util.List;

import lombok.Getter;
import lombok.Setter;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

@Getter
@Setter
@Step(id = "DatastaxCassandra-3-Output", image = "DatastaxCassandraOutput.svg", name = "AutSoftCassandraOutput.StepName",
        i18nPackageName = "hu.autsoft.sensorhub.pentaho.cassandra.output.messages", description = "AutSoftCassandraOutput.StepDescription",
        categoryDescription = "AutSoftCassandraOutput.StepCategory",
        documentationUrl = "http://sensorhub.autsoft.hu/docs")
public class CassandraOutputMeta extends AbstractCassandraMeta {
    public static final Class<?> PKG = CassandraOutputMeta.class;

    private static final String SYNC_ENABLED = "sync_enabled";
    private static final String BATCH_SIZE = "batch_size";
    private static final String QUERY_COMPRESSION = "query_compression";
    private static final String SPECIFY_FIELDS = "specify_fields";
    private static final String TTL = "ttl";
    private static final String STREAM_NAME = "stream_name";
    private static final String COLUMN_NAME = "column_name";
    private static final String FIELD = "field";
    private static final String FIELD_MAPPING = "field_mapping";


    protected boolean syncMode;
    protected int batchSize = 1000;
    protected boolean specifyFields = false;
    protected String[] streamFields;
    protected String[] cassandraFields;
    protected int ttl = 0;


    public void allocate(int nrRows) {
        this.streamFields = new String[nrRows];
        this.cassandraFields = new String[nrRows];
    }

    @Override
    public String getXML() {
        StringBuilder retval = new StringBuilder();

        if (!Utils.isEmpty(this.cassandraNodes)) {
            retval.append(FOUR_SPACES_FOR_XML).append(XMLHandler.addTagValue(CASSANDRA_NODES, this.cassandraNodes));
        }

        if (!Utils.isEmpty(this.cassandraPort)) {
            retval.append(FOUR_SPACES_FOR_XML).append(XMLHandler.addTagValue(CASSANDRA_PORT, this.cassandraPort));
        }

        if (!Utils.isEmpty(this.password)) {
            retval.append(FOUR_SPACES_FOR_XML).append(XMLHandler.addTagValue(PASSWORD, Encr.encryptPasswordIfNotUsingVariables(this.password)));
        }

        if (!Utils.isEmpty(this.username)) {
            retval.append(FOUR_SPACES_FOR_XML).append(XMLHandler.addTagValue(USERNAME, this.username));
        }

        if (!Utils.isEmpty(this.keyspace)) {
            retval.append(FOUR_SPACES_FOR_XML).append(XMLHandler.addTagValue(CASSANDRA_KEYSPACE, this.keyspace));
        }

        retval.append(FOUR_SPACES_FOR_XML).append(XMLHandler.addTagValue(CASSANDRA_WITH_SSL, this.SslEnabled));

        if (!Utils.isEmpty(this.trustStoreFilePath)) {
            retval.append(FOUR_SPACES_FOR_XML).append(XMLHandler.addTagValue(CASSANDRA_TRUSTSTORE_FILE_PATH, this.trustStoreFilePath));
        }

        if (!Utils.isEmpty(this.trustStorePass)) {
            retval.append(FOUR_SPACES_FOR_XML).append(XMLHandler.addTagValue(CASSANDRA_TRUSTSTORE_PASS,
                    Encr.encryptPasswordIfNotUsingVariables(this.trustStorePass)));
        }

        if (!Utils.isEmpty(this.columnfamily)) {
            retval.append(FOUR_SPACES_FOR_XML).append(XMLHandler.addTagValue(CASSANDRA_COLUMN_FAMILY, this.columnfamily));
        }

        retval.append(FOUR_SPACES_FOR_XML).append(XMLHandler.addTagValue(SYNC_ENABLED, this.syncMode));
        retval.append(FOUR_SPACES_FOR_XML).append(XMLHandler.addTagValue(BATCH_SIZE, this.batchSize));
        retval.append(FOUR_SPACES_FOR_XML).append(XMLHandler.addTagValue(QUERY_COMPRESSION, this.compression.toString()));
        retval.append(FOUR_SPACES_FOR_XML).append(XMLHandler.addTagValue(SPECIFY_FIELDS, this.specifyFields));

        if (this.specifyFields) {
            retval.append("    <field_mapping>");
            for (int i = 0; i < this.cassandraFields.length; i++) {
                retval.append("      <field>");
                retval.append(FOUR_SPACES_FOR_XML + FOUR_SPACES_FOR_XML).append(XMLHandler.addTagValue(COLUMN_NAME, this.cassandraFields[i]));
                retval.append(FOUR_SPACES_FOR_XML + FOUR_SPACES_FOR_XML).append(XMLHandler.addTagValue(STREAM_NAME, this.streamFields[i]));
                retval.append("      </field>");
            }
            retval.append("    </field_mapping>");
        }

        retval.append(FOUR_SPACES_FOR_XML).append(XMLHandler.addTagValue(TTL, this.ttl));

        return retval.toString();
    }

    @Override
    public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metastore) throws KettleXMLException {
        this.cassandraNodes = XMLHandler.getTagValue(stepnode, CASSANDRA_NODES);
        this.cassandraPort = XMLHandler.getTagValue(stepnode, CASSANDRA_PORT);
        this.username = XMLHandler.getTagValue(stepnode, USERNAME);
        this.password = XMLHandler.getTagValue(stepnode, PASSWORD);

        if (!Utils.isEmpty(this.password)) {
            this.password = Encr.decryptPasswordOptionallyEncrypted(this.password);
        }

        this.keyspace = XMLHandler.getTagValue(stepnode, CASSANDRA_KEYSPACE);
        this.SslEnabled = "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, CASSANDRA_WITH_SSL));
        this.trustStoreFilePath = XMLHandler.getTagValue(stepnode, CASSANDRA_TRUSTSTORE_FILE_PATH);
        this.trustStorePass = XMLHandler.getTagValue(stepnode, CASSANDRA_TRUSTSTORE_PASS);

        if (!Utils.isEmpty(this.trustStorePass)) {
            this.trustStorePass = Encr.decryptPasswordOptionallyEncrypted(this.trustStorePass);
        }

        this.columnfamily = XMLHandler.getTagValue(stepnode, CASSANDRA_COLUMN_FAMILY);
        this.syncMode = "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, SYNC_ENABLED));

        String batchSize = XMLHandler.getTagValue(stepnode, BATCH_SIZE);
        this.batchSize = (Utils.isEmpty(batchSize) ? 1000 : Integer.valueOf(batchSize));

        String sCompression = XMLHandler.getTagValue(stepnode, QUERY_COMPRESSION);
        this.compression = (Utils.isEmpty(sCompression) ? ConnectionCompression.SNAPPY : ConnectionCompression.fromString(sCompression));

        this.specifyFields = "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, SPECIFY_FIELDS));

        Node fields = XMLHandler.getSubNode(stepnode, FIELD_MAPPING);
        int nrRows = XMLHandler.countNodes(fields, FIELD);

        allocate(nrRows);
        for (int i = 0; i < nrRows; i++) {
            Node knode = XMLHandler.getSubNodeByNr(fields, FIELD, i);

            this.cassandraFields[i] = XMLHandler.getTagValue(knode, COLUMN_NAME);
            this.streamFields[i] = XMLHandler.getTagValue(knode, STREAM_NAME);
        }

        this.ttl = Const.toInt(XMLHandler.getTagValue(stepnode, TTL), 0);
    }

    @Override
    public void readRep(Repository rep, IMetaStore metastore, ObjectId id_step, List<DatabaseMeta> databases)
            throws KettleException {
        this.cassandraNodes = rep.getStepAttributeString(id_step, 0, CASSANDRA_NODES);
        this.cassandraPort = rep.getStepAttributeString(id_step, 0, CASSANDRA_PORT);
        this.username = rep.getStepAttributeString(id_step, 0, USERNAME);
        this.password = rep.getStepAttributeString(id_step, 0, PASSWORD);
        if (!Utils.isEmpty(this.password)) {
            this.password = Encr.decryptPasswordOptionallyEncrypted(this.password);
        }
        this.keyspace = rep.getStepAttributeString(id_step, 0, CASSANDRA_KEYSPACE);
        this.SslEnabled = rep.getStepAttributeBoolean(id_step, 0, CASSANDRA_WITH_SSL);
        this.trustStoreFilePath = rep.getStepAttributeString(id_step, 0, CASSANDRA_TRUSTSTORE_FILE_PATH);
        this.trustStorePass = rep.getStepAttributeString(id_step, 0, CASSANDRA_TRUSTSTORE_PASS);
        if (!Utils.isEmpty(this.trustStorePass)) {
            this.trustStorePass = Encr.decryptPasswordOptionallyEncrypted(this.trustStorePass);
        }
        this.columnfamily = rep.getStepAttributeString(id_step, 0, CASSANDRA_COLUMN_FAMILY);
        this.syncMode = rep.getStepAttributeBoolean(id_step, 0, SYNC_ENABLED);

        String batchSize = rep.getStepAttributeString(id_step, 0, BATCH_SIZE);
        this.batchSize = (Utils.isEmpty(batchSize) ? 1000 : Integer.valueOf(batchSize));

        String sCompression = rep.getStepAttributeString(id_step, 0, QUERY_COMPRESSION);
        this.compression = (Utils.isEmpty(sCompression) ? ConnectionCompression.SNAPPY : ConnectionCompression.fromString(sCompression));

        this.specifyFields = rep.getStepAttributeBoolean(id_step, 0, SPECIFY_FIELDS);

        int nrCols = rep.countNrStepAttributes(id_step, COLUMN_NAME);
        int nrStreams = rep.countNrStepAttributes(id_step, STREAM_NAME);
        int nrRows = nrCols < nrStreams ? nrStreams : nrCols;

        allocate(nrRows);
        for (int idx = 0; idx < nrRows; idx++) {
            this.cassandraFields[idx] = Const.NVL(rep.getStepAttributeString(id_step, idx, COLUMN_NAME), "");
            this.streamFields[idx] = Const.NVL(rep.getStepAttributeString(id_step, idx, STREAM_NAME), "");
        }

        this.ttl = Const.toInt(rep.getStepAttributeString(id_step, 0, TTL), 0);
    }

    @Override
    public void saveRep(Repository rep, IMetaStore metastore, ObjectId id_transformation, ObjectId id_step) throws KettleException {
        if (!Utils.isEmpty(this.cassandraNodes)) {
            rep.saveStepAttribute(id_transformation, id_step, 0, CASSANDRA_NODES, this.cassandraNodes);
        }

        if (!Utils.isEmpty(this.cassandraPort)) {
            rep.saveStepAttribute(id_transformation, id_step, 0, CASSANDRA_PORT, this.cassandraPort);
        }

        if (!Utils.isEmpty(this.username)) {
            rep.saveStepAttribute(id_transformation, id_step, 0, USERNAME, this.username);
        }

        if (!Utils.isEmpty(this.password)) {
            rep.saveStepAttribute(id_transformation, id_step, 0, PASSWORD,
                    Encr.encryptPasswordIfNotUsingVariables(this.password));
        }

        if (!Utils.isEmpty(this.keyspace)) {
            rep.saveStepAttribute(id_transformation, id_step, 0, CASSANDRA_KEYSPACE, this.keyspace);
        }

        rep.saveStepAttribute(id_transformation, id_step, 0, CASSANDRA_WITH_SSL, this.SslEnabled);

        if (!Utils.isEmpty(this.trustStoreFilePath)) {
            rep.saveStepAttribute(id_transformation, id_step, 0, CASSANDRA_TRUSTSTORE_FILE_PATH, this.trustStoreFilePath);
        }

        if (!Utils.isEmpty(this.trustStorePass)) {
            rep.saveStepAttribute(id_transformation, id_step, 0, CASSANDRA_TRUSTSTORE_PASS,
                    Encr.encryptPasswordIfNotUsingVariables(this.trustStorePass));
        }

        if (!Utils.isEmpty(this.columnfamily)) {
            rep.saveStepAttribute(id_transformation, id_step, 0, CASSANDRA_COLUMN_FAMILY, this.columnfamily);
        }

        rep.saveStepAttribute(id_transformation, id_step, 0, SYNC_ENABLED, this.syncMode);

        rep.saveStepAttribute(id_transformation, id_step, 0, BATCH_SIZE, this.batchSize);

        if (this.compression != null) {
            rep.saveStepAttribute(id_transformation, id_step, 0, QUERY_COMPRESSION, this.compression.toString());
        }

        rep.saveStepAttribute(id_transformation, id_step, 0, SPECIFY_FIELDS, this.specifyFields);

        if (this.specifyFields) {
            int nrRows = this.cassandraFields.length < this.streamFields.length ? this.streamFields.length :
                    this.cassandraFields.length;
            for (int idx = 0; idx < nrRows; idx++) {
                String columnName = idx < this.cassandraFields.length ? this.cassandraFields[idx] : "";
                String streamName = idx < this.streamFields.length ? this.streamFields[idx] : "";
                rep.saveStepAttribute(id_transformation, id_step, idx, COLUMN_NAME, columnName);
                rep.saveStepAttribute(id_transformation, id_step, idx, STREAM_NAME, streamName);
            }
        }

        rep.saveStepAttribute(id_transformation, id_step, 0, TTL, this.ttl);
    }

    @Override
    public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
                      RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                      Repository repository, IMetaStore metaStore) {
        CheckResult cr;

        if ((prev == null) || (prev.size() == 0)) {
            cr = new CheckResult(3, "Not receiving any fields from previous steps!", stepMeta);
            remarks.add(cr);
        } else {
            cr = new CheckResult(1,"Step is connected to previous one, receiving " + prev.size() + " fields", stepMeta);
            remarks.add(cr);
        }

        if (input.length > 0) {
            cr = new CheckResult(1, "Step is receiving info from other steps.", stepMeta);
            remarks.add(cr);
        } else {
            cr = new CheckResult(4, "No input received from other steps!", stepMeta);
            remarks.add(cr);
        }

        if ((this.SslEnabled) && ((Utils.isEmpty(this.trustStoreFilePath)) || (Utils.isEmpty(this.trustStorePass)))) {
            cr = new CheckResult(4,"SSL is enabled but the trust storefile or/and password is not entered", stepMeta);
            remarks.add(cr);
        }

        if (Utils.isEmpty(this.keyspace)) {
            cr = new CheckResult(4, "No keyspace specified!", stepMeta);
            remarks.add(cr);
        }

        if (Utils.isEmpty(this.columnfamily)) {
            cr = new CheckResult(4, "No column family (table) specified!", stepMeta);
            remarks.add(cr);
        }
    }

    @Override
    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans) {
        return new CassandraOutput(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    @Override
    public StepDataInterface getStepData() {
        return new CassandraOutputData();
    }

    @Override
    public void setDefault() {
        super.setDefault();

        this.syncMode = false;
        this.batchSize = 1000;
        this.specifyFields = false;
        this.streamFields = new String[0];
        this.cassandraFields = new String[0];
        this.ttl = 0;
    }

    @Override
    public String getDialogClassName() {
        return CassandraOutputDialog.class.getName();
    }

    public StepDialogInterface getDialog(Shell shell, StepMetaInterface meta, TransMeta transMeta, String name) {
        return new CassandraOutputDialog(shell, meta, transMeta, name);
    }

    @Override
    public boolean supportsErrorHandling() {
        return false;
    }
}
