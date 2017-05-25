package hu.autsoft.sensorhub.pentaho.cassandra.input;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import hu.autsoft.sensorhub.pentaho.cassandra.*;

import java.util.List;

import lombok.Getter;
import lombok.Setter;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

@Getter
@Setter
@Step(id = "DatastaxCassandra-3-Input", image = "DatastaxCassandraInput.svg", name = "AutSoftCassandraInput.StepName",
        i18nPackageName = "hu.autsoft.sensorhub.pentaho.cassandra.input.messages", description = "AutSoftCassandraInput.StepDescription",
        categoryDescription = "AutSoftCassandraInput.StepCategory",
        documentationUrl = "http://sensorhub.autsoft.hu/docs")
public class CassandraInputMeta extends AbstractCassandraMeta {
    private static final String CQL = "cql";
    private static final String EXECUTE_FOR_EACH_INPUT = "execute_for_each_input";
    private static final String ROW_LIMIT = "rowLimit";

    private String cqlStatement;
    private int rowLimit = 0;
    private Boolean executeForEachInputRow;

    @Override
    public String getXML() {
        StringBuilder retval = new StringBuilder();

        if (!org.pentaho.di.core.util.Utils.isEmpty(this.cassandraNodes)) {
            retval.append(FOUR_SPACES_FOR_XML).append(XMLHandler.addTagValue(CASSANDRA_NODES, this.cassandraNodes));
        }

        if (!org.pentaho.di.core.util.Utils.isEmpty(this.cassandraPort)) {
            retval.append(FOUR_SPACES_FOR_XML).append(XMLHandler.addTagValue(CASSANDRA_PORT, this.cassandraPort));
        }

        if (!org.pentaho.di.core.util.Utils.isEmpty(this.username)) {
            retval.append(FOUR_SPACES_FOR_XML).append(XMLHandler.addTagValue(USERNAME, this.username));
        }

        if (!org.pentaho.di.core.util.Utils.isEmpty(this.password)) {
            retval.append(FOUR_SPACES_FOR_XML).append(XMLHandler.addTagValue(PASSWORD, Encr.encryptPasswordIfNotUsingVariables(this.password)));
        }

        if (!org.pentaho.di.core.util.Utils.isEmpty(this.keyspace)) {
            retval.append(FOUR_SPACES_FOR_XML).append(XMLHandler.addTagValue(CASSANDRA_KEYSPACE, this.keyspace));
        }

        if (this.SslEnabled != null) {
            retval.append(FOUR_SPACES_FOR_XML).append(XMLHandler.addTagValue(CASSANDRA_WITH_SSL, this.SslEnabled ? "Y" : "N"));
        }

        if (!org.pentaho.di.core.util.Utils.isEmpty(this.trustStoreFilePath)) {
            retval.append(FOUR_SPACES_FOR_XML).append(XMLHandler.addTagValue(CASSANDRA_TRUSTSTORE_FILE_PATH, this.trustStoreFilePath));
        }

        if (!org.pentaho.di.core.util.Utils.isEmpty(this.trustStorePass)) {
            retval.append(FOUR_SPACES_FOR_XML).append(XMLHandler.addTagValue(CASSANDRA_TRUSTSTORE_PASS, Encr.encryptPasswordIfNotUsingVariables(this.trustStorePass)));
        }

        if (this.compression != null) {
            retval.append(FOUR_SPACES_FOR_XML).append(XMLHandler.addTagValue(COMPRESSION, this.compression.toString()));
        }

        if (!org.pentaho.di.core.util.Utils.isEmpty(this.cqlStatement)) {
            retval.append(FOUR_SPACES_FOR_XML).append(XMLHandler.addTagValue(CQL, this.cqlStatement));
        }

        if (this.executeForEachInputRow != null) {
            retval.append(FOUR_SPACES_FOR_XML).append(XMLHandler.addTagValue(EXECUTE_FOR_EACH_INPUT, this.executeForEachInputRow));
        }

        if (this.rowLimit > 0) {
            retval.append(FOUR_SPACES_FOR_XML).append(XMLHandler.addTagValue(ROW_LIMIT, this.rowLimit));
        }

        return retval.toString();
    }

    @Override
    public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metastore) throws KettleXMLException {
        this.cassandraNodes = XMLHandler.getTagValue(stepnode, CASSANDRA_NODES);
        this.cassandraPort = XMLHandler.getTagValue(stepnode, CASSANDRA_PORT);
        this.username = XMLHandler.getTagValue(stepnode, USERNAME);
        this.password = XMLHandler.getTagValue(stepnode, PASSWORD);
        if (!org.pentaho.di.core.util.Utils.isEmpty(this.password)) {
            this.password = Encr.decryptPasswordOptionallyEncrypted(this.password);
        }
        this.keyspace = XMLHandler.getTagValue(stepnode, CASSANDRA_KEYSPACE);
        this.SslEnabled = "Y".equals(XMLHandler.getTagValue(stepnode, CASSANDRA_WITH_SSL));
        this.trustStoreFilePath = XMLHandler.getTagValue(stepnode, CASSANDRA_TRUSTSTORE_FILE_PATH);
        this.trustStorePass = XMLHandler.getTagValue(stepnode, CASSANDRA_TRUSTSTORE_PASS);
        String sCompression = XMLHandler.getTagValue(stepnode, COMPRESSION);
        this.cqlStatement = XMLHandler.getTagValue(stepnode, CQL);
        this.executeForEachInputRow = "Y".equals(XMLHandler.getTagValue(stepnode, EXECUTE_FOR_EACH_INPUT));
        String sRowLimit = XMLHandler.getTagValue(stepnode, ROW_LIMIT);

        checkNulls();

        if (this.cqlStatement == null) {
            this.cqlStatement = "";
        }
        if (!org.pentaho.di.core.util.Utils.isEmpty(sRowLimit)) {
            this.rowLimit = Integer.parseInt(sRowLimit);
        }
        this.compression = (org.pentaho.di.core.util.Utils.isEmpty(sCompression) ? ConnectionCompression.SNAPPY : ConnectionCompression.fromString(sCompression));
    }

    @Override
    public void readRep(Repository rep, IMetaStore metastore, ObjectId id_step, List<DatabaseMeta> databases)
            throws KettleException {
        this.cassandraNodes = rep.getStepAttributeString(id_step, 0, CASSANDRA_NODES);
        this.cassandraPort = rep.getStepAttributeString(id_step, 0, CASSANDRA_PORT);
        this.username = rep.getStepAttributeString(id_step, 0, USERNAME);
        this.password = rep.getStepAttributeString(id_step, 0, PASSWORD);
        if (!org.pentaho.di.core.util.Utils.isEmpty(this.password)) {
            this.password = Encr.decryptPasswordOptionallyEncrypted(this.password);
        }
        this.keyspace = rep.getStepAttributeString(id_step, 0, CASSANDRA_KEYSPACE);
        this.SslEnabled = rep.getStepAttributeBoolean(id_step, CASSANDRA_WITH_SSL);
        this.trustStoreFilePath = rep.getStepAttributeString(id_step, 0, CASSANDRA_TRUSTSTORE_FILE_PATH);
        this.trustStorePass = rep.getStepAttributeString(id_step, 0, CASSANDRA_TRUSTSTORE_PASS);
        String sCompression = rep.getStepAttributeString(id_step, 0, COMPRESSION);
        this.cqlStatement = rep.getStepAttributeString(id_step, 0, CQL);
        this.executeForEachInputRow = rep.getStepAttributeBoolean(id_step, EXECUTE_FOR_EACH_INPUT);
        this.rowLimit = ((int) rep.getStepAttributeInteger(id_step, ROW_LIMIT));

        if (!org.pentaho.di.core.util.Utils.isEmpty(this.trustStorePass)) {
            this.trustStorePass = Encr.decryptPasswordOptionallyEncrypted(this.trustStorePass);
        }

        checkNulls();

        if (this.cqlStatement == null) {
            this.cqlStatement = "";
        }
        this.compression = (org.pentaho.di.core.util.Utils.isEmpty(sCompression) ? ConnectionCompression.SNAPPY : ConnectionCompression.fromString(sCompression));
    }

    @Override
    public void saveRep(Repository rep, IMetaStore metastore, ObjectId id_transformation, ObjectId id_step) throws KettleException {
        if (!org.pentaho.di.core.util.Utils.isEmpty(this.cassandraNodes)) {
            rep.saveStepAttribute(id_transformation, id_step, 0, CASSANDRA_NODES, this.cassandraNodes);
        }

        if (!org.pentaho.di.core.util.Utils.isEmpty(this.cassandraPort)) {
            rep.saveStepAttribute(id_transformation, id_step, 0, CASSANDRA_PORT, this.cassandraPort);
        }

        if (!org.pentaho.di.core.util.Utils.isEmpty(this.username)) {
            rep.saveStepAttribute(id_transformation, id_step, 0, USERNAME, this.username);
        }

        if (!org.pentaho.di.core.util.Utils.isEmpty(this.password)) {
            rep.saveStepAttribute(id_transformation, id_step, 0, PASSWORD, Encr.encryptPasswordIfNotUsingVariables(this.password));
        }

        if (!org.pentaho.di.core.util.Utils.isEmpty(this.keyspace)) {
            rep.saveStepAttribute(id_transformation, id_step, 0, CASSANDRA_KEYSPACE, this.keyspace);
        }

        if (this.SslEnabled != null) {
            rep.saveStepAttribute(id_transformation, id_step, 0, CASSANDRA_WITH_SSL, this.SslEnabled);
        }

        if (!org.pentaho.di.core.util.Utils.isEmpty(this.trustStoreFilePath)) {
            rep.saveStepAttribute(id_transformation, id_step, 0, CASSANDRA_TRUSTSTORE_FILE_PATH, this.trustStoreFilePath);
        }

        if (!org.pentaho.di.core.util.Utils.isEmpty(this.trustStorePass)) {
            rep.saveStepAttribute(id_transformation, id_step, 0, CASSANDRA_TRUSTSTORE_PASS,
                    Encr.encryptPasswordIfNotUsingVariables(this.trustStorePass));
        }

        if (this.compression != null) {
            rep.saveStepAttribute(id_transformation, id_step, 0, COMPRESSION, this.compression.toString());
        }

        if (!org.pentaho.di.core.util.Utils.isEmpty(this.cqlStatement)) {
            rep.saveStepAttribute(id_transformation, id_step, 0, CQL, this.cqlStatement);
        }

        if (this.executeForEachInputRow != null) {
            rep.saveStepAttribute(id_transformation, id_step, 0, EXECUTE_FOR_EACH_INPUT, this.executeForEachInputRow);
        }

        rep.saveStepAttribute(id_transformation, id_step, 0, ROW_LIMIT, this.rowLimit);
    }

    @Override
    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans) {
        return new CassandraInput(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    @Override
    public StepDataInterface getStepData() {
        return new CassandraInputData();
    }

    @Override
    public String getDialogClassName() {
        return CassandraInputDialog.class.getName();
    }

    public StepDialogInterface getDialog(Shell shell, StepMetaInterface meta, TransMeta transMeta, String name) {
        return new CassandraInputDialog(shell, meta, transMeta, name);
    }

    @Override
    public void setDefault() {
        super.setDefault();

        this.executeForEachInputRow = Boolean.FALSE;
        this.cqlStatement = "select <fields> from <column family> where <condition>;";
        this.rowLimit = 0;
    }

    @Override
    public void getFields(RowMetaInterface row, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space, Repository repository, IMetaStore metaStore)
            throws KettleStepException {
        CassandraConnection connection;
        String nodes = space.environmentSubstitute(this.cassandraNodes);
        String port = space.environmentSubstitute(this.cassandraPort);
        String keyspace = space.environmentSubstitute(this.keyspace);
        String cql = space.environmentSubstitute(this.cqlStatement);

        if ((org.pentaho.di.core.util.Utils.isEmpty(nodes)) || (org.pentaho.di.core.util.Utils.isEmpty(port)) || (org.pentaho.di.core.util.Utils.isEmpty(keyspace)) || (org.pentaho.di.core.util.Utils.isEmpty(cql))) {
            return;
        }

        String user = space.environmentSubstitute(this.username);
        String pass = space.environmentSubstitute(this.password);
        String trustfile = space.environmentSubstitute(this.trustStoreFilePath);
        String trustpass = space.environmentSubstitute(this.trustStorePass);


        logDebug("meta: opening connection");
        try {
            connection = Utils.connect(nodes, port, user, pass, keyspace, this.SslEnabled, trustfile, trustpass, this.compression);
        } catch (CassandraConnectionException e) {
            throw new KettleStepException("Failed to create connection", e);
        }

        Session session = connection.getSession();

        logDebug("meta: parsing cql '" + cql + "'");
        ResultSet rs = session.execute(cql);
        createOutputRowMeta(row, rs);

        connection.release();
    }

    void createOutputRowMeta(RowMetaInterface row, ResultSet rs) {
        row.clear();

        for (ColumnDefinitions.Definition d : rs.getColumnDefinitions()) {
            logDebug(d.getName() + ',' + d.getType().getName() + ',' + d.getType().asFunctionParameterString());

            ValueMetaBase valueMeta = new ValueMetaBase(d.getName(), Utils.convertDataType(d.getType()));
            valueMeta.setTrimType(0);
            row.addValueMeta(valueMeta);
        }
    }
}
