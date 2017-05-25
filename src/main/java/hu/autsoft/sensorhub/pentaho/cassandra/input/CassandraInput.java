package hu.autsoft.sensorhub.pentaho.cassandra.input;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import hu.autsoft.sensorhub.pentaho.cassandra.CassandraConnection;
import hu.autsoft.sensorhub.pentaho.cassandra.CassandraConnectionException;
import hu.autsoft.sensorhub.pentaho.cassandra.ConnectionCompression;
import hu.autsoft.sensorhub.pentaho.cassandra.Utils;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;


public class CassandraInput extends BaseStep implements StepInterface {
    private static Class<?> PKG = CassandraInput.class;

    private CassandraInputMeta meta;
    private CassandraInputData data;
    private CassandraConnection connection;

    private String[] rowColumns;

    private Boolean executeForEachInputRow = Boolean.FALSE;
    private int rowLimit;

    private String cqlStatement;

    public CassandraInput(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    @Override
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
        this.meta = ((CassandraInputMeta) smi);
        this.data = ((CassandraInputData) sdi);
        ResultSet rs = null;

        Object[] r = getRow();
        if ((this.executeForEachInputRow) && (r == null)) {
            setOutputDone();
            return false;
        }

        if (this.first) {
            this.first = false;

            this.data.outputRowMeta = new RowMeta();
            rs = this.connection.getSession().execute(this.cqlStatement);
            this.meta.createOutputRowMeta(this.data.outputRowMeta, rs);

            this.rowColumns = this.data.outputRowMeta.getFieldNames();
        }

        try {
            int rowCount = 0;
            do {
                Object[] outputRow = new Object[this.rowColumns.length];
                Row row = rs.one();
                incrementLinesInput();
                rowCount++;

                String[] arrayOfString;
                int j = (arrayOfString = this.rowColumns).length;

                for (int i = 0; i < j; i++) {
                    String col = arrayOfString[i];
                    Object o = row.getObject(col);

                    if ((o instanceof Integer)) {
                        o = (long) (Integer) o;
                    }

                    if (o instanceof Float) {
                        o = Double.valueOf((Float) o);
                    }

                    outputRow[i] = o;
                }

                putRow(this.data.outputRowMeta, outputRow);

                if ((isStopped()) || (rs.isExhausted())) break;

            } while ((this.rowLimit == 0) || (rowCount < this.rowLimit));
        } catch (KettleException e) {
            logError(BaseMessages.getString(PKG, "CassandraInputDialog.Error.StepCanNotContinueForErrors", e.getMessage()));
            logError(Const.getStackTracker(e));
            setErrors(1L);
            stopAll();
            setOutputDone();
            return false;
        }

        setOutputDone();
        return false;
    }

    @Override
    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        this.meta = ((CassandraInputMeta) smi);
        this.data = ((CassandraInputData) sdi);
        String nodes = environmentSubstitute(this.meta.getCassandraNodes());
        String port = environmentSubstitute(this.meta.getCassandraPort());
        String username = environmentSubstitute(this.meta.getUsername());
        String password = environmentSubstitute(this.meta.getPassword());
        String keyspace = environmentSubstitute(this.meta.getKeyspace());
        Boolean withSSL = this.meta.getSslEnabled();
        String trustStoreFilePath = environmentSubstitute(this.meta.getTrustStoreFilePath());
        String trustStorePass = environmentSubstitute(this.meta.getTrustStorePass());
        String compression = environmentSubstitute(this.meta.getCompression().name());
        this.executeForEachInputRow = this.meta.getExecuteForEachInputRow();
        this.rowLimit = this.meta.getRowLimit();
        this.cqlStatement = environmentSubstitute(this.meta.getCqlStatement());
        try {
            this.connection = Utils.connect(nodes, port, username, password, keyspace, withSSL,
                    trustStoreFilePath, trustStorePass, ConnectionCompression.fromString(compression));
        } catch (CassandraConnectionException e) {
            logError("Could initialize step: " + e.getMessage());
        }

        return super.init(this.meta, this.data);
    }

    @Override
    public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
        if (this.connection != null) {
            this.connection.release();
        }
        super.dispose(this.meta, this.data);
    }
}
