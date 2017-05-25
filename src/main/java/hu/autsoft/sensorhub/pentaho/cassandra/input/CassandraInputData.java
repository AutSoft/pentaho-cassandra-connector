package hu.autsoft.sensorhub.pentaho.cassandra.input;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class CassandraInputData extends BaseStepData implements StepDataInterface {

    protected RowMetaInterface inputRowMeta;
    protected RowMetaInterface outputRowMeta;

    public RowMetaInterface getInputRowMeta() {
        return this.inputRowMeta;
    }

    public void setInputRowMeta(RowMetaInterface rmi) {
        this.inputRowMeta = rmi;
    }

    public RowMetaInterface getOutputRowMeta() {
        return this.outputRowMeta;
    }

    public void setOutputRowMeta(RowMetaInterface rmi) {
        this.outputRowMeta = rmi;
    }
}
