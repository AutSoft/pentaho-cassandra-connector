package hu.autsoft.sensorhub.pentaho.cassandra.output;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class CassandraOutputData extends BaseStepData implements StepDataInterface {
    protected RowMetaInterface m_outputRowMeta;

    public RowMetaInterface getOutputRowMeta() {
        return this.m_outputRowMeta;
    }

    public void setOutputRowMeta(RowMetaInterface rmi) {
        this.m_outputRowMeta = rmi;
    }
}

