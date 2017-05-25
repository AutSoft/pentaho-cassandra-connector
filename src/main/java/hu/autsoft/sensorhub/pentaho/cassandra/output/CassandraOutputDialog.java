package hu.autsoft.sensorhub.pentaho.cassandra.output;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;

import hu.autsoft.sensorhub.pentaho.cassandra.CassandraConnection;
import hu.autsoft.sensorhub.pentaho.cassandra.CommonDialog;
import hu.autsoft.sensorhub.pentaho.cassandra.ConnectionCompression;
import hu.autsoft.sensorhub.pentaho.cassandra.Utils;

import java.util.*;

import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;


public class CassandraOutputDialog extends BaseStepDialog implements StepDialogInterface {
    private static final Class<?> PKG = CassandraOutputMeta.class;

    private final CassandraOutputMeta currentMeta;
    private final CassandraOutputMeta originalMeta;

    private Text stepnameText;
    private CTabItem writeTab;
    private TextVar hostText;
    private TextVar portText;
    private TextVar userText;
    private TextVar passText;
    private TextVar keyspaceText;
    private CCombo columnFamilyCombo;
    private Button sslenabledBut;
    private Button syncModeEnabledBut;
    private TextVar truststorefileText;
    private TextVar truststorepassText;
    private TextVar batchSizeText;
    private CCombo wCompression;
    private Button specifyFieldsBut;
    private TableView fieldsList;
    private Button getFieldsBut;
    private ColumnInfo[] ciFields;
    private final Map<String, Integer> inputFields = new HashMap<>();
    private TextVar ttlText;
    private CassandraConnection connection = null;

    public CassandraOutputDialog(Shell parent, Object in, TransMeta tr, String name) {
        super(parent, (BaseStepMeta) in, tr, name);

        this.currentMeta = ((CassandraOutputMeta) in);
        this.originalMeta = ((CassandraOutputMeta) this.currentMeta.clone());
    }

    @Override
    public String open() {
        Shell parent = getParent();
        Display display = parent.getDisplay();

        this.shell = new Shell(parent, 3312);

        this.props.setLook(this.shell);
        setShellImage(this.shell, this.currentMeta);

        ModifyListener lsMod = e -> CassandraOutputDialog.this.currentMeta.setChanged();
        this.changed = this.currentMeta.hasChanged();

        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = 5;
        formLayout.marginHeight = 5;

        this.shell.setLayout(formLayout);
        this.shell.setText(this.stepname);
        this.props.setLook(this.shell);

        int middle = this.props.getMiddlePct();
        int margin = 4;

        Label stepnameLabel = new Label(this.shell, 131072);
        stepnameLabel.setText(BaseMessages.getString(PKG, "System.Label.StepName"));
        this.props.setLook(stepnameLabel);

        FormData fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.right = new FormAttachment(middle, -margin);
        fd.top = new FormAttachment(0, margin);
        stepnameLabel.setLayoutData(fd);
        this.stepnameText = new Text(this.shell, 18436);
        this.stepnameText.setText(this.stepname);
        this.props.setLook(this.stepnameText);
        this.stepnameText.addModifyListener(lsMod);

        fd = new FormData();
        fd.left = new FormAttachment(middle, 0);
        fd.top = new FormAttachment(0, margin);
        fd.right = new FormAttachment(100, 0);
        this.stepnameText.setLayoutData(fd);

        CTabFolder wTabFolder = new CTabFolder(this.shell, 2048);
        this.props.setLook(wTabFolder, 5);
        wTabFolder.setSimple(false);

        CTabItem connectionTab = new CTabItem(wTabFolder, 2048);
        connectionTab.setText(BaseMessages.getString(PKG, "AutSoftCassandraOutputDialog.Tab.Connection"));

        Composite wConnectionComp = new Composite(wTabFolder, 0);
        this.props.setLook(wConnectionComp);
        wConnectionComp.setLayout(formLayout);

        Label hostLab = new Label(wConnectionComp, 131072);
        this.props.setLook(hostLab);
        hostLab.setText(BaseMessages.getString(PKG, "AutSoftCassandraOutputDialog.Hostname.Label"));
        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.top = new FormAttachment(0, margin);
        fd.right = new FormAttachment(middle, -margin);
        hostLab.setLayoutData(fd);

        this.hostText = new TextVar(this.transMeta, wConnectionComp, 18436);
        this.props.setLook(this.hostText);

        this.hostText.addModifyListener(e -> CassandraOutputDialog.this.hostText.setToolTipText(CassandraOutputDialog.this.transMeta.environmentSubstitute(CassandraOutputDialog.this.hostText.getText())));
        this.hostText.addModifyListener(lsMod);
        fd = new FormData();
        fd.left = new FormAttachment(middle, 0);
        fd.top = new FormAttachment(0, margin);
        fd.right = new FormAttachment(100, 0);
        this.hostText.setLayoutData(fd);

        Label portLab = new Label(wConnectionComp, 131072);
        this.props.setLook(portLab);
        portLab.setText(BaseMessages.getString(PKG, "AutSoftCassandraOutputDialog.Port.Label"));
        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.top = new FormAttachment(this.hostText, margin);
        fd.right = new FormAttachment(middle, -margin);
        portLab.setLayoutData(fd);

        this.portText = new TextVar(this.transMeta, wConnectionComp, 18436);
        this.props.setLook(this.portText);

        this.portText.addModifyListener(e -> CassandraOutputDialog.this.portText.setToolTipText(CassandraOutputDialog.this.transMeta.environmentSubstitute(CassandraOutputDialog.this.portText.getText())));
        this.portText.addModifyListener(lsMod);
        fd = new FormData();
        fd.left = new FormAttachment(middle, 0);
        fd.top = new FormAttachment(this.hostText, margin);
        fd.right = new FormAttachment(100, 0);
        this.portText.setLayoutData(fd);

        Label userLab = new Label(wConnectionComp, 131072);
        this.props.setLook(userLab);
        userLab.setText(BaseMessages.getString(PKG, "AutSoftCassandraOutputDialog.User.Label"));
        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.top = new FormAttachment(this.portText, margin);
        fd.right = new FormAttachment(middle, -margin);
        userLab.setLayoutData(fd);

        this.userText = new TextVar(this.transMeta, wConnectionComp, 18436);
        this.props.setLook(this.userText);
        this.userText.addModifyListener(lsMod);
        fd = new FormData();
        fd.left = new FormAttachment(middle, 0);
        fd.top = new FormAttachment(this.portText, margin);
        fd.right = new FormAttachment(100, 0);
        this.userText.setLayoutData(fd);

        Label passLab = new Label(wConnectionComp, 4325376);
        this.props.setLook(passLab);
        passLab.setText(BaseMessages.getString(PKG, "AutSoftCassandraOutputDialog.Password.Label"));
        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.top = new FormAttachment(this.userText, margin);
        fd.right = new FormAttachment(middle, -margin);
        passLab.setLayoutData(fd);

        this.passText = new TextVar(this.transMeta, wConnectionComp, 18436);
        this.props.setLook(this.passText);
        this.passText.addModifyListener(lsMod);

        fd = new FormData();
        fd.left = new FormAttachment(middle, 0);
        fd.top = new FormAttachment(this.userText, margin);
        fd.right = new FormAttachment(100, 0);
        this.passText.setLayoutData(fd);

        Label keyspaceLab = new Label(wConnectionComp, 131072);
        this.props.setLook(keyspaceLab);
        keyspaceLab.setText(BaseMessages.getString(PKG, "AutSoftCassandraOutputDialog.Keyspace.Label"));
        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.top = new FormAttachment(this.passText, margin);
        fd.right = new FormAttachment(middle, -margin);
        keyspaceLab.setLayoutData(fd);

        this.keyspaceText = new TextVar(this.transMeta, wConnectionComp, 18436);
        this.props.setLook(this.keyspaceText);

        this.keyspaceText.addModifyListener(e -> CassandraOutputDialog.this.keyspaceText.setToolTipText(CassandraOutputDialog.this.transMeta.environmentSubstitute(CassandraOutputDialog.this.keyspaceText.getText())));
        this.keyspaceText.addModifyListener(lsMod);
        fd = new FormData();
        fd.left = new FormAttachment(middle, 0);
        fd.top = new FormAttachment(this.passText, margin);
        fd.right = new FormAttachment(100, 0);
        this.keyspaceText.setLayoutData(fd);

        Label sslenabledLab = new Label(wConnectionComp, 131072);
        sslenabledLab.setText(BaseMessages.getString(PKG, "AutSoftCassandraOutputDialog.SSLEnabled.Label"));
        sslenabledLab.setToolTipText(BaseMessages.getString(PKG, "AutSoftCassandraOutputDialog.SSLEnabled.TipText"));
        this.props.setLook(sslenabledLab);
        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.top = new FormAttachment(this.keyspaceText, margin);
        fd.right = new FormAttachment(middle, -margin);
        sslenabledLab.setLayoutData(fd);

        this.sslenabledBut = new Button(wConnectionComp, 32);
        this.props.setLook(this.sslenabledBut);
        fd = new FormData();
        fd.left = new FormAttachment(middle, 0);
        fd.top = new FormAttachment(this.keyspaceText, margin);
        fd.right = new FormAttachment(100, 0);
        this.sslenabledBut.setLayoutData(fd);

        Label truststorefileLab = new Label(wConnectionComp, 131072);
        this.props.setLook(truststorefileLab);
        truststorefileLab.setText(BaseMessages.getString(PKG, "AutSoftCassandraOutputDialog.TrustStoreFile.Label"));
        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.top = new FormAttachment(this.sslenabledBut, margin);
        fd.right = new FormAttachment(middle, -margin);
        truststorefileLab.setLayoutData(fd);

        this.truststorefileText = new TextVar(this.transMeta, wConnectionComp, 18436);
        this.props.setLook(this.truststorefileText);
        this.truststorefileText.addModifyListener(lsMod);
        fd = new FormData();
        fd.left = new FormAttachment(middle, 0);
        fd.top = new FormAttachment(this.sslenabledBut, margin);
        fd.right = new FormAttachment(100, 0);
        this.truststorefileText.setLayoutData(fd);

        Label truststorepassLab = new Label(wConnectionComp, 4325376);
        this.props.setLook(truststorepassLab);
        truststorepassLab.setText(BaseMessages.getString(PKG, "AutSoftCassandraOutputDialog.TrustStorePassword.Label"));
        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.top = new FormAttachment(this.truststorefileText, margin);
        fd.right = new FormAttachment(middle, -margin);
        truststorepassLab.setLayoutData(fd);

        this.truststorepassText = new TextVar(this.transMeta, wConnectionComp, 18436);
        this.props.setLook(this.truststorepassText);
        this.truststorepassText.addModifyListener(lsMod);
        fd = new FormData();
        fd.left = new FormAttachment(middle, 0);
        fd.top = new FormAttachment(this.truststorefileText, margin);
        fd.right = new FormAttachment(100, 0);
        this.truststorepassText.setLayoutData(fd);

        Label syncModeEnabledLab = new Label(wConnectionComp, 131072);
        syncModeEnabledLab.setText(BaseMessages.getString(PKG, "AutSoftCassandraOutputDialog.SyncModeEnabled.Label"));
        syncModeEnabledLab.setToolTipText(BaseMessages.getString(PKG, "AutSoftCassandraOutputDialog.SyncModeEnabled.TipText"));
        this.props.setLook(syncModeEnabledLab);
        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.top = new FormAttachment(this.truststorepassText, margin);
        fd.right = new FormAttachment(middle, -margin);
        syncModeEnabledLab.setLayoutData(fd);

        this.syncModeEnabledBut = new Button(wConnectionComp, 32);
        this.props.setLook(this.syncModeEnabledBut);
        fd = new FormData();
        fd.left = new FormAttachment(middle, 0);
        fd.top = new FormAttachment(this.truststorepassText, margin);
        fd.right = new FormAttachment(100, 0);
        this.syncModeEnabledBut.setLayoutData(fd);

        Label batchSizeLab = new Label(wConnectionComp, 131072);
        batchSizeLab.setText(BaseMessages.getString(PKG, "AutSoftCassandraOutputDialog.BatchSize.Label"));
        batchSizeLab.setToolTipText(BaseMessages.getString(PKG, "AutSoftCassandraOutputDialog.BatchSize.TipText"));
        this.props.setLook(batchSizeLab);
        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.top = new FormAttachment(this.syncModeEnabledBut, margin);
        fd.right = new FormAttachment(middle, -margin);
        batchSizeLab.setLayoutData(fd);

        this.batchSizeText = new TextVar(this.transMeta, wConnectionComp, 18436);
        this.batchSizeText.addModifyListener(lsMod);
        this.props.setLook(this.batchSizeText);
        fd = new FormData();
        fd.left = new FormAttachment(middle, 0);
        fd.top = new FormAttachment(this.syncModeEnabledBut, margin);
        fd.right = new FormAttachment(100, 0);
        this.batchSizeText.setLayoutData(fd);

        Label useCompressionLab = new Label(wConnectionComp, 131072);
        useCompressionLab.setText(BaseMessages.getString(PKG, "AutSoftCassandraOutputDialog.UseCompression.Label"));
        useCompressionLab.setToolTipText(BaseMessages.getString(PKG, "AutSoftCassandraOutputDialog.UseCompression.TipText"));
        this.props.setLook(useCompressionLab);
        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.top = new FormAttachment(this.batchSizeText, margin);
        fd.right = new FormAttachment(middle, -margin);
        useCompressionLab.setLayoutData(fd);

        this.wCompression = new CCombo(wConnectionComp, 2048);

        this.wCompression.add(ConnectionCompression.NONE.getText());
        this.wCompression.add(ConnectionCompression.SNAPPY.getText());
        this.wCompression.add(ConnectionCompression.PIEDPIPER.getText() + " (Coming soon)");
        this.wCompression.setEditable(false);
        this.wCompression.addModifyListener(lsMod);
        this.props.setLook(this.wCompression);

        this.wCompression.addModifyListener(e -> CommonDialog.setCompressionTooltips(wCompression, CassandraOutputDialog.PKG));

        this.wCompression.addModifyListener(lsMod);
        fd = new FormData();
        fd.left = new FormAttachment(middle, 0);
        fd.top = new FormAttachment(this.batchSizeText, margin);

        this.wCompression.setLayoutData(fd);

        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.top = new FormAttachment(0, 0);
        fd.right = new FormAttachment(100, 0);
        wConnectionComp.setLayoutData(fd);

        wConnectionComp.layout();
        connectionTab.setControl(wConnectionComp);

        this.writeTab = new CTabItem(wTabFolder, 2048);
        this.writeTab.setText(BaseMessages.getString(PKG, "AutSoftCassandraOutputDialog.Tab.WriteOptions"));

        Composite wWriteComp = new Composite(wTabFolder, 0);
        this.props.setLook(wWriteComp);
        wWriteComp.setLayout(formLayout);

        Label columnFamilyLab = new Label(wWriteComp, 131072);
        this.props.setLook(columnFamilyLab);
        columnFamilyLab.setText(BaseMessages.getString(PKG, "AutSoftCassandraOutputDialog.ColumnFamily.Label"));
        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.top = new FormAttachment(0, margin);
        fd.right = new FormAttachment(middle, -margin);
        columnFamilyLab.setLayoutData(fd);

        Button getColumnFamiliesBut = new Button(wWriteComp, 16777224);
        this.props.setLook(getColumnFamiliesBut);
        getColumnFamiliesBut.setText(BaseMessages.getString(PKG, "AutSoftCassandraOutputDialog.GetColFam.Button"));
        fd = new FormData();
        fd.top = new FormAttachment(0, -margin);
        fd.right = new FormAttachment(100, 0);
        getColumnFamiliesBut.setLayoutData(fd);
        getColumnFamiliesBut.addSelectionListener(new SelectionAdapter() {
            public void widgetSelected(SelectionEvent e) {
                CassandraOutputDialog.this.setupColumnFamiliesCombo();
            }
        });

        this.columnFamilyCombo = new CCombo(wWriteComp, 2048);
        this.props.setLook(this.columnFamilyCombo);
        this.columnFamilyCombo.addModifyListener(lsMod);
        fd = new FormData();
        fd.left = new FormAttachment(middle, 0);
        fd.top = new FormAttachment(0, margin);
        fd.right = new FormAttachment(getColumnFamiliesBut, -margin);
        this.columnFamilyCombo.setLayoutData(fd);

        Label ttlLab = new Label(wWriteComp, 131072);
        ttlLab.setText(BaseMessages.getString(PKG, "AutSoftCassandraOutputDialog.TTL.Label"));
        this.props.setLook(ttlLab);
        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.top = new FormAttachment(this.columnFamilyCombo, margin);
        fd.right = new FormAttachment(middle, -margin);
        ttlLab.setLayoutData(fd);

        this.ttlText = new TextVar(this.transMeta, wWriteComp, 18436);
        this.ttlText.setToolTipText(BaseMessages.getString(PKG, "AutSoftCassandraOutputDialog.TTL.TipText"));
        this.ttlText.addModifyListener(lsMod);
        this.props.setLook(this.ttlText);
        fd = new FormData();
        fd.left = new FormAttachment(middle, 0);
        fd.top = new FormAttachment(this.columnFamilyCombo, margin);
        fd.right = new FormAttachment(100, 0);
        this.ttlText.setLayoutData(fd);


        Label specifyFieldsLab = new Label(wWriteComp, 131072);
        specifyFieldsLab.setText(BaseMessages.getString(PKG, "AutSoftCassandraOutputDialog.SpecifyFields.Label"));
        this.props.setLook(specifyFieldsLab);
        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.top = new FormAttachment(this.ttlText, margin);
        fd.right = new FormAttachment(middle, -margin);
        specifyFieldsLab.setLayoutData(fd);

        this.specifyFieldsBut = new Button(wWriteComp, 32);
        this.props.setLook(this.specifyFieldsBut);
        fd = new FormData();
        fd.left = new FormAttachment(middle, 0);
        fd.top = new FormAttachment(this.ttlText, margin);
        fd.right = new FormAttachment(100, 0);
        this.specifyFieldsBut.setLayoutData(fd);

        this.specifyFieldsBut.addSelectionListener(new SelectionAdapter() {
            public void widgetSelected(SelectionEvent arg0) {
                CassandraOutputDialog.this.setFlags();
            }
        });

        Label fieldsLabel = new Label(wWriteComp, 131072);
        fieldsLabel.setText(BaseMessages.getString(PKG, "AutSoftCassandraOutputDialog.OutputFields.Label"));
        this.props.setLook(fieldsLabel);
        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.top = new FormAttachment(this.specifyFieldsBut, margin);
        fd.right = new FormAttachment(middle, -margin);
        fieldsLabel.setLayoutData(fd);

        this.getFieldsBut = new Button(wWriteComp, 8);
        this.getFieldsBut.setText(BaseMessages.getString(PKG, "AutSoftCassandraOutputDialog.GetFields.Button"));
        fd = new FormData();
        fd.right = new FormAttachment(100, 0);
        fd.top = new FormAttachment(this.specifyFieldsBut, margin);
        this.getFieldsBut.setLayoutData(fd);

        int tableCols = 2;
        int UpInsRows = this.currentMeta.getStreamFields() != null ? this.currentMeta.getStreamFields().length : 1;
        this.ciFields = new ColumnInfo[tableCols];
        this.ciFields[0] = new ColumnInfo(BaseMessages.getString(PKG, "AutSoftCassandraOutputDialog.ColumnInfo.TableField"),
                2, new String[]{""}, false);
        this.ciFields[1] = new ColumnInfo(BaseMessages.getString(PKG, "AutSoftCassandraOutputDialog.ColumnInfo.StreamField"),
                2, new String[]{""}, false);
        this.fieldsList = new TableView(this.transMeta, wWriteComp,68354, this.ciFields, UpInsRows, lsMod, this.props);

        fd = new FormData();
        fd.left = new FormAttachment(middle, 0);
        fd.top = new FormAttachment(this.specifyFieldsBut, margin);
        fd.right = new FormAttachment(this.getFieldsBut, -margin);
        fd.bottom = new FormAttachment(100, -margin);
        this.fieldsList.setLayoutData(fd);

        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.top = new FormAttachment(0, 0);
        fd.right = new FormAttachment(100, 0);
        wWriteComp.setLayoutData(fd);

        wWriteComp.layout();
        this.writeTab.setControl(wWriteComp);

        this.wOK = new Button(this.shell, 8);
        this.wOK.setText(BaseMessages.getString(PKG, "System.Button.OK"));

        this.wCancel = new Button(this.shell, 8);
        this.wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

        setButtonPositions(new Button[]{this.wOK, this.wCancel}, margin, null);

        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.top = new FormAttachment(this.stepnameText, margin);
        fd.right = new FormAttachment(100, 0);
        fd.bottom = new FormAttachment(this.wOK, -margin);
        wTabFolder.setLayoutData(fd);

        this.lsCancel = e -> CassandraOutputDialog.this.cancel();
        this.lsOK = e -> CassandraOutputDialog.this.ok();
        this.lsGet = e -> CassandraOutputDialog.this.get();
        this.wCancel.addListener(13, this.lsCancel);
        this.wOK.addListener(13, this.lsOK);
        this.getFieldsBut.addListener(13, this.lsGet);

        this.lsDef = new SelectionAdapter() {
            public void widgetDefaultSelected(SelectionEvent e) {
                CassandraOutputDialog.this.ok();
            }
        };
        
        this.stepnameText.addSelectionListener(this.lsDef);
        
        this.shell.addShellListener(new ShellAdapter() {
            public void shellClosed(ShellEvent e) {
                CassandraOutputDialog.this.cancel();
            }
        });
        
        wTabFolder.addSelectionListener(new SelectionListener() {
            public void widgetSelected(SelectionEvent arg0) {
                if (CassandraOutputDialog.this.writeTab.equals(arg0.item)) {
                    CassandraOutputDialog.this.setStreamFieldCombo();
                    CassandraOutputDialog.this.setTableFieldCombo();
                }
            }

            public void widgetDefaultSelected(SelectionEvent arg0) {}
        });
        
        wTabFolder.setSelection(0);
        this.stepnameText.setSelection(0);
        setSize();

        getData();

        this.columnFamilyCombo.addModifyListener(e -> {
            CassandraOutputDialog.this.columnFamilyCombo.setToolTipText(CassandraOutputDialog.this.transMeta.environmentSubstitute(CassandraOutputDialog.this.columnFamilyCombo.getText()));
            CassandraOutputDialog.this.setTableFieldCombo();
        });
        
        this.shell.open();
        while (!this.shell.isDisposed()) {
            if (!display.readAndDispatch()) {
                display.sleep();
            }
        }

        return this.stepname;
    }

    @Override
    public void dispose() {
        if (this.connection != null) {
            this.connection.release();
        }
        super.dispose();
    }

    private void setupColumnFamiliesCombo() {
        String nodes = this.transMeta.environmentSubstitute(this.hostText.getText());
        String port_s = this.transMeta.environmentSubstitute(this.portText.getText());
        String username = this.transMeta.environmentSubstitute(this.userText.getText());
        String password = this.transMeta.environmentSubstitute(this.passText.getText());
        String keyspace = this.transMeta.environmentSubstitute(this.keyspaceText.getText());
        Boolean withSSL = this.sslenabledBut.getSelection();
        String truststorefile = this.transMeta.environmentSubstitute(this.truststorefileText.getText());
        String truststorepass = this.transMeta.environmentSubstitute(this.truststorepassText.getText());
        ConnectionCompression compression = ConnectionCompression.fromString(this.wCompression.getText());
        Cluster cluster;
        try {
            try {
                this.connection = Utils.connect(nodes, port_s, username, password, keyspace, withSSL, truststorefile, truststorepass, compression);
                cluster = this.connection.getSession().getCluster();
                Collection<TableMetadata> colFams = cluster.getMetadata().getKeyspace(this.transMeta.environmentSubstitute(this.keyspaceText.getText())).getTables();
                this.columnFamilyCombo.removeAll();
                for (TableMetadata row : colFams) {
                    this.columnFamilyCombo.add(row.getName());
                }
            }
            catch (Exception ex) {
                this.logError(String.valueOf(BaseMessages.getString(PKG, "AutSoftCassandraOutputDialog.Error.ProblemGettingSchemaInfo.Message", (String[])new String[0])) + ":\n\n" + ex.getMessage(), ex);
                new org.pentaho.di.ui.core.dialog.ErrorDialog(this.shell, BaseMessages.getString(PKG, "AutSoftCassandraOutputDialog.Error.ProblemGettingSchemaInfo.Title", (String[])new String[0]), String.valueOf(BaseMessages.getString(PKG, "AutSoftCassandraOutputDialog.Error.ProblemGettingSchemaInfo.Message", (String[])new String[0])) + ":\n\n" + ex.getMessage(), ex);
                if (this.connection != null) {
                    this.connection.release();
                }
            }
        }
        finally {
            if (this.connection != null) {
                this.connection.release();
            }
        }
    }

    private void setTableFieldCombo() {

        Runnable fieldLoader = () -> {
            if (!CassandraOutputDialog.this.shell.isDisposed()) {
                String nodes = CassandraOutputDialog.this.transMeta.environmentSubstitute(CassandraOutputDialog.this.hostText.getText());
                String port_s = CassandraOutputDialog.this.transMeta.environmentSubstitute(CassandraOutputDialog.this.portText.getText());
                String username = CassandraOutputDialog.this.transMeta.environmentSubstitute(CassandraOutputDialog.this.userText.getText());
                String password = CassandraOutputDialog.this.transMeta.environmentSubstitute(CassandraOutputDialog.this.passText.getText());
                String keyspace = CassandraOutputDialog.this.transMeta.environmentSubstitute(CassandraOutputDialog.this.keyspaceText.getText());
                Boolean withSSL = CassandraOutputDialog.this.sslenabledBut.getSelection();
                String truststorefile = CassandraOutputDialog.this.transMeta.environmentSubstitute(CassandraOutputDialog.this.truststorefileText.getText());
                String truststorepass = CassandraOutputDialog.this.transMeta.environmentSubstitute(CassandraOutputDialog.this.truststorepassText.getText());
                ConnectionCompression compression = ConnectionCompression.fromString(CassandraOutputDialog.this.wCompression.getText());
                String columnFamily = CassandraOutputDialog.this.columnFamilyCombo.getText();


                CassandraOutputDialog.this.ciFields[0].setComboValues(new String[0]);
                if (!org.pentaho.di.core.util.Utils.isEmpty(columnFamily)) {
                    String[] fieldNames = null;
                    try {
                        CassandraOutputDialog.this.connection = Utils.connect(nodes, port_s, username, password, keyspace, withSSL,
                                truststorefile, truststorepass, compression);
                        Cluster cluster = CassandraOutputDialog.this.connection.getSession().getCluster();
                        Metadata clusterMeta = cluster != null ? cluster.getMetadata() : null;
                        KeyspaceMetadata keyspaceMeta = clusterMeta != null ? clusterMeta.getKeyspace(keyspace) : null;
                        TableMetadata tableMeta = keyspaceMeta != null ? keyspaceMeta.getTable(columnFamily) : null;

                        if (tableMeta != null) {
                            List<ColumnMetadata> column = tableMeta.getColumns();
                            Iterator<ColumnMetadata> iter = column.iterator();
                            fieldNames = new String[column.size()];
                            int i = 0;
                            while (iter.hasNext()) {
                                fieldNames[(i++)] = iter.next().getName();
                            }
                        }
                    } catch (Exception localException) {
                        localException.printStackTrace();
                    }

                    if (!CassandraOutputDialog.this.fieldsList.isDisposed()) {
                        CassandraOutputDialog.this.ciFields[0].setComboValues(fieldNames);
                    }
                }
                if (CassandraOutputDialog.this.connection != null)
                    CassandraOutputDialog.this.connection.release();
            }
        };
        this.shell.getDisplay().asyncExec(fieldLoader);
    }

    private void setStreamFieldCombo() {

        Runnable fieldLoader = () -> {
            try {
                StepMeta stepMeta = CassandraOutputDialog.this.transMeta.findStep(CassandraOutputDialog.this.stepname);
                RowMetaInterface row = CassandraOutputDialog.this.transMeta.getPrevStepFields(stepMeta);

                if ((row != null) && (CassandraOutputDialog.this.inputFields.size() == 0)) {
                    for (int i = 0; i < row.size(); i++) {
                        CassandraOutputDialog.this.inputFields.put(row.getValueMeta(i).getName(), i);
                    }
                }
            } catch (KettleException e) {
                CassandraOutputDialog.this.logError(BaseMessages.getString(CassandraOutputDialog.PKG, "System.Dialog.GetFieldsFailed.Message"));
            }


            Set<String> keySet = CassandraOutputDialog.this.inputFields.keySet();
            List<String> entries = new ArrayList<>(keySet);

            String[] fieldNames = entries.toArray(new String[entries.size()]);

            Const.sortStrings(fieldNames);
            if (!CassandraOutputDialog.this.fieldsList.isDisposed()) {
                CassandraOutputDialog.this.ciFields[1].setComboValues(fieldNames);
            }
        };
        this.shell.getDisplay().asyncExec(fieldLoader);
    }

    private void setFlags() {
        boolean specifyFields = this.specifyFieldsBut.getSelection();
        this.fieldsList.setEnabled(specifyFields);
        this.getFieldsBut.setEnabled(specifyFields);
    }

    private void ok() {
        if (org.pentaho.di.core.util.Utils.isEmpty(this.stepnameText.getText())) {
            return;
        }

        this.stepname = this.stepnameText.getText();
        this.currentMeta.setCassandraNodes(this.hostText.getText());
        this.currentMeta.setCassandraPort(this.portText.getText());
        this.currentMeta.setUsername(this.userText.getText());
        this.currentMeta.setPassword(this.passText.getText());
        this.currentMeta.setKeyspace(this.keyspaceText.getText());
        this.currentMeta.setColumnfamily(this.columnFamilyCombo.getText());

        if (!this.originalMeta.equals(this.currentMeta)) {
            this.currentMeta.setChanged();
            this.changed = this.currentMeta.hasChanged();
        }

        this.currentMeta.setSslEnabled(this.sslenabledBut.getSelection());
        this.currentMeta.setTrustStoreFilePath(this.truststorefileText.getText());
        this.currentMeta.setTrustStorePass(this.truststorepassText.getText());
        this.currentMeta.setSyncMode(this.syncModeEnabledBut.getSelection());

        String batchSize = this.batchSizeText.getText();
        this.currentMeta.setBatchSize(org.pentaho.di.core.util.Utils.isEmpty(batchSize) ? 0 : Integer.valueOf(batchSize));
        this.currentMeta.setCompression(ConnectionCompression.fromString(this.wCompression.getText()));

        this.currentMeta.setTtl(Const.toInt(this.ttlText.getText(), 0));

        this.currentMeta.setSpecifyFields(this.specifyFieldsBut.getSelection());
        int nrRows = this.fieldsList.nrNonEmpty();
        this.currentMeta.allocate(nrRows);
        String[] streamFields = this.currentMeta.getStreamFields();
        String[] cassandraFields = this.currentMeta.getCassandraFields();
        for (int i = 0; i < nrRows; i++) {
            TableItem item = this.fieldsList.getNonEmpty(i);
            cassandraFields[i] = Const.NVL(item.getText(1), "");
            streamFields[i] = Const.NVL(item.getText(2), "");
        }
        dispose();
    }

    private void cancel() {
        this.stepname = null;
        this.currentMeta.setChanged(this.changed);

        dispose();
    }

    private void get() {
        try {
            RowMetaInterface r = this.transMeta.getPrevStepFields(this.stepname);
            if ((r != null) && (!r.isEmpty())) {
                BaseStepDialog.getFieldsFromPrevious(r, this.fieldsList, 1, new int[]{1, 2}, new int[0], -1, -1,
                        null);
            }
        } catch (KettleException e) {
            new ErrorDialog(this.shell,
                    BaseMessages.getString(PKG, "AutSoftCassandraOutputDialog.FailedToGetFields.DialogTitle"),
                    BaseMessages.getString(PKG, "AutSoftCassandraOutputDialog.FailedToGetFields.DialogMessage"), e);
        }
    }

    private void getData() {
        if (!org.pentaho.di.core.util.Utils.isEmpty(this.currentMeta.getCassandraNodes())) {
            this.hostText.setText(this.currentMeta.getCassandraNodes());
        }

        if (!org.pentaho.di.core.util.Utils.isEmpty(this.currentMeta.getCassandraPort())) {
            this.portText.setText(this.currentMeta.getCassandraPort());
        }

        if (!org.pentaho.di.core.util.Utils.isEmpty(this.currentMeta.getUsername())) {
            this.userText.setText(this.currentMeta.getUsername());
        }

        if (!org.pentaho.di.core.util.Utils.isEmpty(this.currentMeta.getPassword())) {
            this.passText.setText(this.currentMeta.getPassword());
        }

        if (!org.pentaho.di.core.util.Utils.isEmpty(this.currentMeta.getKeyspace())) {
            this.keyspaceText.setText(this.currentMeta.getKeyspace());
        }

        if (!org.pentaho.di.core.util.Utils.isEmpty(this.currentMeta.getColumnfamily())) {
            this.columnFamilyCombo.setText(this.currentMeta.getColumnfamily());
        }

        this.sslenabledBut.setSelection(this.currentMeta.getSslEnabled());

        this.syncModeEnabledBut.setSelection(this.currentMeta.isSyncMode());

        if (!org.pentaho.di.core.util.Utils.isEmpty(this.currentMeta.getTrustStoreFilePath())) {
            this.truststorefileText.setText(this.currentMeta.getTrustStoreFilePath());
        }

        if (!org.pentaho.di.core.util.Utils.isEmpty(this.currentMeta.getTrustStorePass())) {
            this.truststorepassText.setText(this.currentMeta.getTrustStorePass());
        }

        this.batchSizeText.setText(String.valueOf(this.currentMeta.getBatchSize()));
        this.wCompression.setText(this.currentMeta.getCompression().toString());

        this.ttlText.setText(String.valueOf(this.currentMeta.getTtl()));
        this.specifyFieldsBut.setSelection(this.currentMeta.isSpecifyFields());

        for (int i = 0; i < this.currentMeta.getCassandraFields().length; i++) {
            TableItem item = this.fieldsList.table.getItem(i);
            if (this.currentMeta.getCassandraFields()[i] != null) {
                item.setText(1, this.currentMeta.getCassandraFields()[i]);
            }
            if (this.currentMeta.getStreamFields()[i] != null) {
                item.setText(2, this.currentMeta.getStreamFields()[i]);
            }
        }

        this.fieldsList.setRowNums();
        this.fieldsList.optWidth(true);

        setFlags();
    }
}
