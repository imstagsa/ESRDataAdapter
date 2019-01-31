package net.digitaledge;

import java.util.List;

import org.eclipse.core.databinding.beans.PojoObservables;
import org.eclipse.jface.databinding.swt.SWTObservables;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.Button;

import com.jaspersoft.studio.data.ADataAdapterComposite;
import com.jaspersoft.studio.data.DataAdapterDescriptor;

import net.digitaledge.adapter.ESRAdapter;
import net.digitaledge.data.ESRDataConnection;
import net.digitaledge.data.ESRDataSource;
import net.sf.jasperreports.data.DataAdapter;
import net.sf.jasperreports.engine.JasperReportsContext;


/**
 * Inside this class are defined the controls shown when the 
 * adapter is created on edited from Jaspersoft Studio. 
 * This controls can be used the configure the data adapter.
 * With the example data adapter it provide the controls to define
 * the number of record, the number of value for each record, and
 * the range between every value is generated
 * 
 *
 */
public class ESRDataAdapterComposite extends ADataAdapterComposite {
    

	private ESRDataAdapterDescriptor dataAdapterDescriptor;
	private Text esHostField;
	private Text esPortField;
	//private Text esClusterField;
	private Text esIndexesField;
	private Text esTypesField;
	//private Text esUsernameField;
	//private Text esPasswordField;
	private Combo esSearchModeField;
	
	public ESRDataAdapterComposite(Composite parent, int style, JasperReportsContext jrContext) {
		super(parent, style, jrContext);
		initComponents();
	}

	private void initComponents() {
		setLayout(new GridLayout(2, false));
		createLabel("Indice");
		esIndexesField = createTextField(false);
		createLabel("Type");
		esTypesField = createTextField(false);                
		createLabel("Hostname");
		esHostField = createTextField(false);
		createLabel("Port");
		esPortField = createTextField(false);
		//createLabel("Cluster");
		//esClusterField = createTextField(false);
		//createLabel("Username");
		//esUsernameField = createTextField(false);
		//createLabel("Password");
		//esPasswordField = createTextField(true);
		createLabel("Query Mode");
		esSearchModeField = createComboField();
	}

	private void createLabel(String text) {
		Label label = new Label(this, SWT.NONE);
		label.setText(text);
		label.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, false, false, 1, 1));
	}

	private Text createTextField(boolean password) {
		Text textField = new Text(this, !password ? SWT.BORDER : SWT.BORDER | SWT.PASSWORD);
		textField.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false, 1, 1));
		return textField;
	}
	
	private Combo createComboField() {
		Combo comboField = new Combo(this, SWT.BORDER);
		comboField.add("Hits Mode - Returns Hits Data", ESRDataSource.ES_MODE_HITS);
		comboField.add("Aggregation Mode - Returns Aggregations Data", ESRDataSource.ES_MODE_AGGS);
		comboField.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false, 1, 1));	
		return comboField;
	}
	
	public DataAdapterDescriptor getDataAdapter() {
		if (dataAdapterDescriptor == null) {
			dataAdapterDescriptor = new ESRDataAdapterDescriptor();
		}
		return dataAdapterDescriptor;
	}

	@Override
	public void setDataAdapter(DataAdapterDescriptor dataAdapterDescriptor) {
		super.setDataAdapter(dataAdapterDescriptor);

		this.dataAdapterDescriptor = (ESRDataAdapterDescriptor) dataAdapterDescriptor;
		ESRAdapter dataAdapter = (ESRAdapter) dataAdapterDescriptor.getDataAdapter();
		bindWidgets(dataAdapter);
	}

	@Override
	protected void bindWidgets(DataAdapter dataAdapter) {
		bindingContext.bindValue(SWTObservables.observeText(esHostField, SWT.Modify), PojoObservables.observeValue(dataAdapter, "elasticSearchHost"));
		bindingContext.bindValue(SWTObservables.observeText(esPortField, SWT.Modify), PojoObservables.observeValue(dataAdapter, "elasticSearchPort"));
		bindingContext.bindValue(SWTObservables.observeSingleSelectionIndex(esSearchModeField), PojoObservables.observeValue(dataAdapter, "elasticSearchMode"));
		bindingContext.bindValue(SWTObservables.observeText(esIndexesField, SWT.Modify), PojoObservables.observeValue(dataAdapter, "elasticSearchIndexes"));
		bindingContext.bindValue(SWTObservables.observeText(esTypesField, SWT.Modify), PojoObservables.observeValue(dataAdapter, "elasticSearchTypes"));
		//bindingContext.bindValue(SWTObservables.observeText(esClusterField, SWT.Modify), PojoObservables.observeValue(dataAdapter, "elasticSearchCluster"));
		//bindingContext.bindValue(SWTObservables.observeText(esUsernameField, SWT.Modify), PojoObservables.observeValue(dataAdapter, "elasticSearchUsername"));
		//bindingContext.bindValue(SWTObservables.observeText(esPasswordField, SWT.Modify), PojoObservables.observeValue(dataAdapter, "elasticSearchPassword"));
				
	}

	@Override
	public String getHelpContextId() {
		return PREFIX.concat("adapter_elasticsearch");
	}
}