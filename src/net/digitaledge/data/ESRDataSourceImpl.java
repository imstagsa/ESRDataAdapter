package net.digitaledge.data;
import java.util.List;

import com.jaspersoft.studio.data.fields.IFieldsProvider;
import com.jaspersoft.studio.utils.jasper.JasperReportsConfiguration;

import net.digitaledge.adapter.ESRAdapter;
import net.digitaledge.adapter.ESRAdapterService;
import net.sf.jasperreports.data.DataAdapterService;
import net.sf.jasperreports.engine.JRDataSource;
import net.sf.jasperreports.engine.JRDataset;
import net.sf.jasperreports.engine.JRException;
import net.sf.jasperreports.engine.JRField;
import net.sf.jasperreports.engine.JasperReport;
import net.sf.jasperreports.engine.design.JRDesignField;

public class ESRDataSourceImpl implements IFieldsProvider {

	private final String restQuery = new String("{\"query\": {\"match_all\": {}}}"); 
	private ESRDataConnection eSJDataConnection = null;

	@Override
	public boolean supportsGetFieldsOperation(JasperReportsConfiguration jConfig) {
		return true;
	}

	@Override
	public List<JRDesignField> getFields(DataAdapterService con, JasperReportsConfiguration jConfig, JRDataset jDataset)
			throws JRException, UnsupportedOperationException {
		
		if(con instanceof ESRAdapterService)
		{
			ESRAdapter ESJadapter = (ESRAdapter)((ESRAdapterService)con).getDataAdapter();
			eSJDataConnection = new ESRDataConnection(ESJadapter.getElasticSearchIndexes(), ESJadapter.getElasticSearchTypes(), Integer.parseInt(ESJadapter.getElasticSearchMode()), ESJadapter.getElasticSearchHost(), Integer.parseInt(ESJadapter.getElasticSearchPort()), "","","");
			eSJDataConnection.setSearch(restQuery);
			return eSJDataConnection.getFieldsMapping();
		}
		return null;
	}


	//@Override
	public boolean supportsGetFieldsOperation() {
		return true;
	}


}