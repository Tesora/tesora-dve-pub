// OS_STATUS: public
package com.tesora.dve.resultset.collector;

import java.sql.ParameterMetaData;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.tesora.dve.resultset.ColumnMetadata;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;

public class MetadataUtils {

	public static ColumnSet buildColumnSet(ResultSetMetaData rsmd, ProjectionInfo projectionMetadata) throws SQLException {
		ColumnSet rsColumnSet = new ColumnSet();
		if (projectionMetadata != null && projectionMetadata.getWidth() != rsmd.getColumnCount()) 
			throw new SQLException("Computed projection metadata length does not match actual metadata length");
		for (int colIdx = 1; colIdx <= rsmd.getColumnCount(); colIdx++) {
            ColumnMetadata cm = Singletons.require(HostService.class).getDBNative().getResultSetColumnInfo(rsmd, projectionMetadata, colIdx);
			rsColumnSet.addColumn(cm);
		}
		return rsColumnSet;
	}
	
	public static ColumnSet buildParameterSet(ParameterMetaData pmd) throws SQLException {
		ColumnSet cs = new ColumnSet();
		for(int colIdx = 1; colIdx <= pmd.getParameterCount(); colIdx++) {
            ColumnMetadata cm= Singletons.require(HostService.class).getDBNative().getParameterColumnInfo(pmd, colIdx);
			cs.addColumn(cm);
		}
		return cs;
	}
}
