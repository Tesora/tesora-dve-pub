// OS_STATUS: public
package com.tesora.dve.sql.infoschema;

import java.util.Collections;
import java.util.List;

import com.tesora.dve.sql.infoschema.engine.ScopedColumnInstance;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class DelegatingInformationSchemaColumn extends LogicalInformationSchemaColumn {

	private List<LogicalInformationSchemaColumn> pathToActual;
	
	public DelegatingInformationSchemaColumn(List<LogicalInformationSchemaColumn> actual, UnqualifiedName logicalName) {
		super(logicalName, actual.get(actual.size() - 1).getType()); 
		pathToActual = actual;
	}

	public DelegatingInformationSchemaColumn(LogicalInformationSchemaColumn actual, UnqualifiedName logicalName) {
		this(Collections.singletonList(actual),logicalName);
	}
	
	public LogicalInformationSchemaTable getActualTable() {
		return pathToActual.get(0).getTable();
	}
	
	public List<LogicalInformationSchemaColumn> getPath() {
		return pathToActual;
	}
	
	public ColumnInstance rewriteToActual(TableInstance onTable) {
		ColumnInstance current = null;
		for(LogicalInformationSchemaColumn lisc : pathToActual) {
			if (current == null)
				current = new ColumnInstance(lisc,onTable);
			else {
				current = new ScopedColumnInstance(lisc,current);
			}
		}
		return current;
	}
	
}
