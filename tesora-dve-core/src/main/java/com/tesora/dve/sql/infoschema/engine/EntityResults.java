// OS_STATUS: public
package com.tesora.dve.sql.infoschema.engine;

import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.IntermediateResultSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.infoschema.AbstractInformationSchemaColumnView;
import com.tesora.dve.sql.infoschema.CatalogInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.SyntheticLogicalInformationSchemaColumn;
import com.tesora.dve.sql.schema.SchemaContext;

// an entity based result set; holds the catalog entities prior to converting them into a result set.
public class EntityResults {

	private LogicalQuery logicalQuery;
	private List<CatalogEntity> entities;
	
	public EntityResults(LogicalQuery lq, List<CatalogEntity> results) {
		logicalQuery = lq;
		entities = results;
	}
	
	public List<CatalogEntity> getEntities() { return entities; }
	
	public IntermediateResultSet getResultSet(SchemaContext sc) {
		ColumnSet cs = LogicalSchemaQueryEngine.buildProjectionMetadata(logicalQuery.getProjectionColumns());
		ArrayList<ResultRow> rows = new ArrayList<ResultRow>();
		for(CatalogEntity ce : entities) {
			ResultRow rr = new ResultRow();
			for(List<AbstractInformationSchemaColumnView> pt : logicalQuery.getProjectionColumns()) {
				if (pt.size() == 1) {
					AbstractInformationSchemaColumnView c = pt.get(0);
					if (!c.isSynthetic()) {
						CatalogInformationSchemaColumn cisc = (CatalogInformationSchemaColumn) c.getLogicalColumn();
						rr.addResultColumn(cisc.getValue(sc, ce,c));
					} else {
						SyntheticLogicalInformationSchemaColumn cisc = (SyntheticLogicalInformationSchemaColumn) c.getLogicalColumn();
						rr.addResultColumn(cisc.getValue());
					}
				} else {
					CatalogEntity ent = ce;
					for(int i = 0; i < pt.size(); i++) {
						AbstractInformationSchemaColumnView c = pt.get(i);
						boolean last = (i == (pt.size() - 1));
						CatalogInformationSchemaColumn cisc = (CatalogInformationSchemaColumn) c.getLogicalColumn();
						if (last) {
							rr.addResultColumn(cisc.getValue(sc, ent, c));
						}
						else
							ent = (CatalogEntity) cisc.getValue(sc, ent, c);
					}
				}
			}
			if (rr.getRow().isEmpty()) continue;
			rows.add(rr);
		}
		return new IntermediateResultSet(cs, rows);

	}
	

}
