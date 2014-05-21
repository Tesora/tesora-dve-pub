package com.tesora.dve.sql.infoschema.engine;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

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
