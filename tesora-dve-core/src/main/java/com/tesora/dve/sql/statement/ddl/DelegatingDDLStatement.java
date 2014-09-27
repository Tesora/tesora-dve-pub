package com.tesora.dve.sql.statement.ddl;

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
import java.util.Collections;
import java.util.List;

import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.sql.infoschema.InformationSchemaColumn;
import com.tesora.dve.sql.infoschema.InformationSchemaTable;
import com.tesora.dve.sql.infoschema.computed.ComputedInformationSchemaTable;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.NameAlias;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.schema.Column;
import com.tesora.dve.sql.schema.DistributionKeyTemplate;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEPersistentGroup;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.TempColumn;
import com.tesora.dve.sql.schema.TempTable;
import com.tesora.dve.sql.schema.DistributionVector.Model;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.schema.types.TempColumnType;
import com.tesora.dve.sql.statement.dml.AliasInformation;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;

public class DelegatingDDLStatement extends DDLStatement {

	protected PEPersistentGroup onGroup;
	protected InformationSchemaTable originalView;
	
	public DelegatingDDLStatement(PEPersistentGroup theGroup, InformationSchemaTable showTab) {
		super(false);
		onGroup = theGroup;
		originalView = showTab;
	}
	
	@Override
	public Action getAction() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Persistable<?, ?> getRoot() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected void preplan(SchemaContext sc, ExecutionSequence es,boolean explain) throws PEException {
		// not actually ddl
	}

	protected DistributionKeyTemplate buildKeyTemplate() {
		return new DistributionKeyTemplate(null) {
			@Override
			public Model getModel(SchemaContext sc) { return DistributionVector.Model.RANDOM ; }
		};
	}
	
	@SuppressWarnings("unchecked")
	protected DistributionVector buildVector(SchemaContext pc) {
		return new DistributionVector(pc, Collections.EMPTY_LIST, DistributionVector.Model.RANDOM, true) {
			@Override
			public DistributionModel getPersistent(SchemaContext sc) {
				return DistributionVector.Model.RANDOM.getSingleton();
			}
		};
	}
	

	
	@Override
	public ProjectionInfo getProjectionMetadata(SchemaContext pc) {
		ProjectionInfo pi = new ProjectionInfo(originalView.getColumns(pc).size());
		for(int i = 0; i < originalView.getColumns(pc).size(); i++) {
			Column<?> c = originalView.getColumns(pc).get(i);
			String cn = c.getName().getUnquotedName().get();
			pi.addColumn(i+1, cn,cn);
		}
		return pi;
	}

	protected TempTable buildFirstTempTable(SchemaContext pc, PEStorageGroup targetGroup) throws PEException {
		// we can actually build this by reading off columns from the show info schema table
		List<PEColumn> columns = new ArrayList<PEColumn>();
		for(InformationSchemaColumn viewCol : originalView.getColumns(pc)) {
			columns.add(new TempColumn(pc,viewCol.getName(),new TempColumnType(viewCol.getType())));
		}
		return TempTable.buildAdHoc(pc, getDatabase(pc), columns, DistributionVector.Model.BROADCAST, Collections.<PEColumn> emptyList(), targetGroup, false);
	}
	
	protected SelectStatement buildFirstSelect(SchemaContext pc,TempTable tt) {
		TableInstance ti = new TableInstance(tt, tt.getName(pc), null, pc.getNextTable(),false);
		List<ExpressionNode> proj = new ArrayList<ExpressionNode>();
		AliasInformation ai = new AliasInformation();
		for(PEColumn pec : tt.getColumns(pc)) {
			ColumnInstance ci = new ColumnInstance(pec,ti);
			ExpressionAlias ea = new ExpressionAlias(ci,new NameAlias(pec.getName().getUnqualified()),false);
			proj.add(ea);
			ai.addAlias(pec.getName().getUnqualified().getUnquotedName().get());
		}
		FromTableReference ftr = new FromTableReference(ti);
		return new SelectStatement(Collections.singletonList(ftr),proj,null,null,null,null,null,null,null,null,ai,null);
	}

	@Override
	public CacheInvalidationRecord getInvalidationRecord(SchemaContext sc) {
		// does not invalidate
		return null;
	}
	


}
