package com.tesora.dve.sql.infoschema.computed;

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

import com.tesora.dve.db.DBNative;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.persist.PersistedEntity;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.infoschema.AbstractInformationSchema;
import com.tesora.dve.sql.infoschema.InformationSchemaColumn;
import com.tesora.dve.sql.infoschema.InformationSchemaException;
import com.tesora.dve.sql.infoschema.InformationSchemaTable;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.computed.BackedComputedInformationSchemaColumn.BackedComputedInformationSchemaColumnAdapter;
import com.tesora.dve.sql.infoschema.engine.ViewQuery;
import com.tesora.dve.sql.infoschema.persist.CatalogDatabaseEntity;
import com.tesora.dve.sql.infoschema.persist.CatalogSchema;
import com.tesora.dve.sql.infoschema.persist.CatalogTableEntity;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.GeneralCollectingTraversal;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.structural.SortingSpecification;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.Column;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.Lookup;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.util.Cast;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.UnaryFunction;
import com.tesora.dve.variables.KnownVariables;

public class ComputedInformationSchemaTable implements InformationSchemaTable {

	protected LogicalInformationSchemaTable backing;

	protected InfoView view;
	
	protected UnqualifiedName name;
	// null if cannot be plural
	protected UnqualifiedName pluralName;
	
	protected boolean priviledged;
	protected boolean extension;
	
	protected List<ComputedInformationSchemaColumn> columns;
	protected Lookup<ComputedInformationSchemaColumn> lookup;
	
	protected ComputedInformationSchemaColumn orderByColumn;
	protected ComputedInformationSchemaColumn identColumn;
	
	// columns injected from elsewhere.
	protected List<ComputedInformationSchemaColumn> injected;

	protected boolean frozen;

	
	
	public ComputedInformationSchemaTable(InfoView view, LogicalInformationSchemaTable backing, UnqualifiedName viewName,
			UnqualifiedName pluralViewName, boolean requiresPriviledge,
			boolean isExtension) {
		super();
		this.view = view;
		this.name = (view.isCapitalizeNames() ? viewName.getCapitalized().getUnqualified() : viewName); 
		if (pluralViewName == null) this.pluralName = pluralViewName;
		else this.pluralName = (view.isCapitalizeNames() ? pluralViewName.getCapitalized().getUnqualified() : pluralViewName); 
		this.injected = new ArrayList<ComputedInformationSchemaColumn>();
		this.columns = new ArrayList<ComputedInformationSchemaColumn>();
		this.lookup = new Lookup<ComputedInformationSchemaColumn>(columns, 
				new UnaryFunction<Name[], ComputedInformationSchemaColumn>() {

					@Override
					public Name[] evaluate(ComputedInformationSchemaColumn object) {
						return new Name[] { object.getName() };
					}
			
				},				
				false, view.isLookupCaseSensitive()); 
		this.orderByColumn = null;
		this.identColumn = null;
		this.priviledged = requiresPriviledge;
		this.extension = isExtension;
		this.frozen = false;
		this.backing = backing;
	}
	
	@Override
	public String toString() {
		return this.getClass().getSimpleName() + "{view=" + view +", table=" + getName() + "}";
	}

	
	protected void collectInjected(AbstractInformationSchema view, ComputedInformationSchemaTable table, List<ComputedInformationSchemaColumn> acc) {
		for(ComputedInformationSchemaColumn iscv : table.injected) {
			if (table != this) {
				// make a copy, but ensure that we use the local backing column.
				LogicalInformationSchemaColumn newBacking =
						backing.lookup(null, iscv.getLogicalColumn().getName().getUnqualified());
				BackedComputedInformationSchemaColumnAdapter sucky =
						new BackedComputedInformationSchemaColumnAdapter(newBacking);
				ComputedInformationSchemaColumn nc = (ComputedInformationSchemaColumn) iscv.copy(sucky);
				acc.add(nc);
			}
			LogicalInformationSchemaColumn logicalColumn = iscv.getLogicalColumn();
			if (logicalColumn.getReturnType() != null) { 
				ComputedInformationSchemaTable injectedView = (ComputedInformationSchemaTable) view.lookup(logicalColumn.getReturnType());
				if (injectedView != null)
					collectInjected(view, injectedView, acc);
			}
		}
	}
	
	public void inject(AbstractInformationSchema view, DBNative dbn) {
		ArrayList<ComputedInformationSchemaColumn> acc = new ArrayList<ComputedInformationSchemaColumn>();
		collectInjected(view, this, acc);
		for(ComputedInformationSchemaColumn iscv : acc) {
			addColumn(null, iscv);
		}
	}
	

	public LogicalInformationSchemaTable getLogicalTable() {
		return backing;
	}


	public static void derefEntities(LanguageNode in) {
		for(ColumnInstance ci : EntityColumnRefAction.collect(in)) {
			ComputedInformationSchemaColumn cisc = (ComputedInformationSchemaColumn) ci.getColumn();
			Edge<?,ExpressionNode> parent = ci.getParentEdge();
			parent.set(cisc.buildNameTest(ci));
		}
	}

	
	// used by the information_schema views
	public void annotate(SchemaContext sc, ViewQuery vq, SelectStatement in, TableKey onTK) {
		if (onTK.getTable() == this) {
			if (orderByColumn != null && !in.getOrderBysEdge().has()) {
				in.setOrderBy(Collections.singletonList(
						new SortingSpecification(
								new ColumnInstance(orderByColumn, onTK.toInstance()), true)));
			}
		}
	}
	
	protected static class EntityColumnRefAction extends GeneralCollectingTraversal {

		public EntityColumnRefAction() {
			super(Order.POSTORDER, ExecStyle.ONCE);
		}
		
		@Override
		public boolean is(LanguageNode ln) {
			if (EngineConstant.COLUMN.has(ln)) {
				ColumnInstance ci = (ColumnInstance) ln;
				ComputedInformationSchemaColumn isc = (ComputedInformationSchemaColumn) ci.getColumn();
				if (isc.getReturnType() != null)
					return true;
			}
			return false;
		}
		
		public static ListSet<ColumnInstance> collect(LanguageNode in) {
			return GeneralCollectingTraversal.collect(in, new EntityColumnRefAction());
		}
		
	}

	public InfoView getView() {
		return this.view;
	}
	
	protected void validate(AbstractInformationSchema ofView) {
		if (this.identColumn == null)
			throw new InformationSchemaException("Table " + getName() + " has no ident column");
		if (this.orderByColumn == null)
			throw new InformationSchemaException("Table " + getName() + " has no order by column");
		for(ComputedInformationSchemaColumn isc : columns)
			isc.validate(ofView, this);
	}
	
	public void prepare(AbstractInformationSchema view, DBNative dbn) {
		for(ComputedInformationSchemaColumn isc : columns)
			isc.prepare(view, this, dbn);
		validate(view);
	}

	public void freeze() {
		frozen = true;
	}

	@Override
	public InformationSchemaColumn addColumn(SchemaContext sc, InformationSchemaColumn c) {
		ComputedInformationSchemaColumn cv = (ComputedInformationSchemaColumn) c;
		cv.setPosition(columns.size());
		columns.add(cv);
		lookup.refreshBacking(columns);
		cv.setTable(this);
		if (cv.isOrderByColumn())
			orderByColumn = cv;
		if (cv.isIdentColumn())
			identColumn = cv;
		if (cv.isInjected())
			injected.add(cv);
		return cv;
	}

	@Override
	public List<InformationSchemaColumn> getColumns(SchemaContext sc) {
		return Functional.apply(columns,new Cast<InformationSchemaColumn,ComputedInformationSchemaColumn>());
	}

	@Override
	public ComputedInformationSchemaColumn lookup(SchemaContext sc, Name n) {
		return lookup.lookup(n);
	}

	public List<ComputedInformationSchemaColumn> getInjected() {
		return injected;
	}
	
	protected ComputedInformationSchemaColumn lookup(String n) {
		ComputedInformationSchemaColumn iscv = lookup(null,new UnqualifiedName(n));
		if (iscv == null)
			throw new InformationSchemaException("Unable to find column " + n + " in " + view + " table " + getName());
		return iscv;
	}
	
	@Override
	public Name getName() {
		return name;
	}

	@Override
	public Name getName(SchemaContext sc) {
		return getName();
	}
	
	public Name getPluralName() {
		return pluralName;
	}
	
	@Override
	public boolean isInfoSchema() {
		return true;
	}

	@Override
	public boolean isTempTable() {
		return false;
	}

	public ComputedInformationSchemaColumn getOrderByColumn() {
		return orderByColumn;
	}
	
	public ComputedInformationSchemaColumn getIdentColumn() {
		return identColumn;
	}
	
	protected boolean useExtensions(SchemaContext sc) {
		return 	KnownVariables.SHOW_METADATA_EXTENSIONS.getValue(sc.getConnection().getVariableSource()).booleanValue();
	}

	public boolean requiresPriviledge() {
		return priviledged;
	}

	public boolean isExtension() {
		return extension;
	}
	
	public void buildTableEntity(CatalogSchema cs, CatalogDatabaseEntity db, int dmid, int storageid, List<PersistedEntity> acc) throws PEException {
		CatalogTableEntity cte = new CatalogTableEntity(cs,db,getName().get(),dmid,storageid,"MEMORY");
		acc.add(cte);
		int counter = 0;
		for(InformationSchemaColumn isc : getColumns(null)) {
			isc.buildColumnEntity(cs,cte, (++counter), acc);
		}
	}
	

	public void assertPermissions(SchemaContext sc) {
		if (!priviledged) return;
		if (!sc.getPolicyContext().isRoot())
			throw new InformationSchemaException("You do not have permissions to query " + getName().get());
	}

	// filter out columns that are not visible.  columns may not be visible if they are extensions & extensions are not on,
	// or if they are privileged only & the current connection has no privileges.
	public List<ComputedInformationSchemaColumn> getProjectionColumns(boolean includeExtensions, boolean includePriviledged) {
		ArrayList<ComputedInformationSchemaColumn> out = new ArrayList<ComputedInformationSchemaColumn>();
		for(ComputedInformationSchemaColumn isc : columns) {
			if (!isc.isVisible())
				continue;
			if (isc.isExtension() && !includeExtensions)
				continue;
			if (isc.requiresPrivilege() && !includePriviledged)
				continue;
			out.add(isc);
		}
		return out;
	}
	
	public void addSorting(SelectStatement ss, TableInstance ti) {
		ComputedInformationSchemaColumn obc = getOrderByColumn();
		if (obc != null) {
			SortingSpecification sort = new SortingSpecification(new ColumnInstance(null,obc,ti),true);
			sort.setOrdering(Boolean.TRUE);
			ss.getOrderBysEdge().add(sort);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Database<?> getDatabase(SchemaContext sc) {
		return ((Database<?>)sc.getSource().find(sc, view.getCacheKey()));
	}

	// bit of a hack - variables table executes very differently
	public boolean isVariablesTable() {
		return false;
	}

	public boolean isView() {
		return false;
	}

	
}
