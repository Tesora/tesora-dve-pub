package com.tesora.dve.sql.infoschema;

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
import com.tesora.dve.sql.infoschema.annos.InfoView;
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
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.Lookup;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.Table;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.UnaryFunction;
import com.tesora.dve.variables.Variables;

public class InformationSchemaTableView implements
		Table<AbstractInformationSchemaColumnView> {

	protected LogicalInformationSchemaTable backing;
	protected InfoView view;
	
	protected UnqualifiedName name;
	// null if cannot be plural
	protected UnqualifiedName pluralName;
	
	protected boolean priviledged;
	protected boolean extension;
	
	protected List<AbstractInformationSchemaColumnView> columns;
	protected Lookup<AbstractInformationSchemaColumnView> lookup;
	
	protected AbstractInformationSchemaColumnView orderByColumn;
	protected AbstractInformationSchemaColumnView identColumn;
	
	// columns injected from elsewhere.
	protected List<AbstractInformationSchemaColumnView> injected;

	protected boolean frozen;
	
	public InformationSchemaTableView(InfoView view, LogicalInformationSchemaTable basedOn,
			UnqualifiedName viewName, UnqualifiedName pluralViewName, boolean requiresPriviledge,
			boolean isExtension) {
		super();
		backing = basedOn;
		this.view = view;
		this.name = viewName;
		this.pluralName = pluralViewName;
		this.injected = new ArrayList<AbstractInformationSchemaColumnView>();
		this.columns = new ArrayList<AbstractInformationSchemaColumnView>();
		this.lookup = new Lookup<AbstractInformationSchemaColumnView>(columns, view.getNameFunction(), false, false); 
		this.orderByColumn = null;
		this.identColumn = null;
		this.priviledged = requiresPriviledge;
		this.extension = isExtension;
		this.frozen = false;
	}
		
	@Override
	public String toString() {
		return "InformationSchemaTableView{view=" + view + ", table=" + getName() + "}";
	}
	
	public InfoView getView() {
		return this.view;
	}
	
	protected void validate(SchemaView ofView) {
		if (this.identColumn == null)
			throw new InformationSchemaException("Table " + getName() + " has no ident column");
		if (this.orderByColumn == null)
			throw new InformationSchemaException("Table " + getName() + " has no order by column");
		for(AbstractInformationSchemaColumnView isc : columns)
			isc.validate(ofView, this);
	}
	
	public void prepare(SchemaView view, DBNative dbn) {
		for(AbstractInformationSchemaColumnView isc : columns)
			isc.prepare(view, this, dbn);
		validate(view);
	}

	protected void freeze() {
		frozen = true;
	}
	
	protected void collectInjected(SchemaView view, InformationSchemaTableView table, List<AbstractInformationSchemaColumnView> acc) {
		for(AbstractInformationSchemaColumnView iscv : table.injected) {
			if (table != this) {
				// make a copy, but ensure that we use the local backing column.
				InformationSchemaColumnView nc = (InformationSchemaColumnView) iscv.copy();
				nc.backing = backing.lookup(null, iscv.getLogicalColumn().getName().getUnqualified());
				acc.add(nc);
			}
			LogicalInformationSchemaColumn logicalColumn = iscv.getLogicalColumn();
			if (logicalColumn.getReturnType() != null) { 
				InformationSchemaTableView injectedView = view.lookup(logicalColumn.getReturnType());
				if (injectedView != null)
					collectInjected(view, injectedView, acc);
			}
		}
	}
	
	public void inject(SchemaView view, DBNative dbn) {
		ArrayList<AbstractInformationSchemaColumnView> acc = new ArrayList<AbstractInformationSchemaColumnView>();
		collectInjected(view, this, acc);
		for(AbstractInformationSchemaColumnView iscv : acc) {
			addColumn(null, iscv);
		}
	}
	
	@Override
	public AbstractInformationSchemaColumnView addColumn(SchemaContext sc, AbstractInformationSchemaColumnView c) {
		c.setPosition(columns.size());
		columns.add(c);
		lookup.refreshBacking(columns);
		c.setTable(this);
		if (c.isOrderByColumn())
			orderByColumn = c;
		if (c.isIdentColumn())
			identColumn = c;
		if (c.isInjected())
			injected.add(c);
		return c;
	}

	@Override
	public List<AbstractInformationSchemaColumnView> getColumns(SchemaContext sc) {
		return columns;
	}

	@Override
	public AbstractInformationSchemaColumnView lookup(SchemaContext sc, Name n) {
		return lookup.lookup(n);
	}

	public List<AbstractInformationSchemaColumnView> getInjected() {
		return injected;
	}
	
	protected AbstractInformationSchemaColumnView lookup(String n) {
		AbstractInformationSchemaColumnView iscv = lookup(null,new UnqualifiedName(n));
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

	public LogicalInformationSchemaTable getLogicalTable() {
		return backing;
	}

	public AbstractInformationSchemaColumnView getOrderByColumn() {
		return orderByColumn;
	}
	
	public AbstractInformationSchemaColumnView getIdentColumn() {
		return identColumn;
	}
	
	protected boolean useExtensions(SchemaContext sc) {
		return 	Variables.SHOW_METADATA_EXTENSIONS.getValue(sc.getConnection().getVariableSource()).booleanValue();
	}

	public static void derefEntities(LanguageNode in) {
		for(ColumnInstance ci : EntityColumnRefAction.collect(in)) {
			InformationSchemaColumnView cisc = (InformationSchemaColumnView) ci.getColumn();
			Edge<?,ExpressionNode> parent = ci.getParentEdge();
			parent.set(cisc.buildNameTest(ci));
		}
	}

	public boolean requiresPriviledge() {
		return priviledged;
	}

	public boolean isExtension() {
		return extension;
	}
	
	public void assertPermissions(SchemaContext sc) {
		if (!priviledged) return;
		if (!sc.getPolicyContext().isRoot())
			throw new InformationSchemaException("You do not have permissions to query " + getName().get());
	}

	// filter out columns that are not visible.  columns may not be visible if they are extensions & extensions are not on,
	// or if they are privileged only & the current connection has no privileges.
	public List<AbstractInformationSchemaColumnView> getProjectionColumns(boolean includeExtensions, boolean includePriviledged) {
		ArrayList<AbstractInformationSchemaColumnView> out = new ArrayList<AbstractInformationSchemaColumnView>();
		for(AbstractInformationSchemaColumnView isc : columns) {
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
		AbstractInformationSchemaColumnView obc = getOrderByColumn();
		if (obc != null) {
			SortingSpecification sort = new SortingSpecification(new ColumnInstance(null,obc,ti),true);
			sort.setOrdering(Boolean.TRUE);
			ss.getOrderBysEdge().add(sort);
		}
	}
	
	public void buildTableEntity(CatalogSchema cs, CatalogDatabaseEntity db, int dmid, int storageid, List<PersistedEntity> acc) throws PEException {
		CatalogTableEntity cte = new CatalogTableEntity(cs,db,getName().get(),dmid,storageid,"MEMORY");
		acc.add(cte);
		int counter = 0;
		for(AbstractInformationSchemaColumnView isc : getColumns(null)) {
			isc.buildColumnEntity(cs,cte, (++counter), acc);
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
				AbstractInformationSchemaColumnView isc = (AbstractInformationSchemaColumnView) ci.getColumn();
				if (isc.getReturnType() != null)
					return true;
			}
			return false;
		}
		
		public static ListSet<ColumnInstance> collect(LanguageNode in) {
			return GeneralCollectingTraversal.collect(in, new EntityColumnRefAction());
		}
		
	}

	public static final UnaryFunction<Name[], AbstractInformationSchemaColumnView> regularNameFunc = 
		new UnaryFunction<Name[], AbstractInformationSchemaColumnView>() {

		@Override
		public Name[] evaluate(AbstractInformationSchemaColumnView object) {
			return new Name[] { object.getName() };
		}
		
	};

	@SuppressWarnings("unchecked")
	@Override
	public Database<?> getDatabase(SchemaContext sc) {
		return ((Database<?>)sc.getSource().find(sc, view.getCacheKey()));
	}

}
