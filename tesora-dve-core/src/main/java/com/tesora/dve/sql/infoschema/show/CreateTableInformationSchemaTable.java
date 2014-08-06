package com.tesora.dve.sql.infoschema.show;

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
import java.util.HashMap;
import java.util.List;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.Key;
import com.tesora.dve.common.catalog.UserColumn;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.distribution.RandomDistributionModel;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnMetadata;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.IntermediateResultSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.expression.MTTableKey;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.infoschema.InformationSchemaException;
import com.tesora.dve.sql.infoschema.SchemaView;
import com.tesora.dve.sql.infoschema.engine.LogicalSchemaQueryEngine;
import com.tesora.dve.sql.infoschema.engine.ViewQuery;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.schema.LockInfo;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEForeignKey;
import com.tesora.dve.sql.schema.PEKey;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.modifiers.AutoincTableModifier;
import com.tesora.dve.sql.schema.mt.TableScope;
import com.tesora.dve.variables.KnownVariables;

public class CreateTableInformationSchemaTable extends ShowInformationSchemaTable {

	private ShowInformationSchemaTable tableTable = null;

	public CreateTableInformationSchemaTable() {
		super(null, new UnqualifiedName("create table"), new UnqualifiedName("create table"), false, false);
	}

	@Override
	protected void validate(SchemaView ofView) {
		tableTable = (ShowInformationSchemaTable) ofView.lookup("table");
		if (tableTable == null)
			throw new InformationSchemaException("Cannot find show table view in show view");
	}

	/**
	 * @param sc
	 * @param likeExpr
	 * @param scoping
	 * @return
	 */
	public ViewQuery buildLikeSelect(SchemaContext sc, String likeExpr, List<Name> scoping) {
		throw new InformationSchemaException("Illegal operation: show create table does not support the like clause");
	}
	
	/**
	 * @param sc
	 * @param wc
	 * @param scoping
	 * @return
	 */
	public ViewQuery buildWhereSelect(SchemaContext sc, ExpressionNode wc, List<Name> scoping) {
		throw new InformationSchemaException("Illegal operation: show create table does not support the where clause");
	}
	
	@Override
	public IntermediateResultSet executeUniqueSelect(SchemaContext sc, Name onName) {
		TableInstance tempTab = sc.getTemporaryTableSchema().buildInstance(sc, onName);
		// delegate to the table table to get the basic information
		ViewQuery basicQuery = tableTable.buildUniqueSelect(sc, onName);
		try {
			sc.getCatalog().startTxn();
			// build a tschema version so we can massage the definition to add back in the stuff we stripped out
			// and remove the stuff we added
			PEAbstractTable<?> tschema = null;
			PEAbstractTable<?> tempBacking = null;
			UserTable ut = null;
			LockInfo lock = new LockInfo(com.tesora.dve.lockmanager.LockType.RSHARED, "show create table");
			if (tempTab == null) {
				List<CatalogEntity> raw = LogicalSchemaQueryEngine.buildCatalogEntities(sc, basicQuery).getEntities();
				if (raw.size() > 1)
					throw new SchemaException(Pass.SECOND, "Too many tables named " + onName.getSQL());
				if (raw.size() == 0)
					throw new SchemaException(Pass.SECOND, "No such table: " + onName);
				ut = (UserTable)raw.get(0);
				tempBacking = PEAbstractTable.load(ut, sc);
				try {
					tschema = tempBacking.recreate(sc, ut.getCreateTableStmt(), lock);
				} catch (PEException pe) {
					throw new SchemaException(Pass.PLANNER,"Unable to recreate table def from create table stmt");
				}
			} else { 
				tschema = tempTab.getAbstractTable().recreate(sc, tempTab.getAbstractTable().getDeclaration(), lock);
			}

			StringBuilder buf = new StringBuilder();

			TableKey tk = null;
			if (tempTab != null) {
				tk = tempTab.getTableKey();
			} else if (sc.getPolicyContext().isSchemaTenant() || sc.getPolicyContext().isDataTenant()) {
				TableScope ts = sc.getPolicyContext().getOfTenant(ut);
				tk = new MTTableKey(tempBacking,ts,0);
			} else {
				tk = TableKey.make(sc, tempBacking, 0);
			}
			
			if (sc.getPolicyContext().isSchemaTenant() || sc.getPolicyContext().isDataTenant()) {
				Name localName = null;
				if (ut != null)
					localName = sc.getPolicyContext().getLocalName(ut);
				else if (tempTab.getTableKey() instanceof MTTableKey) {
					MTTableKey mttk = (MTTableKey) tempTab.getTableKey();
					localName = mttk.getScope().getName();
				}
				if (localName != null)
					tschema.setName(localName);
				HashMap<UnqualifiedName,UnqualifiedName> fkmap = new HashMap<UnqualifiedName,UnqualifiedName>();
				if (ut != null) {
					for(Key k : ut.getKeys()) {
						// either not a foreign key, or a forward foreign key
						// if forward we would have used the original declared name.
						if (k.getReferencedTable() == null) continue;
						Name visibleName = sc.getPolicyContext().getLocalName(k.getReferencedTable());
						fkmap.put(new UnqualifiedName(k.getReferencedTable().getName()),visibleName.getUnqualified());
					}
				}

				// in mt mode the stored declaration has the mtid; remove it.
				PEColumn c = tschema.getTenantColumn(sc);
				tschema.removeColumn(sc,c);
				if (tschema.isTable() && !tschema.isUserlandTemporaryTable()) {
					List<PEKey> toRemove = new ArrayList<PEKey>();
					for(PEKey k : tschema.getKeys(sc)) {
						if (k.isHidden()) {
							toRemove.add(k);
							continue;
						}
						if (k.containsColumn(c)) {
							k.removeColumn(c);
						}
						if (k.isForeign()) {
							PEForeignKey pefk = (PEForeignKey) k;
							// also reset the physical symbol name to be the logical symbol name
							pefk.setPhysicalSymbol(pefk.getSymbol());
							if (pefk.isForward()) continue;
							UnqualifiedName repl = fkmap.get(pefk.getTargetTableName(null));
							pefk.revertToForward(sc);
							pefk.resetTargetTableName(repl);
						}
					}
					for(PEKey k : toRemove) {
						tschema.asTable().removeKey(sc, k);
					}
				}

			}

			/* Make sure we do not emit implicit default values. */
			if (ut != null) {
				for (final UserColumn uc : ut.getUserColumns()) {
					if (!uc.hasDefault()) {
						final PEColumn tc = tschema.lookup(sc, uc.getName());
						if (tc != null) {
							tc.setDefaultValue(null);
						}
					}
				}
			}

			if (tk.getAbstractTable().isTable()) {
				long nextVal = -1;
				if (tk.getAbstractTable().asTable().hasAutoInc())
					nextVal = tk.readAutoIncrBlock(sc);
				if (nextVal > 1)
					tschema.asTable().getModifiers().setModifier(new AutoincTableModifier(nextVal));
			}

			boolean omitDistVect = 
					KnownVariables.OMIT_DIST_COMMENTS.getValue(sc.getConnection().getVariableSource()).booleanValue();
			
			if (tschema.isTable())
                buf.append(Singletons.require(HostService.class).getDBNative().getEmitter().emitExternalCreateTableStatement(sc,tschema.asTable(),omitDistVect));
			else {
				buf.append("CREATE ");
                Singletons.require(HostService.class).getDBNative().getEmitter().emitViewDeclaration(sc, tschema.asView().getView(sc), null, buf);
			}
				
			ColumnSet cs = new ColumnSet();
			try {
                String varcharTypeName = Singletons.require(HostService.class).getDBNative().getTypeCatalog().findType(java.sql.Types.VARCHAR, true).getTypeName();
				cs.addColumn(buildColumnMetadata(tschema.isTable() ? "Table" : "View",255,varcharTypeName, java.sql.Types.VARCHAR));
				cs.addColumn(buildColumnMetadata(tschema.isTable() ? "Create Table" : "Create View",255,varcharTypeName, java.sql.Types.VARCHAR));
				if (tschema.isView()) {
					cs.addColumn(buildColumnMetadata("character_set_client",255,varcharTypeName,java.sql.Types.VARCHAR));
					cs.addColumn(buildColumnMetadata("collation_connection",255,varcharTypeName,java.sql.Types.VARCHAR));					
				}
			} catch (PEException pe) {
				throw new SchemaException(Pass.PLANNER, "Unable to find varchar type?",pe);
			}

			ResultRow rr = new ResultRow();
			rr.addResultColumn(onName.getUnqualified().get());
			rr.addResultColumn(buf.toString());
			if (tschema.isView()) {
				rr.addResultColumn(tschema.asView().getView(sc).getCharset().getUnquotedName().get());
				rr.addResultColumn(tschema.asView().getView(sc).getCollation().getUnquotedName().get());
			}
			return new IntermediateResultSet(cs, rr);
		} finally {
			sc.getCatalog().rollbackTxn();
		}
		
	}
	
	/**
	 * @param c
	 * @param udb
	 * @param rdm
	 * @param dbn
	 * @return
	 */
	public UserTable persist(CatalogDAO c, UserDatabase udb, RandomDistributionModel rdm, DBNative dbn) {
		return null;
	}
	
	private static ColumnMetadata buildColumnMetadata(String name, int size, String typeName, int typeCode) {
		ColumnMetadata cmc = new ColumnMetadata();
		cmc.setName(name);
		cmc.setSize(size);
		cmc.setNativeTypeName(typeName);
		cmc.setDataType(typeCode);
		cmc.setAliasName(name);
		return cmc;

	}
}
