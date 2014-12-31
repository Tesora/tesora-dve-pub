package com.tesora.dve.sql.infoschema.direct;

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

import com.tesora.dve.db.DBNative;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnMetadata;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.IntermediateResultSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.MTTableKey;
import com.tesora.dve.sql.expression.ScopeEntry;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.infoschema.InfoView;
import com.tesora.dve.sql.infoschema.InformationSchemaException;
import com.tesora.dve.sql.infoschema.ShowOptions;
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
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.ddl.SchemaQueryStatement;
import com.tesora.dve.variables.KnownVariables;

public class DirectShowCreateTable extends DirectShowSchemaTable {

	public DirectShowCreateTable(SchemaContext sc, 
			List<PEColumn> cols, List<DirectColumnGenerator> columnGenerators) {
		super(sc, InfoView.SHOW, cols, new UnqualifiedName("create table"), null, false, false,
				columnGenerators);
		// TODO Auto-generated constructor stub
	}

	@Override
	public Statement buildShowPlural(SchemaContext sc, List<Name> scoping,
			ExpressionNode likeExpr, ExpressionNode whereExpr,
			ShowOptions options) {
		throw new InformationSchemaException("Illegal operation: show create table does not support multiple targets");
	}

	@Override
	public Statement buildUniqueStatement(SchemaContext sc, Name onName,
			ShowOptions opts) {
		TableInstance tab = sc.getTemporaryTableSchema().buildInstance(sc, onName);
		// delegate to the table table to get the basic information
		try {
			sc.getCatalog().startTxn();
			// build a tschema version so we can massage the definition to add back in the stuff we stripped out
			// and remove the stuff we added
			PEAbstractTable<?> tschema = null;
			LockInfo lock = new LockInfo(com.tesora.dve.lockmanager.LockType.RSHARED, "show create table");
			if (tab == null) {
				tab = ScopeEntry.resolver.lookupTable(sc, onName, lock);
				if (tab == null)
					throw new SchemaException(Pass.SECOND, "No such table: " + onName);
			}
			tschema = tab.getAbstractTable().recreate(sc, tab.getAbstractTable().getDeclaration(), lock);

			StringBuilder buf = new StringBuilder();

			TableKey tk = tab.getTableKey();
			
			if (sc.getPolicyContext().isSchemaTenant() || sc.getPolicyContext().isDataTenant()) {
				Name localName = null;
				if (tab.getTableKey() instanceof MTTableKey) {
					MTTableKey mttk = (MTTableKey) tab.getTableKey();
					localName = mttk.getScope().getName();
				}
				if (localName != null)
					tschema.setName(localName);
				HashMap<UnqualifiedName,UnqualifiedName> fkmap = new HashMap<UnqualifiedName,UnqualifiedName>();
				/*
				if (ut != null) {
					for(Key k : ut.getKeys()) {
						// either not a foreign key, or a forward foreign key
						// if forward we would have used the original declared name.
						if (k.getReferencedTable() == null) continue;
						Name visibleName = sc.getPolicyContext().getLocalName(k.getReferencedTable());
						fkmap.put(new UnqualifiedName(k.getReferencedTable().getName()),visibleName.getUnqualified());
					}
				}
				*/

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
                buf.append(Singletons.require(DBNative.class).getEmitter().emitExternalCreateTableStatement(sc,sc.getValues(),tschema.asTable(),omitDistVect));
			else {
				buf.append("CREATE ");
                Singletons.require(DBNative.class).getEmitter().emitViewDeclaration(sc, sc.getValues(), tschema.asView().getView(sc), null, buf);
			}
				
			ColumnSet cs = new ColumnSet();
			try {
                String varcharTypeName = Singletons.require(DBNative.class).getTypeCatalog().findType(java.sql.Types.VARCHAR, true).getTypeName();
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
			return new SchemaQueryStatement(false, getName().get(),new IntermediateResultSet(cs,rr));
		} finally {
			sc.getCatalog().rollbackTxn();
		}
	}

	private static ColumnMetadata buildColumnMetadata(String name, int size, String typeName, int typeCode) {
		ColumnMetadata cmc = new ColumnMetadata();
		cmc.setName(name);
		cmc.setSize(size);
		cmc.setTypeName(typeName);
		cmc.setDataType(typeCode);
		cmc.setAliasName(name);
		return cmc;

	}

}
