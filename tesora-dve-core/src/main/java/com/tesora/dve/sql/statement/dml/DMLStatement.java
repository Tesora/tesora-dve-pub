package com.tesora.dve.sql.statement.dml;

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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.db.Emitter;
import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.expression.ColumnKey;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.jg.JoinGraph;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.Column;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.DistributionKey;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.QualifiedName;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.SchemaContext.DistKeyOpType;
import com.tesora.dve.sql.statement.CacheableStatement;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.transform.SchemaMapper;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.execution.DMLExplainReason;
import com.tesora.dve.sql.transform.execution.DMLExplainRecord;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.ExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionType;
import com.tesora.dve.sql.transform.strategy.TransformFactory;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.ListSetMap;

public abstract class DMLStatement extends Statement implements CacheableStatement {

	
	private AliasInformation aliases = null;
	private DerivedInfo derived = null;
	
	// copy information; only point backwards to the source
	protected SchemaMapper source = null;

	protected DMLStatement(SourceLocation location) {
		super(location);
	}
	
	public void setAliases(AliasInformation ai) {
		aliases = ai;
	}
		
	public DerivedInfo getDerivedInfo() {
		if (derived == null) derived = buildDerivedInfo();
		return derived;
	}
		
	@Override
	public ListSet<TableKey> getAllTableKeys() {
		return getDerivedInfo().getAllTableKeys();
	}
	
	@Override
	protected void preplan(SchemaContext pc, ExecutionSequence es,boolean explain) throws PEException {
		// we would have acquired appropriate locks during parsing.  we lock all tables in preparation for lock tables.
	}

	@Override
	public boolean filterStatement(SchemaContext sc) {
		if (!sc.getConnection().hasFilter()) {
			return false;
		}
		ListSet<TableKey> tabs = getDerivedInfo().getAllTableKeys();
		for(TableKey tk : tabs) {
			if (tk.getTable() instanceof PETable) {
				if (sc.getConnection().isFilteredTable(
						new QualifiedName(
								tk.getAbstractTable().getDatabase(sc).getName().getUnquotedName().getUnqualified(),
								tk.getAbstractTable().getName().getUnquotedName().getUnqualified()))) {
					return true;
				}
			}
		}
		return false;
	}
	
	public AliasInformation getAliases() {
		if (aliases == null) throw new IllegalStateException("Missing aliases");
		return aliases; 
	}

	public boolean isDegenerate(SchemaContext sc) {
		ListSet<TableKey> tables = EngineConstant.TABLES.getValue(this,sc);
		if (tables != null && tables.size() > 1) {
			JoinGraph jg = EngineConstant.PARTITIONS.getValue(this, sc);
			// only degenerate if there is more than one partition & no joins
			if (jg.getPartitions().size() > 1 && jg.getJoins().isEmpty())
				return true;
			return false;
			/*
			ListSet<ColocatedJoin> invalidJoins = EngineConstant.INVALID_JOINS.getValue(this,getPersistenceContext());
			if ((partitions == null || partitions.isEmpty()) && (invalidJoins == null || invalidJoins.isEmpty()))
				// cross join? 
				return true;
				*/
		}
		return false;
	}
	
	@Override
	public List<PEStorageGroup> getStorageGroups(SchemaContext pc) {
		return getDerivedInfo().getStorageGroups(pc);
	}
	
	@Override
	public Database<?> getDatabase(SchemaContext pc) {
		ListSet<Database<?>> alldbs = getDerivedInfo().getDatabases(pc);
		if (alldbs.isEmpty()) return null;
		if (alldbs.size() == 1) return alldbs.get(0);
		if (alldbs.size() != 1) {
			Database<?> candidate = pc.getCurrentDatabase(false);
			if (candidate != null) return candidate;
		}
		return alldbs.get(0);
	}
	
	// requires redistribution if there is either more than one partition, or at least one table
	// for which there is no partition
	// well, more specifically, it requires redistribution if there is more than one persistent group
	// (because worker groups are based on persistent groups)
	public boolean requiresRedistribution(SchemaContext sc) {
/*
		ListSet<Partition> partitions = EngineConstant.PARTITIONS.getValue(this,getPersistenceContext());
		ListSet<ColocatedJoin> invalidJoins = EngineConstant.INVALID_JOINS.getValue(this,getPersistenceContext());
		if (partitions != null && partitions.size() > 1) {
			if (!EngineConstant.EQUIJOINS.hasValue(this,getPersistenceContext())) {
				// no joins at all - see if there is more than one persistent group involved
				ListSet<PEStorageGroup> groups = EngineConstant.GROUPS.getValue(this,getPersistenceContext());
				if (groups != null && groups.size() > 1)
					return true;
			} else {
				return true;
			}
		}
		if (invalidJoins != null && invalidJoins.size() > 0)
			return true;
		return false;
		*/
		JoinGraph jg = EngineConstant.PARTITIONS.getValue(this, sc);
		if (jg != null && jg.getPartitions().size() > 1) {
			if (jg.getJoins().isEmpty()) {
				// no joins at all - see if there is more than one persistent group involved
				ListSet<PEStorageGroup> groups = EngineConstant.GROUPS.getValue(this,sc);
				if (groups != null && groups.size() > 1)
					return true;				
			} else {
				return true;
			}
		}
		return false;
	}

	
	protected DerivedInfo buildDerivedInfo() {
		return new DerivedInfo(this);
	}
	
	public abstract List<TableInstance> getBaseTables();
	
	protected List<TableInstance> computeBaseTables(List<FromTableReference> refs) {
		ArrayList<TableInstance> results = new ArrayList<TableInstance>();
		for(FromTableReference ftr : refs)
			if (ftr.getBaseTable() != null)
				results.add(ftr.getBaseTable());
		return results;
	}
	
	protected void addColumns(SchemaContext sc, List<ExpressionNode> into, TableInstance ofTable) {
		final Map<ColumnKey, ExpressionNode> temp = new ListSetMap<ColumnKey, ExpressionNode>();
		addColumns(sc, temp, ofTable, Collections.<ColumnKey> emptySet());
		into.addAll(temp.values());
	}
	
	protected void addColumns(SchemaContext sc, Map<ColumnKey, ExpressionNode> into, TableInstance ofTable, Set<ColumnKey> excluded) {
		boolean showTenantColumn = sc.getPolicyContext().showTenantColumn();
		for (Column<?> c : ofTable.getTable().getColumns(sc)) {
			// now, these columns have no specified name (they are synthetic), so specify none and let the reference figure it out)
			// don't add the tenant column unless the tenant id is null.
			if (!c.isTenantColumn() || showTenantColumn) {
				final ColumnInstance ci = new ColumnInstance(null, c, ofTable);
				final ColumnKey ck = ci.getColumnKey();
				if (!excluded.contains(ck)) {
					into.put(ck, ci);
				}
			}
		}
	}

	public SchemaMapper getMapper() {
		return source;
	}
	
	public void setMapper(SchemaMapper sm) {
		source = sm;
	}
	
	// one of insert, delete, update, select
	public abstract ExecutionType getExecutionType();
	
	public abstract ExecutionStep buildSingleKeyStep(SchemaContext sc, TableKey tab, DistributionKey kv, DMLStatement sql) throws PEException;

	public static final DMLExplainRecord distKeyExplain = DMLExplainReason.DISTRIBUTION_KEY_MATCHED.makeRecord(); 
	
	protected static void planViaTransforms(SchemaContext sc, DMLStatement dmls, ExecutionSequence es, BehaviorConfiguration config) throws PEException {
		// for now, we're going to say dml statements are cacheable - we'll override this later
		// don't cache plans with parameters yet - won't work right for reuse
		if (es.getPlan() != null && !sc.getValueManager().hasPassDownParams()) 
			es.getPlan().setCacheable(true);
		try {
			TransformFactory.featurePlan(sc, dmls, es, config);
		} catch (Throwable t) {
			// see if we can emit something useful here
			StringBuilder buf = new StringBuilder();
			buf.append("Exception encountered planning statement: ").append(PEConstants.LINE_SEPARATOR);
			try {
				buf.append(dmls.getSQL(sc)).append(PEConstants.LINE_SEPARATOR);
				buf.append("Involving table defs").append(PEConstants.LINE_SEPARATOR);
				HashSet<PEAbstractTable<?>> tabs = new HashSet<PEAbstractTable<?>>();
				emitTables(sc,dmls.getDerivedInfo().getAllTableKeys(),tabs,buf);
			} catch (Throwable it) {
				throw new PEException(t);
			}
			throw new PEException(buf.toString(), t);
		}		
	}
	
	protected static void emitTables(SchemaContext sc, Collection<TableKey> tables, Set<PEAbstractTable<?>> tabs, StringBuilder buf) {
		for(TableKey tk : tables) {
			if (!(tk.getTable() instanceof PEAbstractTable)) continue;
			if (tk.getAbstractTable().isVirtualTable()) continue;
			if (!tabs.add(tk.getAbstractTable())) continue;
            Emitter emitter = Singletons.require(HostService.class).getDBNative().getEmitter();
			emitter.setOptions(EmitOptions.PEMETADATA);
			PEAbstractTable<?> peat = tk.getAbstractTable();
			if (peat.isView() && peat.asView().getView(sc).isMerge(sc,peat.asView())) {
				emitter.emitDebugViewDeclaration(sc, peat.asView(), buf);
//				emitter.emitDeclaration(sc, peat.getDistributionVector(sc), buf);
				buf.append(PEConstants.LINE_SEPARATOR);
				ProjectingStatement viewDef = peat.asView().getView(sc).getViewDefinition(sc, peat.asView(),false);
				emitTables(sc,viewDef.getAllTableKeys(),tabs,buf);
			} else {
				buf.append(peat.getDeclaration()).append(" ");
				emitter.emitDeclaration(sc, peat.getDistributionVector(sc), buf);
				buf.append(PEConstants.LINE_SEPARATOR);
			}
		}
	}
	
	@Override
	public void plan(SchemaContext sc, ExecutionSequence es, BehaviorConfiguration config) throws PEException {
		planViaTransforms(sc, this,es, config);
	}
		
	public abstract DistKeyOpType getKeyOpType();

	@Override
	public String toString() {
		return System.identityHashCode(this) + "@ " + super.toString();
	}	
}
