package com.tesora.dve.sql.statement.ddl;

import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.db.DBEmptyTextResultConsumer;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStepRebalance;
import com.tesora.dve.queryplan.QueryStepDDLNestedOperation.NestedOperationDDLCallback;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PEPersistentGroup;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.PEViewTable;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.RangeDistribution;
import com.tesora.dve.sql.schema.RangeDistributionVector;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.execution.ComplexDDLExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.worker.WorkerGroup;

public class StartRebalanceStatement extends DDLStatement {

	PEPersistentGroup onGroup;
	
	public StartRebalanceStatement(PEPersistentGroup grp) {
		super(true);
		this.onGroup = grp;
	}

	@Override
	public Action getAction() {
		return Action.ALTER;
	}

	@Override
	public Persistable<?, ?> getRoot() {
		// no root as yet
		return null;
	}

	@Override
	public CacheInvalidationRecord getInvalidationRecord(SchemaContext sc) {
		return CacheInvalidationRecord.GLOBAL;
	}

	@Override
	public void plan(SchemaContext sc, ExecutionSequence es, BehaviorConfiguration config) throws PEException {
		MultiMap<PEDatabase,PETable> plain = new MultiMap<PEDatabase,PETable>();
		MultiMap<PEDatabase,PETable> fks = new MultiMap<PEDatabase,PETable>();
		MultiMap<PEDatabase,PEViewTable> views = new MultiMap<PEDatabase,PEViewTable>();
		AddStorageSiteStatement.sortTablesOnGroup(sc,onGroup,plain,fks,views);			
		ListSet<PEDatabase> dbs = new ListSet<PEDatabase>();
		dbs.addAll(views.keySet());
		dbs.addAll(plain.keySet());
		dbs.addAll(fks.keySet());
		Pair<List<PEAbstractTable<?>>,Boolean> tableDeclOrder = AddStorageSiteStatement.buildTableDeclOrder(sc,plain,fks);
		MultiMap<RangeDistribution,PETable> tablesByRange = new MultiMap<RangeDistribution,PETable>();
		for(PEAbstractTable<?> abst : tableDeclOrder.getFirst()) {
			if (abst.isView()) continue;
			PETable pet = abst.asTable();
			if (pet.getDistributionVector(sc).isRange()) {
				RangeDistributionVector rdv = (RangeDistributionVector) pet.getDistributionVector(sc);
				tablesByRange.put(rdv.getRangeDistribution().getDistribution(sc),pet);
			}
		}
		List<AddStorageGenRangeInfo> rangeInfo = buildRangeInfo(sc,tablesByRange);
		/*
		for(AddStorageGenRangeInfo info : rangeInfo) {
			info.display(System.out);
		}
		*/
		es.append(new ComplexDDLExecutionStep(null,onGroup,null,Action.ALTER,
				new RebalanceCallback(onGroup,rangeInfo,tableDeclOrder.getSecond())));		
	}
	
	private List<AddStorageGenRangeInfo> buildRangeInfo(SchemaContext sc, MultiMap<RangeDistribution,PETable> tablesByRange) {
		List<AddStorageGenRangeInfo> out = new ArrayList<AddStorageGenRangeInfo>();
		for(RangeDistribution rd : tablesByRange.keySet()) 
			out.add(new AddStorageGenRangeInfo(sc,rd,(List<PETable>) tablesByRange.get(rd)));
		return out;
	}

	private static class RebalanceCallback extends NestedOperationDDLCallback {

		private final List<AddStorageGenRangeInfo> rangeInfo;
		private final PEPersistentGroup onGroup;
		private final boolean ignoreFKS;
		
		public RebalanceCallback(PEPersistentGroup onGroup, List<AddStorageGenRangeInfo> rangeInfo, boolean ignoreFKS) {
			this.onGroup = onGroup;
			this.rangeInfo = rangeInfo;
			this.ignoreFKS = ignoreFKS;
		}
		
		@Override
		public void executeNested(SSConnection conn, WorkerGroup wg,
				DBResultConsumer resultConsumer) throws Throwable {
			SchemaContext cntxt = conn.getSchemaContext();
			cntxt.beginSaveContext();
			PersistentGroup pg = null;
			try {
				pg = onGroup.persistTree(cntxt);
			} finally {
				cntxt.endSaveContext();
			}
            QueryStepRebalance rangeRebalance = new QueryStepRebalance(pg,rangeInfo,ignoreFKS);
            rangeRebalance.execute(conn, wg, DBEmptyTextResultConsumer.INSTANCE);
		}

		@Override
		public String description() {
			return "rebalance operation";
		}

		@Override
		public CacheInvalidationRecord getInvalidationRecord() {
			return CacheInvalidationRecord.GLOBAL;
		}
		
	}
}
