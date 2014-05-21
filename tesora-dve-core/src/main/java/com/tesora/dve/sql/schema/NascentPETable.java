package com.tesora.dve.sql.schema;

import java.util.LinkedHashMap;
import java.util.List;

import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.common.catalog.PersistentDatabase;
import com.tesora.dve.common.catalog.PersistentTable;
import com.tesora.dve.common.catalog.TableState;
import com.tesora.dve.common.catalog.UserColumn;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.TempTableDeclHints;
import com.tesora.dve.queryplan.TempTableGenerator;
import com.tesora.dve.resultset.ColumnMetadata;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.server.messaging.ConditionalWorkerRequest;
import com.tesora.dve.server.messaging.ConditionalWorkerRequest.GuardFunction;
import com.tesora.dve.server.messaging.WorkerExecuteRequest;
import com.tesora.dve.server.messaging.WorkerRequest;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.schema.modifiers.TableModifier;
import com.tesora.dve.sql.schema.types.TempColumnType;
import com.tesora.dve.worker.Worker;
import com.tesora.dve.worker.WorkerGroup;

// A nascent pe table is one which we need to act like it's already persistent, even though it's not
// This is used in create table as select support.
public class NascentPETable extends PETable {

	// we use table generator at runtime
	
	public NascentPETable(SchemaContext pc, Name name, 
			List<TableComponent<?>> fieldsAndKeys, DistributionVector dv, List<TableModifier> modifier, 
			PEPersistentGroup defStorage, PEDatabase db,
			TableState theState) {
		super(pc,name,fieldsAndKeys,dv,modifier,defStorage,db,theState);
	}

	@Override
	public boolean mustBeCreated() {
		return true;
	}
	
	public TempTableGenerator getTableGenerator(SchemaContext sc) {
		return new CTATableGenerator(sc);
	}
	
	protected class CTATableGenerator extends TempTableGenerator {
		
		protected final SchemaContext context;
		
		public CTATableGenerator(SchemaContext cntxt) {
			this.context = cntxt;
		}
		
		public UserTable createTableFromMetadata(WorkerGroup targetWG, 
				TempTableDeclHints tempHints, String tempTableName,
				PersistentDatabase database, ColumnSet metadata, DistributionModel tempDist) throws PEException {

			for(ColumnMetadata cm : metadata.getColumnList()) {
				UserColumn uc = new UserColumn(cm);
				if ( cm.usingAlias() ) 
					uc.setName(cm.getAliasName());
				PEColumn pec = PEColumn.build(context,uc);
				PEColumn existing = lookup(context,pec.getName());
				if (existing != null && existing.getType().equals(TempColumnType.TEMP_TYPE))
					existing.setType(pec.getType());
			}

			context.beginSaveContext();
			try {
				return getPersistent(context);
			} finally {
				context.endSaveContext();
			}
			
		}
		
		public String buildCreateTableStatement(UserTable theTable, boolean useSystemTempTable) throws PEException {
			String out = Singletons.require(HostService.class).getDBNative().getEmitter().emitCreateTableStatement(context, NascentPETable.this);
			return out;
		}
		
		public void addCleanupStep(SSConnection ssCon, UserTable theTable, PersistentDatabase database, WorkerGroup cleanupWG) {
			WorkerRequest wer = new WorkerExecuteRequest(
							ssCon.getNonTransactionalContext(), 
							UserTable.getDropTableStmt(theTable.getName(), false)).onDatabase(database);
			cleanupWG.addCleanupStep(
					new ConditionalWorkerRequest(wer,
							new GuardFunction() {

								@Override
								public boolean proceed(Worker w,
										DBResultConsumer consumer)
										throws PEException {
									// we should make this dependent on the execution!
									return false;
								}
						
					}));
			
		}		

		
	}
	
}
