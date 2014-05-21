package com.tesora.dve.sql.statement.session;

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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.Key;
import com.tesora.dve.common.catalog.KeyColumn;
import com.tesora.dve.common.catalog.User;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.common.catalog.CatalogDAO.CatalogDAOFactory;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.db.GenericSQLCommand;
import com.tesora.dve.dbc.ServerDBConnection;
import com.tesora.dve.distribution.BroadcastDistributionModel;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryPlanner;
import com.tesora.dve.queryplan.QueryStepFilterOperation.OperationFilter;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.schema.cache.InvalidationScope;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.FilterExecutionStep;
import com.tesora.dve.sql.transform.execution.ProjectingExecutionStep;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.MapOfMaps;

public class AnalyzeKeysStatement extends SessionStatement {

	private List<TableInstance> tables;
	
	public AnalyzeKeysStatement(List<TableInstance> tabs) {
		super();
		tables = tabs;
	}
	
	public List<TableInstance> getTables() {
		return tables;
	}
	
	@Override
	public boolean isPassthrough() {
		return false;
	}
	
	@Override
	public void plan(SchemaContext pc, ExecutionSequence es) throws PEException {
		// sort the table instances by database
		MultiMap<Database<?>,PETable> bydb = new MultiMap<Database<?>,PETable>();
		for(TableInstance ti : tables) {
			if (ti.getAbstractTable().isView()) continue;
			bydb.put(ti.getAbstractTable().getDatabase(pc), ti.getAbstractTable().asTable());
		}
		for(Database<?> db : bydb.keySet()) {
			ListSet<PETable> tabs = new ListSet<PETable>(bydb.get(db));
			String toAdd = db.getName().getUnquotedName().getSQL();
			while(!tabs.isEmpty()) {
				PEStorageGroup curgroup = null;
				List<PETable> curset = new ArrayList<PETable>();
				for(Iterator<PETable> iter = tabs.iterator(); iter.hasNext();) {
					PETable tab = iter.next();
					if (curgroup == null) {
						curgroup = tab.getPersistentStorage(pc);
						curset.add(tab);
						iter.remove();
					} else if (curgroup.equals(tab.getPersistentStorage(pc))) {
						curset.add(tab);
						iter.remove();
					}
				}
				StringBuilder buf = new StringBuilder();
				GenericSQLCommand.Builder builder = new GenericSQLCommand.Builder();
				buf.append("select table_name, index_name, column_name, cardinality from information_schema.statistics where table_schema = '");
				int offset = buf.length();
				buf.append(toAdd).append("' and table_name in (");
				ListOfPairs<SchemaCacheKey<?>,InvalidationScope> toInvalidate = new ListOfPairs<SchemaCacheKey<?>,InvalidationScope>();
				for(Iterator<PETable> iter = curset.iterator(); iter.hasNext();) {
					PETable tab = iter.next();
					buf.append("'").append(tab.getName(pc).getUnqualified().getUnquotedName().get()).append("'");
					if (iter.hasNext()) buf.append(",");
					toInvalidate.add(tab.getCacheKey(),InvalidationScope.LOCAL);
				}
				buf.append(")");
				builder.withDBName(offset, toAdd);
				GenericSQLCommand gsql = builder.build(buf.toString());
				ProjectingExecutionStep pes = ProjectingExecutionStep.build(db, curgroup, gsql);
				FilterExecutionStep fes = new FilterExecutionStep(pes, new StoreStatisticsFilter(db.getId(),new CacheInvalidationRecord(toInvalidate)));
				es.append(fes);
			}
		}
	}
	
	// we need to use the emitter builder to get the right stuff
	// the query we want is 
	// select table_schema, table_name, index_name, column_name, cardinality 
	// from information_schema.statistics
	// where table_schema like '<name on site>' and table_name in (?,....)
	// 
	// the table_name values will be the same on every site, but the table_schema won't be
	//
	
	private static class StoreStatisticsFilter implements OperationFilter {

		private final int dbid;
		private final CacheInvalidationRecord record;
		
		public StoreStatisticsFilter(int databaseID, CacheInvalidationRecord rec) {
			this.dbid = databaseID;
			this.record = rec;
		}

		@Override
		public void filter(SSConnection ssCon, ColumnSet columnSet,	List<ArrayList<String>> rowData, DBResultConsumer results)
				throws Throwable {
			// table_name, index_name, <column_name, cardinality>>
			MapOfMaps<String,String,MultiMap<String,Long>> lookup = new MapOfMaps<String,String, MultiMap<String,Long>>();
			for(List<String> row : rowData) {
				// recall that the column set is:
				// table name, index name, column name, cardinality
				String tabName = row.get(0);
				String indexName = row.get(1);
				String columnName = row.get(2);
				String rawCard = row.get(3);
				if (rawCard == null) continue;
				if ("NULL".equals(rawCard)) continue;
				Long cardinality = Long.valueOf(rawCard);
				MultiMap<String,Long> columnCard = lookup.get(tabName,indexName);
				if (columnCard == null) {
					columnCard = new MultiMap<String,Long>();
					lookup.put(tabName,indexName,columnCard);
				}
				columnCard.put(columnName, cardinality);
			}
			CatalogDAO c = ssCon.getCatalogDAO();
			c.begin();
			try {
				for(String tabName : lookup.keySet()) {
					Map<String,MultiMap<String,Long>> indices = lookup.get(tabName);
					UserTable tab = c.findUserTable(dbid, tabName, false);
					if (tab == null) continue;
					boolean bcast = tab.getDistributionModel().equals(BroadcastDistributionModel.MODEL_NAME);
					for(Key k : tab.getKeys()) {
						if (k.isForeignKey()) continue;
						// should figure out which is returned
						MultiMap<String,Long> any = indices.get(k.getSymbol());
						if (any == null) any = indices.get(k.getName());
						if (any == null) continue;
						Map<String,KeyColumn> keycols = new HashMap<String,KeyColumn>();
						for(KeyColumn kc : k.getColumns()) {
							keycols.put(kc.getSourceColumn().getName(), kc);
						}
						// for broadcast we take the sum of the values and divide by the number of them
						// for all others, just the sum.  so either way we need the sum, figure that out now.
						for(String cn : any.keySet()) {
							KeyColumn kc = keycols.get(cn);
							if (kc == null) continue;
							long divisor = 0;
							long sum = 0;
							for(Long l : any.get(cn)) {
								sum += l.longValue();
								divisor++;
							}
							long result = sum;
							if (bcast) 
								result = Math.round((1.0 * sum)/divisor);
							kc.setCardinality(result);
						}
					}
				}
				c.commit();
			} catch (Throwable t) {
				c.rollback(t);
			} finally {
				QueryPlanner.invalidateCache(record);				
			}
		}

		@Override
		public String describe() {
			return "StoreStatisticsFilter(" + record + ")";
		}
		
	}

	public static class LoadKeyInfo implements Callable<Void> {

		private final String useStmt;
		private final String analyzeStmt;
		
		public LoadKeyInfo(String useDB, String analyze) {
			this.useStmt = useDB;
			this.analyzeStmt = analyze;
		}
		
		@Override
		public Void call() throws Exception {
			CatalogDAO c = null;
			String rootName, rootPass;
			try {
				c = CatalogDAOFactory.newInstance();

				User rootUser = c.findDefaultProject().getRootUser();
				rootName = rootUser.getName();
				rootPass = rootUser.getPlaintextPassword();
			} finally {
				c.close();
				c = null;
			}
			ServerDBConnection conn = null;
			try {
				conn = new ServerDBConnection(rootName,rootPass);
				conn.execute(useStmt);
				conn.execute(analyzeStmt);
			} finally {
				if (conn != null) {
					conn.closeComms();
				}
			}
			return null;
		}
		
	}
	
}
