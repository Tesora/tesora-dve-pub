// OS_STATUS: public
package com.tesora.dve.sql.statement.session;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.apache.commons.lang.builder.CompareToBuilder;

import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStepGeneralOperation.AdhocOperation;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultChunk;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.worker.agent.Envelope;
import com.tesora.dve.server.statistics.manager.GetStatisticsRequest;
import com.tesora.dve.server.statistics.manager.GetStatisticsResponse;
import com.tesora.dve.server.statistics.SiteStatKey;
import com.tesora.dve.server.statistics.TimingSet;
import com.tesora.dve.server.statistics.SiteStatKey.OperationClass;
import com.tesora.dve.server.statistics.SiteStatKey.SiteType;
import com.tesora.dve.sql.schema.PEProvider;
import com.tesora.dve.sql.schema.PEStorageSite;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.TransientSessionExecutionStep;
import com.tesora.dve.worker.WorkerGroup;

public class ShowSitesStatusStatement extends SessionStatement {

	final Map<String,TimingSet> allSites = new TreeMap<String,TimingSet>();

	public ShowSitesStatusStatement() {
		super();
	}

	@Override
	public void plan(SchemaContext pc, ExecutionSequence es, BehaviorConfiguration config) throws PEException {
		for(PEStorageSite p : pc.findAllPersistentSites())
			allSites.put(SiteType.PERSISTENT.name() + p.getName(), 
					new TimingSet(new SiteStatKey(SiteType.PERSISTENT, p.getName().get(), OperationClass.TOTAL)));
		
		for(StorageSite ss : PEProvider.findAllDynamicSites(pc))
			allSites.put(SiteType.DYNAMIC.name() + ss.getName(), 
					new TimingSet(new SiteStatKey(SiteType.DYNAMIC, ss.getName(), OperationClass.TOTAL)));
		
		es.append(new TransientSessionExecutionStep("SHOW SITES STATUS",
				new AdhocOperation() {

					@Override
					public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable {
						try {
							GetStatisticsRequest req = new GetStatisticsRequest();
                            Envelope e = ssCon.newEnvelope(req).to(Singletons.require(HostService.class).getStatisticsManagerAddress());
							ResponseMessage rm = ssCon.sendAndReceive(e);
							
							GetStatisticsResponse gsr = (GetStatisticsResponse) rm;
							ResultChunk chunk = new ResultChunk();
							// put the Global query results in the result set
							addTimingSetToChunk(gsr.getStats().getGlobalQuery(), chunk);
							// put the Global update results in the result set
							addTimingSetToChunk(gsr.getStats().getGlobalUpdate(), chunk);
							// put the Site stats in the result set
							for ( TimingSet ts : gsr.getStats().getAllSiteTimings() ) {
								addTimingSetToChunk(ts, chunk);
							}
							
							// add the remaining allsites to the result set
							for ( Map.Entry<String,TimingSet> entry : allSites.entrySet() )
								addTimingSetToChunk(entry.getValue(), chunk);
							
							chunk.sort(new Comparator<ResultRow>() {
								@Override
								public int compare(ResultRow row1, ResultRow row2) {
									return new CompareToBuilder()
											.append((String) row1.getResultColumn(1).getColumnValue(),
													(String) row2.getResultColumn(1).getColumnValue())
											.append((String) row1.getResultColumn(2).getColumnValue(),
													(String) row2.getResultColumn(2).getColumnValue())
											.append((String) row1.getResultColumn(3).getColumnValue(),
													(String) row2.getResultColumn(3).getColumnValue())
											.toComparison();
								}
							});
							
							resultConsumer.inject(getColumnSet(), chunk.getRowList());
							
						} catch (Exception e) {
							throw new PEException(
									"Unable to plan SHOW SITES STATUS", e);
						}
					}
				}));

	}

	void addTimingSetToChunk(TimingSet ts, ResultChunk chunk) {
		// Add this data to the totals if it is from a site
		if ( ts.getStatKey().getType() != SiteType.GLOBAL ) {
			TimingSet totalTs = allSites.get(ts.getType() + ts.getName());
			if (totalTs != null)
				totalTs.getCumulative().addToValues(ts.getCumulative().getResponseTimeMS(),
						ts.getCumulative().getSampleSize());
			else // the site returned in the stats no longer exists so don't add it to the result set
				return;
		}
		
		ResultRow row = new ResultRow();
		row.addResultColumn(ts.getType());
		row.addResultColumn(ts.getName());
		row.addResultColumn(ts.getOperationClass());
		row.addResultColumn( (long) ts.getCumulative().getResponseTimeMS());
		row.addResultColumn(ts.getCumulative().getSampleSize());
		BigDecimal avgResp = new BigDecimal(0);
		if ( ts.getCumulative().getSampleSize() != 0 ) {
			double avgRespCalc = ts.getCumulative().getResponseTimeMS() / ts.getCumulative().getSampleSize();
			avgResp = new BigDecimal((new DecimalFormat("#.##").format(avgRespCalc)));
		}
		row.addResultColumn(avgResp);
		chunk.addResultRow(row);
		
	}
	
	private ColumnSet getColumnSet() {
		ColumnSet cs = new ColumnSet();
		cs.addColumn("Type", 25, "varchar", java.sql.Types.VARCHAR);
		cs.addColumn("Name", 255, "varchar", java.sql.Types.VARCHAR);
		cs.addColumn("Operation", 25, "varchar", java.sql.Types.VARCHAR);
		cs.addColumn("Cumulative_Response_Time", 11, "bigint", java.sql.Types.BIGINT);
		cs.addColumn("Count", 11, "bigint", java.sql.Types.BIGINT);
		cs.addColumn("Avg_Response_Time", 15, "decimal", java.sql.Types.NUMERIC, 10, 2);

		return cs;
	}
}
