// OS_STATUS: public
package com.tesora.dve.server.connectionmanager;

import org.apache.commons.lang.time.FastDateFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.tesora.dve.common.logutil.ExecutionLogger;
import com.tesora.dve.common.logutil.StructuredExecutionLogger;
import com.tesora.dve.queryplan.QueryPlan;
import com.tesora.dve.sql.schema.SchemaVariables;

public class SlowQueryLogger extends StructuredExecutionLogger {

	private static final Logger slowQueryLogger = Logger.getLogger("slow_query.logger");

	public static void enableSlowQueryLogger(final boolean value) {
		slowQueryLogger.setLevel((value) ? Level.DEBUG : Level.OFF);
	}

	private static final FastDateFormat dateFormatter = FastDateFormat.getInstance("MMM-dd HH:mm:ss");
	
	// enclosing connection
	SSConnection ofConnection;
	
	public SlowQueryLogger(SSConnection conn, QueryPlan qp) {
		super(qp);
		ofConnection = conn;		
	}
	
	@Override
	public void end() {
		super.end();
		ofConnection.clearSlowQueryLogger();
		log();
	}

	@Override
	protected String getSelfLog(int level, String[] tags, StringBuilder buf, int offset, String indent) {
		StringBuilder sub = new StringBuilder();
		sub.append("Plan=").append(getDelta()).append("; conn=").append(ofConnection.getName())
			.append("; startedAt='").append(dateFormatter.format(start)).append("'; sql=")
			.append(getSubject().describeForLog());
		return sub.toString();
	}
	
	private void log() {
		final long planThreshold = SchemaVariables.getLongQueryTimeNanos(null);
		final long stepThreshold = SchemaVariables.getLongPlanStepTimeNanos(null);
		boolean write = false;
		if (planThreshold > 0 && getDelta() > planThreshold) {
			write = true;
		} else if (stepThreshold > 0) {
			for(ExecutionLogger el : getSubLoggers()) {
				if (el.completed() && el.getDelta() > stepThreshold) {
					write = true;
					break;
				}
			}
		}
		if (!write) 
			return;
		// unfortunately we need to buffer the whole thing in memory to avoid interleaving
		// also, make sure logging this thing out never causes an error
		String[] tags = new String[] { "Plan", "Step/Task", "Task" };
		try {
			StringBuilder buf = new StringBuilder();
			accumulateLogMessage(0,tags,buf,0,"");
			slowQueryLogger.debug(buf.toString());
		} catch (Throwable t) {
			slowQueryLogger.warn("Unable to log slow query", t);
		}
	}

}
