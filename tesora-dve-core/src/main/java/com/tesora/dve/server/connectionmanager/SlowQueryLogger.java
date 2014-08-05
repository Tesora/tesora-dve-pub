package com.tesora.dve.server.connectionmanager;

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

import org.apache.commons.lang.time.FastDateFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.tesora.dve.common.logutil.ExecutionLogger;
import com.tesora.dve.common.logutil.StructuredExecutionLogger;
import com.tesora.dve.queryplan.QueryPlan;
import com.tesora.dve.sql.schema.VariableScopeKind;
import com.tesora.dve.variables.KnownVariables;

public class SlowQueryLogger extends StructuredExecutionLogger {

	private static final long NANOS_PER_SECOND = 1000000000;
	
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
		final long planThreshold = 
				Math.round(KnownVariables.LONG_QUERY_TIME.getValue(ofConnection,VariableScopeKind.SESSION).doubleValue() * NANOS_PER_SECOND);
		final long stepThreshold = 
				Math.round(KnownVariables.LONG_PLAN_STEP_TIME.getValue(ofConnection).doubleValue() * NANOS_PER_SECOND);
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
