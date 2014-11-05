package com.tesora.dve.queryplan;

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

import java.util.List;

import org.apache.log4j.Logger;

import com.tesora.dve.db.LateBoundConstants;

public class TriggerValueHandlers {

	private static final Logger logger = Logger.getLogger( TriggerValueHandlers.class );
	
	private final TriggerValueHandler[] handlers;
	private final boolean hasTargetHandlers;
	private final boolean hasAfterHandlers;
	
	public TriggerValueHandlers(List<TriggerValueHandler> handlers) {
		this(handlers.toArray(new TriggerValueHandler[0]));
	}
	
	public TriggerValueHandlers(TriggerValueHandler[] handlers) {
		this.handlers = handlers;
		boolean target = false;
		boolean after = false;
		for(TriggerValueHandler tvh : handlers) {
			if (tvh.hasAfter()) after = true;
			if (tvh.hasTarget()) target = true;
		}
		this.hasTargetHandlers = target;
		this.hasAfterHandlers = after;
	}
	
	public LateBoundConstants onBefore(ExecutionState estate, List<String> row) {
		boolean log = logger.isInfoEnabled();
		StringBuilder buf = null;
		if (log) {
			buf = new StringBuilder();
			buf.append("Trigger row: ");
		}
		Object[] out = new Object[row.size()];
		for(int i = 0; i < handlers.length; i++) {
			out[i] = handlers[i].onBefore(estate,row.get(i));
			if (log) {
				if (i > 0) buf.append(", ");
				buf.append("'").append(row.get(i)).append("'");
			}
		}
		if (log)
			logger.debug(buf.toString());
		return new LateBoundConstants(out);
	}
	
	public LateBoundConstants onTarget(ExecutionState estate, LateBoundConstants beforeVals) {
		if (!hasTargetHandlers) return beforeVals;
		Object[] out = new Object[handlers.length];
		for(int i = 0; i < handlers.length; i++) {
			out[i] = handlers[i].onTarget(estate,beforeVals.getConstantValue(i));
		}
		return new LateBoundConstants(out);
	}

	public LateBoundConstants onAfter(LateBoundConstants beforeVals) {
		if (!hasAfterHandlers) return beforeVals;
		Object[] out = new Object[handlers.length];
		for(int i = 0; i < handlers.length; i++) {
			out[i] = handlers[i].onAfter(beforeVals.getConstantValue(i));
		}
		return new LateBoundConstants(out);
	}

	
}
