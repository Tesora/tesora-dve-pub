package com.tesora.dve.eventing;

import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.common.PEConstants;

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

public abstract class AbstractEvent {

	protected final EventSource generatingState;
	
	protected AbstractEvent(EventSource s) {
		generatingState = s;
	}
	
	public abstract boolean isRequest();

	public String toString() {
		String n = getClass().getName();
		int lastDot = n.lastIndexOf(".");
		return System.identityHashCode(this) + "@" + n.substring(lastDot+1);
	}
	
	public String getHistory() {
		ArrayList<HistoryEntry> entries = new ArrayList<HistoryEntry>();
		collectHistory(entries,0);
		StringBuilder buf = new StringBuilder();
		for(HistoryEntry he : entries) {
			buf.append(he.getLevel()).append(he.getEntry()).append(PEConstants.LINE_SEPARATOR);
		}
		return buf.toString();
	}
	
	public abstract void collectHistory(List<HistoryEntry> entries, int nestLevel);
}
