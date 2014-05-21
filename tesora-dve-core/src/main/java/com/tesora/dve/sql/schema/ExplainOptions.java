// OS_STATUS: public
package com.tesora.dve.sql.schema;

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

import java.util.Arrays;

public class ExplainOptions {

	public static enum ExplainOption {
	
		NONE(true),
		RAW(true),
		NOPLAN(false),
		STATISTICS(true),
		PLANCACHE(true); 
		
		private final boolean trycache;
		private ExplainOption(boolean tc) {
			trycache = tc;
		}
		
		public boolean canTryCache() {
			return trycache;
		}
		
		public static ExplainOption find(UnqualifiedName unq) {
			String lf = unq.getUnquotedName().get();
			for(ExplainOption eo : ExplainOption.values()) {
				if (eo.name().equalsIgnoreCase(lf))
					return eo;
			}
			return null;
		}
	}
	
	public static final ExplainOptions NONE = new ExplainOptions();
	
	private final Object[] values;
	
	private ExplainOptions() {
		values = new Object[ExplainOption.values().length];
	}
	
	private ExplainOptions(ExplainOptions other) {
		values = Arrays.copyOf(other.values, other.values.length);
	}
	
	public ExplainOptions addSetting(ExplainOption o, Object v) {
		ExplainOptions ret = new ExplainOptions(this);
		ret.values[o.ordinal()] = v;
		return ret;
	}
	
	public boolean hasSetting(ExplainOption o) {
		Object e = values[o.ordinal()];
		if (e == null) return false;
		if (e instanceof Boolean)
			return ((Boolean)e).booleanValue();
		return true;
	}
	
	public Object getSetting(ExplainOption o) {
		return values[o.ordinal()];
	}

	public boolean isRaw() {
		return hasSetting(ExplainOption.RAW);
	}
	
	public boolean isStatistics() {
		return hasSetting(ExplainOption.STATISTICS);
	}
	
	public ExplainOptions setStatistics() {
		return addSetting(ExplainOption.STATISTICS,true);
	}
	
	public boolean tryCache() {
		return !hasSetting(ExplainOption.NOPLAN);
	}

	public boolean isPlanCache() {
		return hasSetting(ExplainOption.PLANCACHE);
	}
	
	public ExplainOptions setPlanCache() {
		return addSetting(ExplainOption.PLANCACHE,true);
	}
	
}
