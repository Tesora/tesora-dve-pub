package com.tesora.dve.sql.parser;

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

import com.tesora.dve.sql.schema.LockInfo;

public final class ParserOptions {

	public static enum Option {
		TRACEPARSER, TRACELEXER, TRACETRANSLATOR, FAILEARLY, RESOLVE, DEBUGLOG, DIAGNOSING_EXCEPTIONS,
		ALLOW_DUPLICATES, ALLOW_TENANT_COLUMN, TSCHEMA, PREPARE, OMIT_METADATA_INJECTION, ACTUAL_LITERALS,
		RAW_PLAN_STEP, DISABLE_MT_LOOKUP_CHECKS, OMIT_TENANT_COLUMN_INJECTION,
		INHIBIT_SINGLE_SITE_OPTIMIZATION, IGNORE_MISSING_USER, DO_NOT_LOCK, LOCK_OVERRIDE,
		SESSION_REWRITE_FORCE_PUSHDOWN, INFOSCHEMA_VIEW, TRIGGER_PLANNING, NESTED_PLAN
	}
	
	public static final ParserOptions NONE = new ParserOptions();
	public static final ParserOptions TEST = NONE.setFailEarly();
	public static final ParserOptions DEBUGPARSER = TEST.setTraceParser();
	public static final ParserOptions DEBUGLEXER = DEBUGPARSER.setTraceLexer();
	public static final ParserOptions DEBUGTRANSLATOR = TEST.setTraceTranslator();
	
	private final Object[] settings;
		
	// no public constructor, use the statics instead
	private ParserOptions() {
		settings = new Object[Option.values().length];
	}
	
	private ParserOptions(ParserOptions other) {
		settings = Arrays.copyOf(other.settings, other.settings.length);
	}
	
	private ParserOptions addSetting(Option o, Object v) {
		ParserOptions ret = new ParserOptions(this);
		ret.settings[o.ordinal()] = v;
		return ret;
	}
	
	private ParserOptions removeSetting(Option o) {
		ParserOptions ret = new ParserOptions(this);
		ret.settings[o.ordinal()] = null;
		return ret;
	}
	
	public boolean hasSetting(Option o) {
		Object e = settings[o.ordinal()]; 
		return (e == null ? false : ((Boolean)e).booleanValue());
	}
	
	public Object getSetting(Option o) {
		return settings[o.ordinal()];
	}
	
	// for temporary stuff
	public ParserOptions addSetting(Option o) {
		return addSetting(o, Boolean.TRUE);
	}
	
	public ParserOptions clearSetting(Option o) {
		return removeSetting(o);
	}
	
	public ParserOptions setTraceParser() {
		return addSetting(Option.TRACEPARSER, Boolean.TRUE);
	}
	
	public ParserOptions setTraceLexer() {
		return addSetting(Option.TRACELEXER, Boolean.TRUE);
	}
	
	public ParserOptions setFailEarly() {
		return addSetting(Option.FAILEARLY, Boolean.TRUE);
	}
	
	public ParserOptions setTraceTranslator() {
		return addSetting(Option.TRACETRANSLATOR, Boolean.TRUE);
	}
	
	public ParserOptions setResolve() {
		return addSetting(Option.RESOLVE, Boolean.TRUE);
	}
	
	public ParserOptions unsetResolve() {
		return removeSetting(Option.RESOLVE);
	}
	
	public ParserOptions setDebugLog(boolean v) {
		return addSetting(Option.DEBUGLOG, Boolean.valueOf(v));
	}
	
	public ParserOptions setAllowDuplicates() {
		return addSetting(Option.ALLOW_DUPLICATES, Boolean.TRUE);
	}
	
	public ParserOptions setAllowTenantColumn() {
		return addSetting(Option.ALLOW_TENANT_COLUMN, Boolean.TRUE);
	}
	
	public ParserOptions setTSchema() {
		return addSetting(Option.TSCHEMA, Boolean.TRUE);
	}
	
	public ParserOptions setPrepare() {
		return addSetting(Option.PREPARE, Boolean.TRUE);
	}
	
	public ParserOptions setOmitMetadataInjection() {
		return addSetting(Option.OMIT_METADATA_INJECTION, Boolean.TRUE);
	}
	
	public ParserOptions setIgnoreMissingUser() {
		return addSetting(Option.IGNORE_MISSING_USER, Boolean.TRUE);
	}

	// when this is on - don't make delegating literals
	// used in pstmt and raw plan support
	public ParserOptions setActualLiterals() {
		return addSetting(Option.ACTUAL_LITERALS, Boolean.TRUE);
	}
	
	public ParserOptions setRawPlanStep() {
		return addSetting(Option.RAW_PLAN_STEP, Boolean.TRUE);
	}
	
	// used in mt support for fks
	public ParserOptions disableMTLookupChecks() {
		return addSetting(Option.DISABLE_MT_LOOKUP_CHECKS, Boolean.TRUE);
	}

	public ParserOptions setOmitTenantColumnInjection() {
		return addSetting(Option.OMIT_TENANT_COLUMN_INJECTION, Boolean.TRUE);
	}

	public ParserOptions setInhibitSingleSiteOptimization() {
		return addSetting(Option.INHIBIT_SINGLE_SITE_OPTIMIZATION, Boolean.TRUE);
	}
	
	public ParserOptions setIgnoreLocking() {
		return addSetting(Option.DO_NOT_LOCK, Boolean.TRUE);
	}
	
	public ParserOptions setLockOverride(LockInfo lockInfo) {
		return addSetting(Option.LOCK_OVERRIDE, lockInfo);
	}
	
	public ParserOptions setForceSessionPushdown() {
		return addSetting(Option.SESSION_REWRITE_FORCE_PUSHDOWN, Boolean.TRUE);
	}
	
	public ParserOptions setInfoSchemaView() {
		return addSetting(Option.INFOSCHEMA_VIEW, Boolean.TRUE);
	}
	
	public ParserOptions setTriggerPlanning() {
		return addSetting(Option.TRIGGER_PLANNING, Boolean.TRUE);
	}
	
	public ParserOptions setNestedPlan() {
		return addSetting(Option.NESTED_PLAN, Boolean.TRUE);
	}
	
	public boolean isTraceParser() {
		return hasSetting(Option.TRACEPARSER);
	}
	
	public boolean isTraceLexer() {
		return hasSetting(Option.TRACELEXER);
	}
	
	public boolean isFailEarly() {
		return hasSetting(Option.FAILEARLY);
	}
	
	public boolean isTraceTranslator() {
		return hasSetting(Option.TRACETRANSLATOR);
	}
	
	public boolean isResolve() {
		return hasSetting(Option.RESOLVE);
	}
	
	public boolean isDebugLog() {
		return hasSetting(Option.DEBUGLOG);
	}
	
	public boolean isAllowDuplicates() {
		return hasSetting(Option.ALLOW_DUPLICATES);
	}
	
	public boolean isAllowTenantColumn() {
		return hasSetting(Option.ALLOW_TENANT_COLUMN);
	}
	
	public boolean isOmitTenantColumnInjection() {
		return hasSetting(Option.OMIT_TENANT_COLUMN_INJECTION);
	}
	
	public boolean isTSchema() {
		return hasSetting(Option.TSCHEMA);
	}
	
	public boolean isPrepare() {
		return hasSetting(Option.PREPARE);
	}
	
	public boolean isOmitMetadataInjection() {
		return hasSetting(Option.OMIT_METADATA_INJECTION);
	}
	
	public boolean isActualLiterals() {
		return hasSetting(Option.ACTUAL_LITERALS);
	}
	
	public boolean isRawPlanStep() {
		return hasSetting(Option.RAW_PLAN_STEP);
	}
	
	public boolean isDisableMTLookupChecks() {
		return hasSetting(Option.DISABLE_MT_LOOKUP_CHECKS);
	}
	
	public boolean isInhibitSingleSiteOptimization() {
		return hasSetting(Option.INHIBIT_SINGLE_SITE_OPTIMIZATION);
	}
	
	public boolean isIgnoreMissingUser() {
		return hasSetting(Option.IGNORE_MISSING_USER);
	}
	
	public boolean isIgnoreLocking() {
		return hasSetting(Option.DO_NOT_LOCK);
	}
	
	public LockInfo getLockOverride() {
		return (LockInfo) getSetting(Option.LOCK_OVERRIDE);
	}

	public boolean isForceSessionPushdown() {
		return hasSetting(Option.SESSION_REWRITE_FORCE_PUSHDOWN);
	}

	public boolean isInfoSchemaView() {
		return hasSetting(Option.INFOSCHEMA_VIEW);
	}
	
	public boolean isTriggerPlanning() {
		return hasSetting(Option.TRIGGER_PLANNING);
	}
	
	public boolean isNestedPlan() {
		return hasSetting(Option.NESTED_PLAN);
	}
	
	public String toString() {
		StringBuilder buf = new StringBuilder();
		buf.append("ParserOptions{");
		boolean first = false;
		for(Option o : Option.values()) {
			Object v = settings[o.ordinal()];
			if (v == null) continue;
			if (!first)
				first = true;
			else
				buf.append(", ");
			buf.append(o).append("='").append(v).append("'");
		}
		buf.append("}");
		return buf.toString();
	}
}
