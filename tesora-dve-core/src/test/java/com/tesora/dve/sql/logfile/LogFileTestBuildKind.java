package com.tesora.dve.sql.logfile;

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

public enum LogFileTestBuildKind {
	// None would indicate this test won't run
	NONE,
	// global build kinds
	// coverage build
	COVERAGE,
	// native multi test - all enabled native multi tests
	NATIVE,
	SINGLE,
	MULTI,
	NATIVEMULTI,
	// perf related - generally for comparing native to single
	PERF,
	// any mt test
	MT,
	// ben's perf tests
	B_PERF,
	// grouping for that wierd perms problem
	PERMS_REPRO,
	// build kinds to cause the IT Build to run parallel in Jenkins
	B_GROUP1,
	B_GROUP2,
	B_GROUP3,
	B_GROUPNM1,
	B_GROUPNM2,
	B_GROUPNM3,
	B_GROUPNM4,
	B_JTPCC1,
	B_JTPCC2, REDIST_PERF, REDIST_MULTI,
	B_MAGENTO_SINGLE,
	B_MAGENTO_MULTI
	;
		
	public static LogFileTestBuildKind fromCommandLine(String in) {
		String t = in.trim().toUpperCase();
		return LogFileTestBuildKind.valueOf(t);
	}
	
}
