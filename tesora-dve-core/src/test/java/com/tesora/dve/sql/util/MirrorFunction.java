package com.tesora.dve.sql.util;

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

import com.tesora.dve.sql.parser.ParserInvoker.LineInfo;

public class MirrorFunction extends MirrorApply {

	private boolean unorderedCompare;
	
	public MirrorFunction(LineInfo info, String stmt, MirrorExceptionHandler parent, TestName ctest, boolean unordered, boolean ignoreResults) {
		super(info,stmt, parent, ctest, ignoreResults);
		unorderedCompare = unordered;
	}

	@Override
	public void execute(TestResource check, TestResource sys)
			throws Throwable {
		String message = "Failure at line " + info.getLineNumber() + "; stmt='" + stmt + "':";
		ResourceResponse checkResponse = null;
		ResourceResponse sysResponse = null;
		Exception checkException = null;
		Exception sysException = null;
		if (check != null) try {
			checkResponse = check.getConnection().fetch(info, stmt);
		} catch (Exception e) {
			checkException = e;
		}
		if (sys != null) try {
			sysResponse = sys.getConnection().fetch(info, stmt);
		} catch (Exception e) {
			sysException = e;
		}
		if (!checkResponse(message, check, sys, checkResponse, checkException, sysResponse, sysException))
			return;
		// from now on, only if we have both
		if (checkResponse == null || sysResponse == null || ignoreResponse)
			return;
		ComparisonOptions options = ComparisonOptions.DEFAULT;
		if (unorderedCompare)
			options = options.withIgnoreOrder();
		checkResponse.assertEqualResults(message, sysResponse, options);
	}		
}