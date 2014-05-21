// OS_STATUS: public
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

public abstract class MirrorProc extends MirrorTest {

	protected boolean dosys;
	protected boolean docheck;
	protected String description;
	
	public MirrorProc(String desc) {
		this(true,true, desc);
	}
	
	public MirrorProc() {
		this("mirrorproc");
	}
	
	public MirrorProc(boolean sys, boolean check, String desc) {
		dosys = sys;
		docheck = check;
		description = desc;
	}
	
	public abstract ResourceResponse execute(TestResource mr) throws Throwable;
	
	public String describe() {
		return description;
	}
	
	@Override
	public void execute(TestResource checkdb, TestResource sysdb) throws Throwable {
		ResourceResponse checkResponse = null;
		ResourceResponse sysResponse = null;
		if (docheck)
			checkResponse = execute(checkdb);
		if (dosys)
			sysResponse = execute(sysdb);
		if (checkResponse != null && sysResponse != null) 
			checkResponse.assertEqualResponse(describe(), sysResponse);
	}
}