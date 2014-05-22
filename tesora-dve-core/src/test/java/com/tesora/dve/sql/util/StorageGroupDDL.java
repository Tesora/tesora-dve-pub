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


import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.tesora.dve.common.catalog.TestCatalogHelper;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.SchemaTest;
import com.tesora.dve.sql.SchemaTest.TempTableChecker;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.UnaryFunction;

public class StorageGroupDDL extends TestDDL {
	
	private String siteKern;
	private int nsites;
	private String name;
	private int holdBack;
	private boolean throwDropGroupInUseException = true;
	
	public StorageGroupDDL(String kern, int maxsites, String groupName) {
		this(kern, maxsites, 0, groupName);
	}
	
	public StorageGroupDDL(String kern, int maxsites, int holdback, String groupName) {
		siteKern = kern;
		nsites = maxsites;
		name = groupName;
		this.holdBack = holdback;
	}
	
	public String getName() {
		return name;
	}
	
	public String getAddGenerations() throws Exception {
		if (holdBack == 0 || (nsites - holdBack) == 0)
			return null;
		ArrayList<String> names = new ArrayList<String>();
		for(int i = 1; i <= holdBack; i++) {
			String siteName = siteKern + (nsites - i);
			names.add(siteName);
		}
		return "alter persistent group " + name + " add generation " 
			+ Functional.join(names, ",");
	}
	
	@Override
	public List<String> getCreateStatements() throws Exception {
		ArrayList<String> buf = new ArrayList<String>();
		if (isCreated())
			return buf;
		ArrayList<String> siteNames = new ArrayList<String>();
		for(int i = 0; i < nsites; i++) {
			String sname = siteKern + i;
			siteNames.add(sname);
			// create all the sites up front
			buf.add("create persistent site "
					+ sname 
					+ " url='" + TestCatalogHelper.getInstance().getCatalogUrl()
					+ "' user='" + TestCatalogHelper.getInstance().getCatalogUser()
					+ "' password='" + TestCatalogHelper.getInstance().getCatalogPassword()
					+ "'");
		}
		if (holdBack > 0 && siteNames.size() > holdBack) {
			// we're going to hold back the trailing sites
			int lim = siteNames.size() - holdBack;
			while(siteNames.size() > lim)
				siteNames.remove(siteNames.size() - 1);
		}
		buf.add("create persistent group " + name + " add " + Functional.join(siteNames, ","));
		setCreated();
		return buf;
	}
	
	public List<String> getSetupDrops(String dbname) {
		return Functional.apply(getPhysicalSiteNames(dbname), new UnaryFunction<String, String>() {

			@Override
			public String evaluate(String object) {
				return "DROP DATABASE IF EXISTS " + object;
			}
			
		});
	}
	
	public List<String> getPhysicalSiteNames(String dbname) {
		ArrayList<String> buf = new ArrayList<String>();
		for(int i = 0; i < nsites; i++) {
			buf.add(SchemaTest.buildDBName(siteKern,i,dbname));
		}
		return buf;
	}
	
	public TempTableChecker buildTempTableChecker(String dbName) {
		return new TempTableChecker(siteKern, nsites, dbName);
	}

	@Override
	public List<String> getDestroyStatements() throws Exception {
		return null;
	}

	@Override
	public List<String> getSetupDrops() {
		throw new IllegalStateException("persistent group does not have setup drops");
	}
	
	@Override
	public void destroy(ConnectionResource mr) throws Throwable {
		if (!isCreated()) return;
		try {
			// drop the group
			mr.execute("drop persistent group " + name);
			// and also drop all the sites
			for(int i = 0; i < nsites; i++)
				mr.execute("drop persistent site " + siteKern + i);
			clearCreated();
		} catch (PEException e) {
			if (!throwDropGroupInUseException && 
					(StringUtils.containsIgnoreCase(e.getMessage(), "Unable to drop persistent group") &&
							StringUtils.containsIgnoreCase(e.getMessage(), "because used by database"))) {
				// eat the exception
			} else {
				throw e;
			}
		}
	}
	
	public StorageGroupDDL setThrowDropGroupInUseException(boolean value) {
		this.throwDropGroupInUseException = value;
		return this;
	}
}