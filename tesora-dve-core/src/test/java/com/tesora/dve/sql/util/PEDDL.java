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
import java.util.HashSet;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.tesora.dve.common.catalog.FKMode;
import com.tesora.dve.common.catalog.MultitenantMode;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ResultColumn;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.parser.ParserInvoker.LineTag;
import com.tesora.dve.sql.parser.ParserInvoker.TaggedLineInfo;

public class PEDDL extends ProjectDDL {
	
	private List<StorageGroupDDL> persGroups;
	private boolean tenant = false;
	private boolean throwDropRangeInUseException = true;
	private List<String> templateDeclarations = new ArrayList<String>();
	
	public PEDDL() {
		super();
		persGroups = new ArrayList<StorageGroupDDL>();
	}
	
	public PEDDL(PEDDL other) {
		super(other);
		persGroups = other.persGroups;
		this.templateDeclarations = new ArrayList<String>(other.templateDeclarations);
	}
	
	@Override
	public StorageGroupDDL getPersistentGroup() {
		return persGroups.get(0);
	}
	
	public PEDDL withStorageGroup(StorageGroupDDL sgddl) {
		persGroups.add(sgddl);
		return this;
	}
	
	@Override
	public List<String> getCreateStatements() throws Exception {
		ArrayList<String> buf = new ArrayList<String>();
		if (!tenant) {
			for(StorageGroupDDL sgddl : persGroups) {
				buf.addAll(sgddl.getCreateStatements());
			}
			buf.addAll(templateDeclarations);
			for(DatabaseDDL dbddl : getDatabases()) {
				buf.addAll(dbddl.getCreateStatements());
			}
			setCreated();
		}
		buf.add("use " + getDatabases().get(0).getDatabaseName());
		return buf;
	}

	// our destroy is now slightly more complicated - in addition to dropping the db
	// we also have to drop ranges, persistent groups
	@Override
	public void destroy(ConnectionResource mr) throws Throwable {
		if(mr == null)
			return;
		
		TaggedLineInfo tli = new TaggedLineInfo(-1,null,-1,LineTag.DDL);
		// drop all the dbs
		for(DatabaseDDL dbddl : getDatabases())
			mr.execute(tli, dbddl.getDropStatement());
		HashSet<String> pgnames = new HashSet<String>();
		for(StorageGroupDDL sgddl : persGroups)
			pgnames.add(sgddl.getName());
		ResourceResponse rr = mr.fetch("show containers");
		List<ResultRow> results = rr.getResults();
		for(ResultRow row : results) {
			ResultColumn rc = row.getResultColumn(1);
			String contName = (String) rc.getColumnValue();
			mr.execute(tli,"drop container " + contName);
		}
		// now we have to find ranges that use our persistent group
		rr = mr.fetch("show ranges");
		results = rr.getResults();
		for(ResultRow row : results) {
			ResultColumn rangeNameColumn = row.getResultColumn(1);
			ResultColumn storageGroupColumn = row.getResultColumn(2);
			String rangeName = rangeNameColumn.getColumnValue().getClass().isArray() ? 
					new String((byte[])rangeNameColumn.getColumnValue()) : (String)rangeNameColumn.getColumnValue();
			String groupName = storageGroupColumn.getColumnValue().getClass().isArray() ? 
					new String((byte[])storageGroupColumn.getColumnValue()) : (String ) storageGroupColumn.getColumnValue();
			if (pgnames.contains(groupName.trim())) {
				try {
					mr.execute(tli,"drop range " + rangeName + " persistent group " + groupName.trim());
				} catch (PEException e) {
					if (!throwDropRangeInUseException && (StringUtils.containsIgnoreCase(e.getMessage(), "Unable to drop range") &&
							StringUtils.containsIgnoreCase(e.getMessage(), "because used by table"))) {
						// eat the exception
					} else {
						throw e;
					}
				}
			}
		}
		// now we can drop the persistent group, which apparently also drops the persistent sites
		for(StorageGroupDDL sgddl : persGroups) {
			sgddl.destroy(mr);
		}
	}

	@Override
	public List<String> getSetupDrops() {
		ArrayList<String> buf = new ArrayList<String>();
		ArrayList<String> nativeDrops = new ArrayList<String>();
		for(DatabaseDDL dbddl : getDatabases()) {
			buf.addAll(dbddl.getSetupDrops());
			for(StorageGroupDDL sgddl : persGroups) {
				nativeDrops.addAll(sgddl.getSetupDrops(dbddl.getDatabaseName()));
			}
		}
		buf.addAll(nativeDrops);
		return buf;
	}

	// for mt support - the database name of the tenant is different than the database name of the landlord
	@Override
	public PEDDL buildTenantDDL(String tenantName) {
		PEDDL copy = new PEDDL(this);
		if (getDatabases().size() > 1)
			throw new IllegalStateException("Cannot build tenant ddl: more than one database");
		getDatabases().get(0).dbn = tenantName;
		copy.created = true; // tenants never create
		copy.tenant = true;
		return copy;
	}

	@Override
	public List<String> getDestroyStatements() throws Exception {
		throw new IllegalStateException("PEDDL does not use destroy statements");
	}
	
	// these are all compatibility constructors and functions
	public PEDDL(String dbname, StorageGroupDDL persGroup, String dbtag) {
		this(dbname,persGroup,dbtag,null,null);
	}
	
	public PEDDL(String dbname, StorageGroupDDL persGroup, String dbtag, String charset, String collation) {
		super();
		persGroups = new ArrayList<StorageGroupDDL>();
		withStorageGroup(persGroup);
		withDatabase(new PEDatabaseDDL(dbname,dbtag,charset,collation).withStorageGroup(persGroup));		
	}
	
	public PEDDL withTemplate(String temp, boolean strict) {
		getSinglePEDB().withTemplate(temp, strict);
		return this;
	}
	
	public PEDDL withMTMode(MultitenantMode mm) {
		getSinglePEDB().withMTMode(mm);
		return this;
	}
	
	public PEDDL withFKMode(FKMode fkm) {
		getSinglePEDB().withFKMode(fkm);
		return this;
	}
		
	public PEDDL withTemplateDeclarations(String...decls) {
		for(String s : decls) {
			templateDeclarations.add(s);
		}
		if (templateDeclarations.size() > 0) {
			for(DatabaseDDL db : getDatabases()) {
				PEDatabaseDDL pedb = (PEDatabaseDDL) db;
				pedb.withLoadTemplate(false);
			}
		}
		return this;
	}
	
	public MultitenantMode getMultitenantMode() {
		return getSinglePEDB().getMultitenantMode();
	}
	
	public FKMode getForeignKeyMode() {
		return getSinglePEDB().getForeignKeyMode();
	}
	
	protected PEDatabaseDDL getSinglePEDB() {
		return (PEDatabaseDDL) super.getSingleDB();
	}
	
	@Override
	public void clearCreated() {
		super.clearCreated();
		// also do the same on persistent groups
		for(StorageGroupDDL sgddl : persGroups)
			sgddl.clearCreated();
	}

	public PEDDL setThrowDropRangeInUseException(boolean value) {
		this.throwDropRangeInUseException = value;
		return this;
	}
}
