// OS_STATUS: public
package com.tesora.dve.sql.util;

import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.common.catalog.FKMode;
import com.tesora.dve.common.catalog.MultitenantMode;
import com.tesora.dve.common.catalog.TemplateMode;
import com.tesora.dve.sql.template.TemplateBuilder;

public class PEDatabaseDDL extends DatabaseDDL {

	private String template;
	private Boolean strict;
	private MultitenantMode mtmode;
	private FKMode fkmode;
	private StorageGroupDDL persGroup;
	
	public PEDatabaseDDL(String name, String tag) {
		super(name,tag);
	}
	
	public PEDatabaseDDL(String name) {
		this(name,"database");
	}
		
	public PEDatabaseDDL withTemplate(String temp, boolean strict) {
		this.template = temp;
		this.strict = strict;
		return this;
	}

	public PEDatabaseDDL withMTMode(MultitenantMode mm) {
		this.mtmode = mm;
		return this;
	}

	public MultitenantMode getMultitenantMode() {
		return mtmode;
	}
	
	public PEDatabaseDDL withFKMode(FKMode fkm) {
		this.fkmode = fkm;
		return this;
	}
	
	public FKMode getForeignKeyMode() {
		return fkmode;
	}
	
	public PEDatabaseDDL withStorageGroup(StorageGroupDDL sg) {
		persGroup = sg;
		return this;
	}
	
	@Override
	public boolean isNative() {
		return false;
	}

	@Override
	public DatabaseDDL copy() {
		return new PEDatabaseDDL(dbn,dbtag).withMTMode(mtmode).withStorageGroup(persGroup).withTemplate(template, strict);
	}

	@Override
	public String getCreateDatabaseStatement() {
		StringBuffer out = new StringBuffer();
		out.append("create ");
		if (mtmode != null && mtmode.isMT())
			out.append(" multitenant ");
		out.append(dbtag).append(" if not exists ").append(dbn);
		if (persGroup != null)
			out.append(" default persistent group ").append(persGroup.getName());
		if (template != null) {
			out.append(" using template ").append(template);
		} else {
			out.append(" using template ").append(TemplateMode.OPTIONAL);
		}
		if (fkmode != null)
			out.append(" foreign key mode ").append(fkmode.getPersistentValue());
		return out.toString();
	}

	@Override
	public List<String> getCreateStatements() throws Exception {
		ArrayList<String> buf = new ArrayList<String>();
		if (isCreated()) 
			buf.add("drop" + (mtmode != null ? " multitenant " : " ") + "database if exists " + dbn);
		// here
		if (template != null) {
			buf.add(TemplateBuilder.getClassPathCreate(template));
		}
		buf.add(getCreateDatabaseStatement());
		setCreated();
		buf.add("use " + dbn);
		return buf;
	}

	@Override
	public List<String> getDestroyStatements() throws Exception {
		return null;
	}

	@Override
	public List<String> getSetupDrops() {
		ArrayList<String> out = new ArrayList<String>();
		// first the database itself
		out.add("DROP DATABASE IF EXISTS " + dbn);
		// and then the dyn sites; note that we're relying on the dyn sites being configured for 5 sites here
		for(int i = 1; i < 6; i++)
			out.add("DROP DATABASE IF EXISTS OnPremise_LOCAL_dyn" + i + "_" + dbn);
		return out;
	}

	@Override
	public String getDropStatement() {
		return "drop " + (mtmode != null ? "multitenant " : "") + "database if exists " + dbn; 
	}

}
