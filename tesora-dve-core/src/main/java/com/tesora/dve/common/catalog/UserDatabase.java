// OS_STATUS: public
package com.tesora.dve.common.catalog;

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

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.MapKey;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;

import org.hibernate.annotations.ForeignKey;

import com.tesora.dve.common.ShowSchema;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.infoschema.annos.ColumnView;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaColumn;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaTable;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.annos.TableView;

@InfoSchemaTable(logicalName="database",
		views={@TableView(view=InfoView.SHOW, name="database", pluralName="databases",
				columnOrder={ShowSchema.Database.NAME, 
							 ShowSchema.Database.DEFAULT_PERSISTENT_GROUP,
							 ShowSchema.Database.TEMPLATE,
							 ShowSchema.Database.TEMPLATE_MODE,
							 ShowSchema.Database.MULTITENANT,
							 ShowSchema.Database.FKMODE,
							 ShowSchema.Database.DEFAULT_CHARACTER_SET,
							 ShowSchema.Database.DEFAULT_COLLATION
							 })})
@Entity
@Table(name="user_database")
@XmlAccessorType(XmlAccessType.NONE)
public class UserDatabase implements CatalogEntity, PersistentDatabase {

	private static final long serialVersionUID = 1L;
	
	public static final String DEFAULT = "TestDB";

	@Id
	@GeneratedValue
	@Column( name="user_database_id" )
	int id;
	
	@XmlElement
	String name;

	@ForeignKey(name="fk_db_def_sg")
	@ManyToOne
	@JoinColumn(name="default_group_id")
	PersistentGroup defaultStorageGroup;
	
	@OneToMany(mappedBy="userDatabase",fetch=FetchType.LAZY)
	@MapKey(name="name")
	private Map<String,UserTable> userTables = new HashMap<String,UserTable>();

	@Column(name="template",nullable=true)
	private String templateName;
	@Column(name="template_mode",nullable=true)
	private String templateMode;
	
	@Column(name="multitenant_mode",nullable=false)
	String multitenant_mode;
		
	@Column(name="default_character_set_name",nullable=true)
	String defaultCharacterSetName;

	@Column(name="default_collation_name",nullable=true)
	String defaultCollationName;

	@Column(name="fk_mode",nullable=false)
	String fk_mode;
	
	private transient ColumnSet showColumnSet = null;
	private transient MultitenantMode mtmodeobj = null;
	private transient FKMode fkmodeobj = null;
	
	// Used by tests.
	public UserDatabase(String name, PersistentGroup sg) {
		this(name, sg, null, TemplateMode.OPTIONAL, MultitenantMode.OFF, FKMode.STRICT, null, null);
	}

	public UserDatabase(String name, PersistentGroup sg, String template, TemplateMode templateMode, MultitenantMode mm,
			FKMode fkm,
			String charSet, String collation) {
		this.name = name;
		this.defaultStorageGroup = sg;
		this.templateName = template;
		this.templateMode = (templateMode != null) ? templateMode.toString() : TemplateMode.getCurrentDefault().toString();
		this.multitenant_mode = mm.getPersistentValue();
		this.fk_mode = fkm.getPersistentValue();
		this.defaultCharacterSetName = charSet;
		this.defaultCollationName = collation;
	}
	
	UserDatabase() {
	}

	@Override
	@InfoSchemaColumn(logicalName="id",fieldName="id",
			sqlType=java.sql.Types.INTEGER,
			views={})
	public int getId() {
		return id;
	}

	@InfoSchemaColumn(logicalName="name",fieldName="name",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={@ColumnView(view=InfoView.SHOW, name=ShowSchema.Database.NAME,orderBy=true,ident=true),
			       @ColumnView(view=InfoView.INFORMATION, name="schema_name", orderBy=true, ident=true)})
	public String getName() {
		return name;
	}

	public void setName(String catalogName) {
		this.name = catalogName;
	}

	@InfoSchemaColumn(logicalName="default_storage_group", fieldName="defaultStorageGroup",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={@ColumnView(view=InfoView.SHOW,name=ShowSchema.Database.DEFAULT_PERSISTENT_GROUP,extension=true),
				   @ColumnView(view=InfoView.INFORMATION,name="default_persistent_group")})
	public PersistentGroup getDefaultStorageGroup() {
		return defaultStorageGroup;
	}

	public void setDefaultStorageGroup(PersistentGroup defaultStorageGroup) {
		this.defaultStorageGroup = defaultStorageGroup;
	}

	public final Collection<UserTable> getUserTables() {
		return userTables.values();
	}

	public void addUserTable(UserTable t) {
		this.userTables.put(t.getName(), t);
	}

	public void removeUserTable(UserTable userTable) {
		this.userTables.remove(userTable.getName());
	}

	@Override
	public String getNameOnSite(StorageSite site) {
		return getNameOnSite(getName(), site);
	}
	
	public static String getNameOnSite(String dbName, StorageSite site) {
		return new StringBuffer().append(site.getInstanceIdentifier()).append("_").append(dbName).toString();
	}
	
	public boolean equals(UserDatabase other) {
		return other != null && name.equals(other.name);
	}

	public UserTable getTableByName(String targetTableName) throws PEException {
		return getTableByName(targetTableName,true);
	}
	
	public UserTable getTableByName(String targetTableName, boolean except) throws PEException {
		UserTable t = userTables.get(targetTableName);
		if (t == null && except)
			throw new PEException("Table " + targetTableName + " not found in database " + getName());
		return t;
	}

	
	@Override
	public ColumnSet getShowColumnSet(CatalogQueryOptions cqo) {
		if ( showColumnSet == null ) {
			showColumnSet = new ColumnSet();
			showColumnSet.addColumn("Database", 255, "varchar", Types.VARCHAR);
			if (cqo.emitExtensions()) {
				showColumnSet.addColumn("Default " + PersistentGroup.PERSISTENT_GROUP_CS_HEADER_VAL, 255, "varchar", Types.VARCHAR);
				showColumnSet.addColumn("Template", 255, "varchar", Types.VARCHAR);
				showColumnSet.addColumn("Strict Template", 16, "varchar", Types.VARCHAR);
			}
		}

		return showColumnSet;
	}

	@Override
	public ResultRow getShowResultRow(CatalogQueryOptions cqo) {
		ResultRow rr = new ResultRow();
		rr.addResultColumn(this.name, false);
		if (cqo.emitExtensions()) {
			rr.addResultColumn(this.defaultStorageGroup != null ? this.defaultStorageGroup.getName() : null);
			if ( this.templateName != null ) {
				rr.addResultColumn(this.templateName);
				rr.addResultColumn(this.templateMode);
			}
			else {
				rr.addResultColumn((Object) null);
				rr.addResultColumn((Object) null);
			}
		}
		
		return rr;
	}

	@Override
	public void removeFromParent() throws Throwable {
	}	
	
	@Override
	public List<? extends CatalogEntity> getDependentEntities(CatalogDAO c) throws Throwable {
		// optimized this to handle drop database better
		// we're going to execute some queries to find all of the entities of each type all at once
		// otherwise this just takes too long
		List<CatalogEntity> dependents = new ArrayList<CatalogEntity>();
		Map<String,Object> params = new HashMap<String,Object>();
		params.put("cdb",this);
		// range table relationships
		List<CatalogEntity> temp = c.queryCatalogEntity("from RangeTableRelationship where table.userDatabase = :cdb",params);
		dependents.addAll(temp);
		// keys, key columns
		temp = c.queryCatalogEntity("from KeyColumn kc where kc.key.userTable.userDatabase = :cdb", params);
		dependents.addAll(temp);
		temp = c.queryCatalogEntity("from Key k where k.userTable.userDatabase = :cdb", params);
		dependents.addAll(temp);
		// columns
		temp = c.queryCatalogEntity("from UserColumn where userTable.userDatabase = :cdb",params);
		dependents.addAll(temp);
		boolean domt = getMultitenantMode().isMT();
		if (domt) {
			// scopes
			temp = c.queryCatalogEntity("from TableVisibility tv where tv.table.userDatabase = :cdb", params);
			dependents.addAll(temp);
		}
		temp = c.queryCatalogEntity("from UserTable where userDatabase = :cdb", params);
		dependents.addAll(temp);
		temp = c.queryCatalogEntity("from Priviledge where database = :cdb",params);
		dependents.addAll(temp);
		if (domt) {
			temp = c.queryCatalogEntity("from Priviledge where tenant.userDatabase = :cdb",params);
			dependents.addAll(temp);
			temp = c.queryCatalogEntity("from Tenant where userDatabase = :cdb",params);
			dependents.addAll(temp);
		}
		temp = c.queryCatalogEntity("from Container where baseTable.userDatabase = :cdb", params);
		dependents.addAll(temp);
		temp = c.queryCatalogEntity("from RawPlan where userDatabase = :cdb", params);
		dependents.addAll(temp);
		temp = c.queryCatalogEntity("from Shape s where s.userDatabase = :cdb", params);
		dependents.addAll(temp);
		return dependents;
	}

	@InfoSchemaColumn(logicalName="template",fieldName="templateName",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={@ColumnView(view=InfoView.SHOW,name=ShowSchema.Database.TEMPLATE,extension=true),
				   @ColumnView(view=InfoView.INFORMATION,name="template",extension=true)})
	public String getTemplateName() {
		return templateName;
	}

	public void setTemplateName(final String name) {
		this.templateName = name;
	}

	@InfoSchemaColumn(logicalName = "template_mode", fieldName = "templateMode",
			sqlType = java.sql.Types.VARCHAR, sqlWidth = 16,
			views = { @ColumnView(view = InfoView.SHOW, name = ShowSchema.Database.TEMPLATE_MODE, extension = true),
					@ColumnView(view = InfoView.INFORMATION, name = "template_mode") })
	public String getTemplateMode() {
		return this.templateMode;
	}

	public void setTemplateMode(final TemplateMode mode) {
		this.templateMode = mode.toString();
	}

	@InfoSchemaColumn(logicalName="multitenant", fieldName="multitenant_mode",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={@ColumnView(view=InfoView.SHOW, name=ShowSchema.Database.MULTITENANT,extension=true),
				   @ColumnView(view=InfoView.INFORMATION, name="multitenant")})	
	public String getMultitenantModeString() {
		return this.multitenant_mode;
	}
	
	@InfoSchemaColumn(logicalName="def_character_set_name", fieldName="defaultCharacterSetName",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=32,
			views={@ColumnView(view=InfoView.SHOW, name=ShowSchema.Database.DEFAULT_CHARACTER_SET,extension=true),
				   @ColumnView(view=InfoView.INFORMATION, name="default_character_set_name")})	
	public String getDefaultCharacterSetName() {
        return (this.defaultCharacterSetName == null) ?
				Singletons.require(HostService.class).getDBNative().getDefaultServerCharacterSet() : this.defaultCharacterSetName;
	}

	public void setDefaultCharacterSetName(String value) {
		this.defaultCharacterSetName = value;
	}

	@InfoSchemaColumn(logicalName="def_collation_name", fieldName="defaultCollationName",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=32,
			views={@ColumnView(view=InfoView.SHOW, name=ShowSchema.Database.DEFAULT_COLLATION,extension=true),
				   @ColumnView(view=InfoView.INFORMATION, name="default_collation_name")})	
	public String getDefaultCollationName() {
        return (this.defaultCollationName == null) ?
				Singletons.require(HostService.class).getDBNative().getDefaultServerCollation() : this.defaultCollationName;
	}

	public void setDefaultCollationName(String value) {
		this.defaultCollationName = value;
	}

	public MultitenantMode getMultitenantMode() {
		if (mtmodeobj == null) mtmodeobj = MultitenantMode.toMode(multitenant_mode);
		return mtmodeobj;
	}

	@InfoSchemaColumn(logicalName="fkmode", fieldName="fk_mode",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={@ColumnView(view=InfoView.SHOW, name=ShowSchema.Database.FKMODE,extension=true),
				   @ColumnView(view=InfoView.INFORMATION, name="foreign_key_mode")})	
	public String getFKModeString() {
		return this.fk_mode;
	}

	
	public FKMode getFKMode() {
		if (fkmodeobj == null) fkmodeobj = FKMode.toMode(fk_mode);
		return fkmodeobj;
	}
	
	public boolean hasStrictTemplateMode() {
		return TemplateMode.getModeFromName(this.templateMode).isStrict();
	}

	@Override
	public String getUserVisibleName() {
		return name;
	}

	@Override
	public void onUpdate() {
	}

	@Override
	public void onDrop() {
	}

}
