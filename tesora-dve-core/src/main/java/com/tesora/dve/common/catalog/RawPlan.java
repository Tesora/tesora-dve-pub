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

import java.util.Collections;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.Lob;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import org.hibernate.annotations.ForeignKey;

import com.tesora.dve.common.ShowSchema;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.infoschema.annos.ColumnView;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaColumn;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaTable;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.annos.TableView;

@InfoSchemaTable(logicalName="persistent_plan",
views={@TableView(view=InfoView.SHOW, name="rawplan", pluralName="rawplans", 
		columnOrder={ShowSchema.RawPlan.NAME, 
				ShowSchema.RawPlan.DB, 
				ShowSchema.RawPlan.ENABLED, 
				ShowSchema.RawPlan.CACHE_KEY,
				ShowSchema.RawPlan.COMMENT, 
				ShowSchema.RawPlan.BODY}, extension=true),
       @TableView(view=InfoView.INFORMATION, name="rawplans", pluralName="", 
       columnOrder={"name", "plan_schema", "is_enabled", "cache_key", "plan_comment","definition"}, extension=true)})
@Entity
@Table(name="rawplan")
public class RawPlan implements CatalogEntity, NamedCatalogEntity {

	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue
	@Column(name = "plan_id")
	private int id;

	@Column(name = "name", nullable = false)
	private String name;
	
	@Column(name = "definition", nullable = false)
	@Lob
	String definition;

	@Column(name = "plan_comment", nullable = true)
	String comment;
	
	@ForeignKey(name="fk_raw_plan_db")
	@ManyToOne(optional = false)
	@JoinColumn(name = "user_database_id")
	UserDatabase userDatabase;
	
	@Column(name = "enabled", nullable = false)
	int enabled;
	
	// this is the shrunk value
	@Column(name = "cachekey", nullable = false)
	@Lob
	String cacheKey;
	
	@Override
	public int getId() {
		return id;
	}

	public RawPlan() {
		
	}
	
	public RawPlan(String name, String def, UserDatabase ofDB, boolean enabled, String shrunk, String comment) {
		this.name = name;
		this.definition = def;
		this.comment = comment;
		this.userDatabase = ofDB;
		this.enabled = (enabled ? 1 : 0);
		this.cacheKey = shrunk;
	}
	
	@InfoSchemaColumn(logicalName="name",fieldName="name",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={@ColumnView(view=InfoView.SHOW, name=ShowSchema.RawPlan.NAME,orderBy=true,ident=true),
			       @ColumnView(view=InfoView.INFORMATION, name="name", orderBy=true, ident=true)})
	@Override
	public String getName() {
		return name;
	}

	@InfoSchemaColumn(logicalName="body",fieldName="definition",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={@ColumnView(view=InfoView.SHOW, name=ShowSchema.RawPlan.BODY),
			       @ColumnView(view=InfoView.INFORMATION, name="definition")})
	public String getDefinition() {
		return definition;
	}
	
	public void setDefinition(String def) {
		definition = def;
	}

	@InfoSchemaColumn(logicalName="cachekey",fieldName="cacheKey",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={@ColumnView(view=InfoView.SHOW, name=ShowSchema.RawPlan.CACHE_KEY),
			       @ColumnView(view=InfoView.INFORMATION, name="cache_key")})
	public String getCacheKey() {
		return cacheKey;
	}
	
	public void setCacheKey(String def) {
		cacheKey = def;
	}

	@InfoSchemaColumn(logicalName="comment", fieldName="comment",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={@ColumnView(view=InfoView.SHOW, name=ShowSchema.RawPlan.COMMENT),
				   @ColumnView(view=InfoView.INFORMATION, name="plan_comment")})
	public String getComment() {
		return comment;
	}
	
	public void setComment(String v) {
		comment = v;
	}
	
	@InfoSchemaColumn(logicalName="database",fieldName="userDatabase",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={@ColumnView(view=InfoView.INFORMATION,name="plan_schema"),
				   @ColumnView(view=InfoView.SHOW,name=ShowSchema.RawPlan.DB,visible=false,injected=true)},
			injected=true)
	public UserDatabase getDatabase() {
		return userDatabase;
	}

	public void setDatabase(UserDatabase database) {
		userDatabase = database;
	}
	
	@InfoSchemaColumn(logicalName="enabled", fieldName="enabled",
			sqlType=java.sql.Types.VARCHAR,
			views={@ColumnView(view=InfoView.SHOW,name=ShowSchema.RawPlan.ENABLED),
				   @ColumnView(view=InfoView.INFORMATION,name="is_enabled")})
	public boolean isEnabled() {
		return (enabled == 1 ? true : false);
	}
	
	public void setEnabled(boolean v) {
		enabled = (v ? 1 : 0);
	}
	
	
	
	@Override
	public ColumnSet getShowColumnSet(CatalogQueryOptions cqo)
			throws PEException {
		return null;
	}

	@Override
	public ResultRow getShowResultRow(CatalogQueryOptions cqo)
			throws PEException {
		return null;
	}

	@Override
	public void removeFromParent() throws Throwable {
		
	}

	@Override
	public List<? extends CatalogEntity> getDependentEntities(CatalogDAO c)
			throws Throwable {
		return Collections.emptyList();
	}

	@Override
	public void onUpdate() {
		
	}

	@Override
	public void onDrop() {
		
	}

}
