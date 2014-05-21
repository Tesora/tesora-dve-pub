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
import java.util.Collections;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.xml.bind.annotation.XmlRootElement;

import org.hibernate.annotations.ForeignKey;

import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.infoschema.annos.ColumnView;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaColumn;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaTable;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.annos.TableView;

@InfoSchemaTable(logicalName = "project", views = {
		@TableView(view = InfoView.SHOW, name = "project", pluralName = "projects", columnOrder = { "name",
				"default_policy", "default_storage_group" }, extension = true, priviledged = true),
		@TableView(view = InfoView.INFORMATION, name = "project", pluralName = "", columnOrder = { "name",
				"default_policy", "default_storage_group" }, extension = true, priviledged = true) })
@XmlRootElement
@Entity
@Table(name = "project")
public class Project implements CatalogEntity {

	private static final long serialVersionUID = 1L;

	public static final String DEFAULT = "Default";

	@Id
	@GeneratedValue
	@Column(name = "project_id")
	int id;

	String name;

	@ForeignKey(name="fk_project_def_sg")
	@ManyToOne
	@JoinColumn(name = "default_persistent_group_id")
	PersistentGroup defaultStorageGroup;

	@ForeignKey(name="fk_project_def_policy")
	@ManyToOne
	@JoinColumn(name = "default_policy_id")
	DynamicPolicy defaultPolicy;

	@ForeignKey(name="fk_project_root_user")
	@ManyToOne
	@JoinColumn(name = "root_user_id")
	User rootUser;

	transient ColumnSet showColumnSet = null;

	public Project(String name) {
		this.name = name;
	}

	Project() {
	}

	@Override
	public int getId() {
		return id;
	}

	@InfoSchemaColumn(logicalName = "name", fieldName = "name", sqlType = java.sql.Types.VARCHAR, sqlWidth = 255, views = {
			@ColumnView(view = InfoView.SHOW, name = "name", orderBy = true, ident = true),
			@ColumnView(view = InfoView.INFORMATION, name = "name", orderBy = true, ident = true) })
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@InfoSchemaColumn(logicalName = "default_storage_group", fieldName = "defaultStorageGroup", sqlType = java.sql.Types.VARCHAR, sqlWidth = 255, views = {
			@ColumnView(view = InfoView.SHOW, name = "default_storage_group"),
			@ColumnView(view = InfoView.INFORMATION, name = "default_storage_group") })
	public PersistentGroup getDefaultStorageGroup() {
		return defaultStorageGroup;
	}

	public void setDefaultStorageGroup(PersistentGroup sg) {
		defaultStorageGroup = sg;
	}

	@Override
	public ColumnSet getShowColumnSet(CatalogQueryOptions cqo) {
		if (showColumnSet == null) {
			showColumnSet = new ColumnSet();
			showColumnSet.addColumn("Project", 255, "varchar", Types.VARCHAR);
		}
		return showColumnSet;
	}

	@Override
	public ResultRow getShowResultRow(CatalogQueryOptions cqo) {
		ResultRow rr = new ResultRow();
		rr.addResultColumn(this.name, false);

		return rr;
	}

	@Override
	public void removeFromParent() {
		// TODO Actually implement the removal of this instance from the parent
	}

	@Override
	public List<CatalogEntity> getDependentEntities(CatalogDAO c) throws Throwable {
		// TODO Return a valid list of dependents
		return Collections.emptyList();
	}

	@InfoSchemaColumn(logicalName = "default_policy", fieldName = "defaultPolicy", sqlType = java.sql.Types.VARCHAR, sqlWidth = 255, views = {
			@ColumnView(view = InfoView.SHOW, name = "default_policy"),
			@ColumnView(view = InfoView.INFORMATION, name = "default_policy") })
	public DynamicPolicy getDefaultPolicy() {
		return defaultPolicy;
	}

	public void setDefaultPolicy(DynamicPolicy dt) {
		this.defaultPolicy = dt;
	}
	
	@InfoSchemaColumn(logicalName = "root_user", fieldName = "rootUser", sqlType = java.sql.Types.VARCHAR, sqlWidth = 255, views = {
			@ColumnView(view = InfoView.SHOW, name = "root_user"),
			@ColumnView(view = InfoView.INFORMATION, name = "root_user") })
	public User getRootUser() {
		return rootUser;
	}

	public void setRootUser(User rootUser) {
		this.rootUser = rootUser;
	}

	@Override
	public void onUpdate() {
	}

	@Override
	public void onDrop() {
	}
}
