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

import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.xml.bind.annotation.XmlElement;

import org.hibernate.annotations.ForeignKey;

import com.tesora.dve.common.ShowSchema;
import com.tesora.dve.distribution.DistributionRange;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.infoschema.annos.ColumnView;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaColumn;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaTable;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.annos.TableView;

@InfoSchemaTable(logicalName = "container", views = {
		@TableView(view = InfoView.SHOW, name = "container", pluralName = "containers", columnOrder = {
				ShowSchema.Container.NAME, ShowSchema.Container.BASE_TABLE,
				ShowSchema.Container.PERSISTENT_GROUP }),
		@TableView(view = InfoView.INFORMATION, name = "container", pluralName = "", columnOrder = {
				"container_name", "base_table", "storage_group" }) })
@Entity
@Table(name = "container")
public class Container implements CatalogEntity, PersistentContainer {

	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue
	@Column(name = "container_id")
	int id;

	@XmlElement
	String name;

	@ForeignKey(name="fk_cont_basetable")
	@ManyToOne
	@JoinColumn(name = "base_table_id")
	private UserTable baseTable;

	@ForeignKey(name="fk_cont_sg")
	@ManyToOne
	@JoinColumn(name = "storage_group_id", nullable=false)
	PersistentGroup storageGroup;

	// this is the backing distribution model
	@ForeignKey(name="fk_cont_dist_model")
	@ManyToOne(optional = false)
	@JoinColumn(name = "distribution_model_id")
	DistributionModel distributionModel;

	// if the backing distribution model is range - this is the range
	@ForeignKey(name="fk_cont_range")
	@ManyToOne(optional = true)
	@JoinColumn(name = "range_id")
	DistributionRange range;
	
	
	public Container(String name, PersistentGroup sg, DistributionModel model, DistributionRange anyRange) {
		this(name, sg, null, model,anyRange);
	}

	public Container(String name, PersistentGroup sg, UserTable baseTable, DistributionModel model, DistributionRange anyRange) {
		this.name = name;
		this.storageGroup = sg;
		this.baseTable = baseTable;
		this.distributionModel = model;
		this.range = anyRange;
	}

	Container() {
	}

	@Override
	@InfoSchemaColumn(logicalName = "id", fieldName = "id", sqlType = java.sql.Types.INTEGER, views = {})
	public int getId() {
		return id;
	}

	@InfoSchemaColumn(logicalName = "container_name", fieldName = "name", sqlType = java.sql.Types.VARCHAR, sqlWidth = 255, views = {
			@ColumnView(view = InfoView.SHOW, name = ShowSchema.Container.NAME, orderBy = true, ident = true),
			@ColumnView(view = InfoView.INFORMATION, name = "container_name", orderBy = true, ident = true) })
	public String getName() {
		return name;
	}

	public void setName(String catalogName) {
		this.name = catalogName;
	}

	@InfoSchemaColumn(logicalName = "base_table", fieldName = "baseTable", sqlType = java.sql.Types.INTEGER, sqlWidth = 11, views = {
			@ColumnView(view = InfoView.SHOW, name = ShowSchema.Container.BASE_TABLE),
			@ColumnView(view = InfoView.INFORMATION, name = "base_table") })
	public UserTable getBaseTable() {
		return baseTable;
	}

	public void setBaseTable(UserTable baseTable) {
		this.baseTable = baseTable;
	}

	@InfoSchemaColumn(logicalName = "storage_group", fieldName = "storageGroup", sqlType = java.sql.Types.INTEGER, sqlWidth = 11, views = {
			@ColumnView(view = InfoView.SHOW, name = ShowSchema.Container.PERSISTENT_GROUP),
			@ColumnView(view = InfoView.INFORMATION, name = "storage_group") })
	public PersistentGroup getStorageGroup() {
		return storageGroup;
	}

	public void setStorageGroup(PersistentGroup storageGroup) {
		this.storageGroup = storageGroup;
	}

	public DistributionModel getDistributionModel() {
		return distributionModel;
	}
	
	public DistributionRange getRange() {
		return range;
	}
	
	@Override
	public ColumnSet getShowColumnSet(CatalogQueryOptions cqo)
			throws PEException {
		// TODO Auto-generated method stub
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
		return c.findContainerTenants(this);
	}

	@Override
	public void onUpdate() {
	}

	@Override
	public void onDrop() {
	}
}
