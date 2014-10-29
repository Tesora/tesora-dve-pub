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
import javax.persistence.DiscriminatorColumn;
import javax.persistence.DiscriminatorType;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.Table;

import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.db.DBResultConsumer.RowCountAdjuster;
import com.tesora.dve.distribution.IKeyValue;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.ExecutionState;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.schema.SchemaContext.DistKeyOpType;
import com.tesora.dve.worker.WorkerGroup;
import com.tesora.dve.worker.WorkerGroup.MappingSolution;

/**
 * Entity implementation class for Entity: DistributionModel
 *
 */
@Entity
@Table(name="distribution_model")
@Inheritance(strategy=InheritanceType.SINGLE_TABLE)
@DiscriminatorColumn(
		name="name",
		discriminatorType=DiscriminatorType.STRING
		)
@SuppressWarnings("serial")
public abstract class DistributionModel implements CatalogEntity {
	
	static DBResultConsumer.RowCountAdjuster IDENTITY = new DBResultConsumer.RowCountAdjuster() {
		@Override
		public long adjust(long rawRowCount, int siteCount) {
			return rawRowCount;
		}
	};

	@Id
	@GeneratedValue
	private int id;

	@Column(name="name", insertable=false, updatable=false)
	private String name;
	
//  TODO:
//    Doesn't appear to be used for anything yet so removing
//	@OneToMany(mappedBy="distributionModel")
//	private Set<UserTable> userTables = new HashSet<UserTable>();

	private transient ColumnSet showColumnSet = null;
	
	// The no-arg constructor is private so that JAXB can create a base class
	@SuppressWarnings("unused")
	private DistributionModel() {
	}
	
	protected DistributionModel(String name) {
		super();
		this.name = name;
	}   
	
	public WorkerGroup.MappingSolution mapKeyForInsert(CatalogDAO c, StorageGroup wg, IKeyValue key) 
			throws PEException {
		return MappingSolution.AllWorkers;
	}
	public WorkerGroup.MappingSolution mapKeyForUpdate(CatalogDAO c, StorageGroup wg, IKeyValue key) throws PEException {
		return MappingSolution.AllWorkers;
	}
	public WorkerGroup.MappingSolution mapKeyForQuery(CatalogDAO c, StorageGroup wg, IKeyValue key, DistKeyOpType operation) throws PEException {
		return MappingSolution.AllWorkers;
	}
	
	public MappingSolution mapForQuery(WorkerGroup wg, SQLCommand command) throws PEException {
		return WorkerGroup.MappingSolution.AllWorkers;
	}

	public void prepareGenerationAddition(ExecutionState estate, WorkerGroup wg, UserTable userTable, StorageGroupGeneration newGen) throws PEException {
		// Most models do nothing
	}
	
	@Override
	public int getId() {
		return this.id;
	}
	
	public String getName() {
		return this.name;
	}

	public boolean equals(DistributionModel other) {
		return other != null && name.equals(other.name);
	}

	@Override
	public String toString()
	{
		return "Distribution Model ID: " + getId() + ", Name: " + getName(); 
	}

	@Override
	public ColumnSet getShowColumnSet(CatalogQueryOptions cqo) {
		if ( showColumnSet == null ) {
			showColumnSet = new ColumnSet();
			showColumnSet.addColumn("Distribution Model", 255, "varchar", Types.VARCHAR);
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

	public RowCountAdjuster getInsertAdjuster() {
		return IDENTITY;
	}

	public RowCountAdjuster getUpdateAdjuster() {
		return IDENTITY;
	}
}
