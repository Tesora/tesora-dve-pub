// OS_STATUS: public
package com.tesora.dve.common.catalog;

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

import com.tesora.dve.common.ShowSchema;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.db.DBResultConsumer.RowCountAdjuster;
import com.tesora.dve.distribution.IKeyValue;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.infoschema.annos.ColumnView;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaColumn;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaTable;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.annos.TableView;
import com.tesora.dve.sql.schema.SchemaContext.DistKeyOpType;
import com.tesora.dve.worker.WorkerGroup;
import com.tesora.dve.worker.WorkerGroup.MappingSolution;

/**
 * Entity implementation class for Entity: DistributionModel
 *
 */
@InfoSchemaTable(logicalName="distribution_model",
		views={@TableView(view=InfoView.SHOW, name="model", pluralName="models", columnOrder={ShowSchema.DistributionModel.NAME}),
		       @TableView(view=InfoView.INFORMATION, name="distribution_model", pluralName="", columnOrder={"name"})})
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

	public void prepareGenerationAddition(SSConnection ssCon, WorkerGroup wg, UserTable userTable, StorageGroupGeneration newGen) throws PEException {
		// Most models do nothing
	}
	
	@Override
	@InfoSchemaColumn(logicalName="id",fieldName="id",
			sqlType=java.sql.Types.INTEGER,
			views={})
	public int getId() {
		return this.id;
	}
	
	@InfoSchemaColumn(logicalName="name",fieldName="name",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={@ColumnView(view=InfoView.SHOW, name=ShowSchema.DistributionModel.NAME,orderBy=true,ident=true),
			       @ColumnView(view=InfoView.INFORMATION, name="name", orderBy=true, ident=true)})
	public String getName() {
		return this.name;
	}

	/*
	public void addUserTable( UserTable userTable )
	{
		boolean found = false;
		for (UserTable t : userTables) {
			if (t.equals(userTable)) {
				found = true;
				break;
			}
		}
		if (!found)
			userTables.add(userTable);
	}
	
	public void removeUserTable(UserTable userTable) {
		userTables.remove(userTable);
	}

	public final Set<UserTable> getUserTables() {
		return userTables;
	}
	*/
	
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
