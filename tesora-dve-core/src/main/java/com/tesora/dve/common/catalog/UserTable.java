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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.Lob;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.OrderBy;
import javax.persistence.Table;

import org.hibernate.annotations.ForeignKey;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.ShowSchema;
import com.tesora.dve.db.DBEmptyTextResultConsumer;
import com.tesora.dve.distribution.ContainerDistributionModel;
import com.tesora.dve.distribution.DistributionRange;
import com.tesora.dve.distribution.KeyTemplate;
import com.tesora.dve.distribution.KeyValue;
import com.tesora.dve.distribution.RangeDistributionModel;
import com.tesora.dve.distribution.RangeTableRelationship;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.groupmanager.GroupManager;
import com.tesora.dve.queryplan.QueryStepDDLOperation;
import com.tesora.dve.resultset.ColumnMetadata;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.infoschema.annos.ColumnView;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaColumn;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaTable;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.annos.TableView;
import com.tesora.dve.worker.WorkerGroup;
import com.tesora.dve.worker.WorkerGroup.Manager;
import com.tesora.dve.worker.WorkerGroup.WorkerGroupFactory;

/**
 * Entity implementation class for Entity: UserTable
 * 
 */
@InfoSchemaTable(logicalName="table",
		views={@TableView(view=InfoView.SHOW, name="table", pluralName="tables",
				columnOrder={ShowSchema.Table.NAME,
							 ShowSchema.Table.TYPE,
							 ShowSchema.Table.MODEL,
							 ShowSchema.Table.PERSISTENT_GROUP,
							 "database"}),
			   @TableView(view=InfoView.INFORMATION, name="tables", pluralName="",
				columnOrder={"table_schema","table_name","table_type","engine",
					   		 "storage_group",
					   		 "row_format","table_collation","create_options","table_comment"})})
@Entity
@Table(name = "user_table"
// would have been really nice to put this in, but then would have to worry about
// collation on name
// ,uniqueConstraints=@UniqueConstraint(columnNames = {"name","user_database_id"})
)
public class UserTable implements CatalogEntity, HasAutoIncrementTracker, NamedCatalogEntity, PersistentTable {

	private static final String TEMP_TABLE_PREFIX = "temp";

	@Id
	@GeneratedValue
	@Column(name = "table_id")
	int id;

	@Column(name = "name", nullable = false)
	String name;

	// Optional: used to store the db native CREATE TABLE stmt
	@Column(name = "create_table_stmt", nullable = true)
	@Lob
	String createTableStmt;
	
	@ForeignKey(name="fk_table_model")
	@ManyToOne(optional = false)
	@JoinColumn(name = "distribution_model_id")
	DistributionModel distributionModel;

	@ForeignKey(name="fk_table_sg")
	@ManyToOne(optional = false)
	@JoinColumn(name = "persistent_group_id")
	PersistentGroup persistentGroup;

	@ForeignKey(name="fk_table_db")
	@ManyToOne(optional = false)
	@JoinColumn(name = "user_database_id")
	UserDatabase userDatabase;

	// optional is true because unused in modes other than mt relaxed and adaptive
	@ForeignKey(name="fk_table_shape")
	@ManyToOne(optional=true)
	@JoinColumn(name="shape_id")
	Shape shape;

	// have to use string for info schema
	// state really only pertains to mt modes
	@Column(name = "state")
	@Enumerated(EnumType.STRING)
	TableState state;

	// contained keys
	@OneToMany(mappedBy="userTable")
	@OrderBy("position ASC")
	List<Key> keys = new ArrayList<Key>();
		
	@OneToMany(mappedBy="referencedTable")
	Set<Key> referencing = new HashSet<Key>();
	
	@OneToMany(cascade=CascadeType.ALL, mappedBy="userTable", fetch=FetchType.LAZY)
	@OrderBy("orderInTable ASC")
	List<UserColumn> userColumns = new ArrayList<UserColumn>();
	
	@OneToOne(mappedBy="table", cascade=CascadeType.ALL)
	AutoIncrementTracker autoIncr;

	@OneToOne(mappedBy="table", cascade=CascadeType.ALL, fetch=FetchType.LAZY)
	UserView view;
	
	// engine can be nullable for views
	@Column(name = "engine", nullable=true)
	String engine = null;
	
	@Column(name = "collation", nullable=true)
	String collation;
	
	@Column(name = "row_format", nullable=true)
	String rowFormat;
	
	@Column(name = "comment", nullable=true, length=2048)
	String comment;
	
	@Column(name = "create_options", nullable=true)
	String createOptions;
	
	transient StorageGroup storageGroup = null;
	transient Map<String,UserColumn> userColumnMap = null;
	transient ColumnSet showColumnSet = null;
	transient Integer rangeID = null;
	
	@ForeignKey(name="fk_table_container")
	@ManyToOne(optional = true)
	@JoinColumn(name = "container_id")
	Container container;

	// ostensibly I can figure this out based on what database this table is part of
	// (if info schema/mysql schema this should be SYSTEM VIEW) and whether there is
	// an associated view for this table, but I can't do it in such a way that I don't break
	// info schema entity queries....so shelving it for now. 
	@Column(name = "table_type", nullable = false)
	String table_type;
	
	private static final long serialVersionUID = 1L;

	private UserTable(String name, String createTableStmt,
			Shape shape,
			DistributionModel distributionModel, PersistentGroup persistentGroup,
			UserDatabase userDatabase, List<Key> keys,
			List<UserColumn> userColumns, AutoIncrementTracker autoIncr,
			TableState state,
			String engine,
			String tableType) {
		super();
		this.name = name;
		this.shape = shape;
		this.createTableStmt = createTableStmt;
		this.distributionModel = distributionModel;
		this.persistentGroup = persistentGroup;
		this.userDatabase = userDatabase;
		this.userColumns = userColumns;
		this.autoIncr = autoIncr;
		this.container = null;
		this.state = state;
		this.engine = engine;
		this.table_type = tableType;
	}

	public UserTable() {
		super();
	}

	public UserTable(String name, DistributionModel dmodel,
			UserDatabase userDatabase, TableState state, String engine, String tableType) {
		this.name = name;
		this.distributionModel = dmodel;
		this.userDatabase = userDatabase;
		this.container = null;
		this.state = state;
		this.engine = engine;
		this.table_type = tableType;
	}

	public UserTable(String name, PersistentGroup sg, DistributionModel dm,
			UserDatabase db, TableState state, String engine, String tableType) {
		this(name, dm, db, state, engine, tableType);
		this.persistentGroup = sg;
		this.container = null;
	}
	
	public UserTable shallowClone() {
		return new UserTable(name, createTableStmt, shape, distributionModel, persistentGroup, userDatabase, keys, getUserColumns(), autoIncr, state, engine, table_type);
	}

	@Override
	@InfoSchemaColumn(logicalName="id",fieldName="id",
			sqlType=java.sql.Types.INTEGER,
			views={})
	public int getId() {
		return this.id;
	}

	@Override
	@InfoSchemaColumn(logicalName="name",fieldName="name",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={@ColumnView(view=InfoView.SHOW, name=ShowSchema.Table.NAME,orderBy=true,ident=true),
			       @ColumnView(view=InfoView.INFORMATION, name="table_name", orderBy=true, ident=true)})
	public String getName() {
		return this.name;
	}
	
	private transient String qname = null;

	public String getQualifiedName() {
		if (qname == null)
			qname = userDatabase.getName() + "." + getName();
		return qname;
	}
	
	public String getNameAsIdentifier() {
        return Singletons.require(HostService.class).getDBNative().getNameForQuery(this);
	}

	public void setName(String n) {
		this.name = n;
	}
	
	public String getCreateTableStmt(boolean tempTable, boolean useSystemTemp) throws PEException {
		// TODO this method should really be pushed into DBNative. We should probably
		// also distinguish between user and temp tables.
		if (createTableStmt == null) {
			StringBuffer createSql = new StringBuffer().append("CREATE ");
			if (useSystemTemp)
				createSql.append("TEMPORARY ");
            createSql.append("TABLE ")
					.append(Singletons.require(HostService.class).getDBNative().getNameForQuery(this))
					.append(" (")
					.append(Singletons.require(HostService.class).getDBNative().getColumnDefForQuery(getUserColumns().get(0)));
			for (int i = 1; i < getUserColumns().size(); ++i) {
                createSql.append(",").append(Singletons.require(HostService.class).getDBNative().getColumnDefForQuery(getUserColumns().get(i)));
			}
			if (tempTable) {
				for(Key k : keys) {
					createSql.append(", ");
					k.printTempTableKey(createSql);
				}
			}
			createSql.append(") DEFAULT CHARSET=UTF8");
			if (tempTable)
//				createSql.append(" ENGINE=MEMORY");
				createSql.append(" ENGINE=MYISAM");
			createTableStmt = createSql.toString();
		}
		return createTableStmt;
	}
	
	public String getCreateTableStmt(boolean tempTable) throws PEException {
		return getCreateTableStmt(tempTable, false);
	}
	
	public String getCreateTableStmt() throws PEException {
		return getCreateTableStmt(false);
	}

	public void setCreateTableStmt(String createTableStmt) {
		this.createTableStmt = createTableStmt;
	}

	public void setShape(Shape targ) {
		this.shape = targ;
	}
	
	public Shape getShape() {
		return this.shape;
	}
	
	public static SQLCommand getDropTableStmt(String tableName, boolean ifExists) {
        return new SQLCommand("DROP TABLE " + (ifExists ? "IF EXISTS " : "") + Singletons.require(HostService.class).getDBNative().quoteIdentifier(tableName));
	}

	public void addUserColumn(UserColumn uc) {
		int order = 1;
		if ( userColumnMap == null ) {
			buildUserColumnMap();
		}
		
		if ( getUserColumns().size() > 0 ) {
			order = getUserColumns().get(getUserColumns().size()-1).getOrderInTable() + 1;
		}
		uc.setOrderInTable(order);
		getUserColumns().add(uc);
		userColumnMap.put(uc.getAliasName(), uc);
	}

	public void removeUserColumn(UserColumn uc) {
		getUserColumns().remove(uc);
		if (userColumnMap != null) {
			userColumnMap.remove(uc);
		}
	}
	
	public void addUserColumnList(List<? extends UserColumn> ucs) {
		if ( userColumnMap == null ) 
			buildUserColumnMap();

		for ( UserColumn uc : ucs )
			addUserColumn( uc );
	}

	public void addColumnMetadataList(List<? extends ColumnMetadata> cms) throws PEException {
		if ( userColumnMap == null ) 
			buildUserColumnMap();

		for ( ColumnMetadata cm : cms ) {
			UserColumn uc = new UserColumn(cm);
			if ( cm.usingAlias() ) 
				uc.setName(cm.getAliasName());
			
			addUserColumn(uc);
		}
	}
	
	public UserColumn getUserColumn(String name) {
		if ( userColumnMap == null ) 
			buildUserColumnMap();
		
		return userColumnMap.get(name);
	}

	@InfoSchemaColumn(logicalName="model",fieldName="distributionModel",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={@ColumnView(view=InfoView.SHOW,name=ShowSchema.Table.MODEL,extension=true)})
	public DistributionModel getDistributionModel() {
		return this.distributionModel;
	}

	public void setDistributionModel(DistributionModel distributionModel) {
		this.distributionModel = distributionModel;
	}

	@InfoSchemaColumn(logicalName="storage_group",fieldName="persistentGroup",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={@ColumnView(view=InfoView.SHOW,name=ShowSchema.Table.PERSISTENT_GROUP,extension=true),
				   @ColumnView(view=InfoView.INFORMATION,name="storage_group",extension=true)})
	public PersistentGroup getPersistentGroup() {
		if (persistentGroup == null)
			throw new PECodingException("Detected attempt to use non-persistent persistent group in persistent context");
		return this.persistentGroup;
	}

	public void setPersistentGroup(PersistentGroup storageGroup) {
		this.persistentGroup = storageGroup;
	}

	public StorageGroup getStorageGroup() {
		return storageGroup;
	}

	public void setStorageGroup(StorageGroup storageGroup) {
		this.storageGroup = storageGroup;
	}

	public final List<UserColumn> getUserColumns() {
		return userColumns;
	}

	public final List<Key> getKeys() {
		return keys;
	}
	
	public final Key getPrimaryKey() {
		for(Key k : keys) {
			if (k.isPrimaryKey())
				return k;
		}
		return null;
	}
	
	public void addKey(Key k) {
		keys.add(k);
	}
	
	public void removeKey(Key k) {
		keys.remove(k);
	}
	
	public void addReferring(Key k) {
		referencing.add(k);
	}
	
	public void removeReferring(Key k) {
		referencing.remove(k);
	}
	
	public final Set<Key> getReferringKeys() {
		return referencing;
	}
	
	@InfoSchemaColumn(logicalName="database",fieldName="userDatabase",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={@ColumnView(view=InfoView.INFORMATION,name="table_schema",injected=true),
				   @ColumnView(view=InfoView.SHOW,name="database",visible=false,injected=true)},
			injected=true)
	public UserDatabase getDatabase() {
		return userDatabase;
	}

	public void setDatabase(UserDatabase database) {
		userDatabase = database;
	}
	
	@Override
	public String toString() {
		return "Table id: " + getId() + " Name: " + getName()
				+ " with Distribution: " + getDistributionModel()
				+ " on Storage Class: " + getStorageGroup()
				+ " contained in: " + (container == null ? "null" : container.getId())
				+ " state: " + state;
	}

	public static UserTable newTempTable(UserDatabase database,
			ColumnSet shape, String tableName, DistributionModel distModel)
			throws PEException {
		UserTable tempTable = new UserTable(tableName, null, distModel, database, TableState.FIXED, "MYISAM", "BASE TABLE");
		if (shape != null)
			tempTable.addColumnMetadataList(shape.getColumnList());
		return tempTable;
	}
	
	public static String getNewTempTableName() {
		return TEMP_TABLE_PREFIX + GroupManager.getCoordinationServices().getGloballyUniqueId(TEMP_TABLE_PREFIX);
	}
	
	public boolean isTempTable() {
		return name.startsWith(TEMP_TABLE_PREFIX);
	}

	public KeyTemplate getDistKey() {
		KeyTemplate distKey = new KeyTemplate();
		for (UserColumn col : getUserColumns()) {
			if (col.getHashPosition() > 0)
				distKey.add(col);
		}
		Collections.sort(distKey, new Comparator<UserColumn>() {
			@Override
			public int compare(UserColumn o1, UserColumn o2) {
				return o1.getHashPosition() - o2.getHashPosition();
			}
		});
		return distKey;
	}
	
	public KeyValue getDistValue(CatalogDAO c) throws PEException {
		KeyValue dv = new KeyValue(this,getRangeID(c));
		for (UserColumn col : getDistKey()) {
			dv.addColumnTemplate(col);
		}
		return dv;
	}
	
	@Override
	public long getNextIncrValue(CatalogDAO c) {
		return autoIncr.getNextValue(c);
	}
	
	@Override
	public long getNextIncrBlock(CatalogDAO c, long blockSize) {
		return autoIncr.getIdBlock(c, blockSize);
	}

	public long readNextIncrValue(CatalogDAO c) {
		return autoIncr.readNextValue(c);
	}
	
	@Override
	public void removeNextIncrValue(CatalogDAO c, long value) {
		autoIncr.removeValue(c, value);
	}
	
	public String displayName() {
		return (userDatabase == null ? getName() : userDatabase.getName() + "." + getName());
//		return userDatabase.getName()+"."+getName();
	}

	public void prepareGenerationAddition(SSConnection ssCon, WorkerGroup wg, StorageGroupGeneration newGen) throws Throwable {
		Set<PersistentSite> netNewSites = new HashSet<PersistentSite>(newGen.getStorageSites());
		netNewSites.removeAll(wg.getStorageSites());
		PersistentGroup newSG = new PersistentGroup(netNewSites);

		Manager wgManager = wg.setManager(null);
		WorkerGroup newWG = WorkerGroupFactory.newInstance(ssCon, newSG, userDatabase);
		newWG.assureDatabase(ssCon, userDatabase);
		try {
			if (getView() != null && getView().getViewMode() == ViewMode.EMULATE) {
				// nothing to do
			} else {
				if (getView() != null)
					// the persisted create table stmt is incorrect - we need to create an context
					// to build a sqlcommand for the actual definition (because it may refer to other databases)
					throw new PEException("No support for storage gen add with views");
				for(Key k : getKeys()) {
					if (!k.isForeignKey()) continue;
					if (k.getTable() == null)
						// forward, this is ok
						continue;
					UserTable ot = k.getTable();
					if (ot.getDatabase().getId() != getDatabase().getId())
						throw new PEException("No support for storage gen add with cross db fks");
				}
				// TODO this could fail on a database with fks
				QueryStepDDLOperation qso =
						new QueryStepDDLOperation(getDatabase(), new SQLCommand(getCreateTableStmt()),null);

				qso.execute(ssCon, newWG, DBEmptyTextResultConsumer.INSTANCE);
			}

			distributionModel.prepareGenerationAddition(ssCon, wg, this, newGen);
		} finally {
			WorkerGroupFactory.purgeInstance(ssCon, newWG);
			wg.setManager(wgManager);
		}
	}

	private void buildUserColumnMap() {
		userColumnMap = new HashMap<String, UserColumn>();
		for (UserColumn col : getUserColumns()) {
			userColumnMap.put(col.getAliasName(), col);
		}
	}

	public void addAutoIncr() {
		addAutoIncr(null);
	}
	
	public void addAutoIncr(Long offset) {
		if (autoIncr == null) {
			autoIncr = new AutoIncrementTracker(this, offset);
		}
	}

	@Override
	public boolean hasAutoIncr() {
		return autoIncr != null;
	}
	
	public AutoIncrementTracker getAutoIncr() {
		return autoIncr;
	}
	
	@Override
	public ColumnSet getShowColumnSet(CatalogQueryOptions cqo) {
		
		if ( showColumnSet == null ) {
			showColumnSet = new ColumnSet();
			showColumnSet.addColumn("Tables_in_" + this.userDatabase.getName(), 255, "varchar", Types.VARCHAR);
			if (cqo.emitExtensions()) {
				showColumnSet.addColumn("Distribution Model", 255, "varchar", Types.VARCHAR);
				showColumnSet.addColumn(PersistentGroup.PERSISTENT_GROUP_CS_HEADER_VAL, 255, "varchar", Types.VARCHAR);
			}
		}

		return showColumnSet;
	}

	@Override
	public ResultRow getShowResultRow(CatalogQueryOptions cqo) {
		ResultRow rr = new ResultRow();
		String tabName = this.name;
/*
		if (cqo.isTenant()) {
			TableVisibility tv = getTenantVisibility();
			if (tv != null && tv.getLocalName() != null)
				tabName = tv.getLocalName();
		}
		*/
		rr.addResultColumn(tabName, false);
		if (cqo.emitExtensions()) {
			rr.addResultColumn(this.distributionModel.getName());
			rr.addResultColumn(this.persistentGroup.getName());
		}
		return rr;
	}

	@InfoSchemaColumn(logicalName="container",fieldName="container",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={})
	public Container getContainer() {
		return container;
	}

	public void setContainer(Container c) {
		container = c;
	}

	public boolean isContainerBaseTable() {
		if (container != null && container.getBaseTable() != null) {
			return this.getId() == container.getBaseTable().getId();
		}
		return false;
	}
	
	@InfoSchemaColumn(logicalName="engine",fieldName="engine",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={
			       //@ColumnView(view=InfoView.SHOW, name=ShowSchema.Table.NAME,orderBy=true,ident=true),
			       @ColumnView(view=InfoView.INFORMATION, name="engine")})
	public String getEngine() {
		return engine;
	}
	
	public void setEngine(String e) {
		engine = e;
	}

	@InfoSchemaColumn(logicalName="tabletype",fieldName="table_type",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={@ColumnView(view=InfoView.SHOW,name=ShowSchema.Table.TYPE),
			       @ColumnView(view=InfoView.INFORMATION, name="table_type")})
	public String getTableType() {
		return table_type;
	}
	
	@InfoSchemaColumn(logicalName="collation",fieldName="collation",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={@ColumnView(view=InfoView.INFORMATION, name="table_collation")})
	public String getCollation() {
		return collation;
	}
	
	public void setCollation(String collation) {
		this.collation = collation;
	}

	@InfoSchemaColumn(logicalName="row_format",fieldName="rowFormat",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=10,
			views={@ColumnView(view=InfoView.INFORMATION, name="row_format")})
	public String getRowFormat() {
		return rowFormat;
	}
	
	public void setRowFormat(String v) {
		rowFormat = v;
	}
	
	@InfoSchemaColumn(logicalName="comment",fieldName="comment",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={@ColumnView(view=InfoView.INFORMATION, name="table_comment")})	
	public String getComment() {
		return comment;
	}
	
	public void setComment(String s) {
		comment = s;
	}

	@InfoSchemaColumn(logicalName="createoptions",fieldName="createOptions",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={@ColumnView(view=InfoView.INFORMATION, name="create_options")})	
	public String getCreateOptions() {
		return createOptions;
	}
	
	public void setCreateOptions(String s) {
		createOptions = s;
	}
	
	@Override
	public void removeFromParent() throws Throwable {
		// distributionModel.removeUserTable(this);
		userDatabase.removeUserTable(this);
	}
	
	@Override
	public List<? extends CatalogEntity> getDependentEntities(CatalogDAO c) throws Throwable {
		List<CatalogEntity> ceList = new ArrayList<CatalogEntity>(); 

		if (view != null) {
			ceList.addAll(view.getDependentEntities(c));
			ceList.add(view);
		}
		
		RangeTableRelationship rtr = c.findRangeTableRelationship(this, false);
		if ( rtr != null )
			ceList.add(rtr);

		for(Key k : keys) {
			ceList.addAll(k.getDependentEntities(c));
			ceList.add(k);
		}
		
		ceList.addAll(getUserColumns());
		ceList.addAll(c.findTenantScopesForTable(this));
		
		if (isContainerBaseTable()) {
			ceList.add(getContainer());
		}
		return ceList;
	}
	
	public void setState(TableState ts) {
		state = ts;
	}

	@InfoSchemaColumn(logicalName="state",fieldName="state",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={})
	public TableState getState() {
		return state;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + id;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		UserTable other = (UserTable) obj;
		if (id != other.id)
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}
	
	public SQLCommand getTruncateStatement() {
        return new SQLCommand("TRUNCATE " + Singletons.require(HostService.class).getDBNative().getNameForQuery(this));
	}

	public UserView getView() {
		return view;
	}
	
	public void setView(UserView uv) {
		view = uv;
	}
	
	@Override
	public void onUpdate() {
	}

	@Override
	public void onDrop() {
	}
	
	public boolean isSynthetic() {
		return PEConstants.INFORMATION_SCHEMA_GROUP_NAME.equals(persistentGroup.getName())
				|| PEConstants.SYSTEM_GROUP_NAME.equals(persistentGroup.getName());
	}

	@Override
	public String getPersistentName() {
		return getName();
	}

	@Override
	public int getNumberOfColumns() {
		return getUserColumns().size();
	}
	
	public Integer getRangeID(CatalogDAO c) throws PEException {
		if (rangeID == null) synchronized(this){
			if (distributionModel.getName().equals(ContainerDistributionModel.MODEL_NAME)) {
				DistributionRange dr = getContainer().getRange();
				if (dr != null)
					rangeID = dr.getId();
			} else if (distributionModel.getName().equals(RangeDistributionModel.MODEL_NAME)) {
				rangeID = c.findRangeForTable(this).getId();
			}
			if (rangeID == null)
				rangeID = -1;
		}
		if (rangeID == -1) return null;
		return rangeID;
	}
}
