package com.tesora.dve.common.catalog;

import java.util.Collections;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import org.hibernate.annotations.ForeignKey;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaColumn;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaTable;

@InfoSchemaTable(logicalName="temporary_table",views={})
@Entity
@Table(name = "user_temp_table")
public class TemporaryTable implements CatalogEntity {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue
	@Column(name="id")
	int id;

	// the connection visible name
	@Column(name="name", nullable=false)
	String name;

	@Column(name="table_engine",nullable=false)
	String engine;
	
	// the connection visible database name
	@Column(name="db", nullable=false)
	String db;
	
	// the session id - specific to the server
	@Column(name="session_id",nullable=false)
	int sessionID;
	
	// the server id
	// can be null when not in multijvm mode
	@ForeignKey(name="fk_server")
	@ManyToOne
	@JoinColumn(name="server_id", nullable=true)
	private ServerRegistration server;

	public TemporaryTable() {
		
	}
	
	public TemporaryTable(String tableName, String tableDatabase, String engineName, int connID, ServerRegistration server) {
		this.server = server;
		this.sessionID = connID;
		this.engine = engineName;
		this.name = tableName;
		this.db = tableDatabase;
	}
	
	@Override
	public int getId() {
		return id;
	}

	@InfoSchemaColumn(logicalName="name", fieldName="name",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={})
	public String getTableName() {
		return name;
	}

	@InfoSchemaColumn(logicalName="dbname", fieldName="db",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={})
	public String getDatabaseName() {
		return db;
	}

	@InfoSchemaColumn(logicalName="engine", fieldName="engine",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={})
	public String getEngineName() {
		return engine;
	}
	

	@InfoSchemaColumn(logicalName="session",fieldName="sessionID",
			sqlType=java.sql.Types.INTEGER,
			views={})
	public int getSessionID() {
		return sessionID;
	}
	
	@InfoSchemaColumn(logicalName="server", fieldName="server",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={})
	public ServerRegistration getServer() {
		return server;
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
		// TODO Auto-generated method stub

	}

	@Override
	public void onDrop() {
		// TODO Auto-generated method stub

	}

}
