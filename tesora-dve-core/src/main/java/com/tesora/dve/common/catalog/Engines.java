// OS_STATUS: public
package com.tesora.dve.common.catalog;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

import com.tesora.dve.common.ShowSchema;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.infoschema.annos.ColumnView;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaColumn;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaTable;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.annos.TableView;

@InfoSchemaTable(logicalName = "engines",
		views = { @TableView(view = InfoView.SHOW, name = "engines", pluralName = "",
				columnOrder = { ShowSchema.Engines.ENGINE,
						ShowSchema.Engines.SUPPORT,
						ShowSchema.Engines.COMMENT,
						ShowSchema.Engines.TRANSACTIONS,
						ShowSchema.Engines.XA,
						ShowSchema.Engines.SAVEPOINTS }),
				@TableView(view = InfoView.INFORMATION, name = "engines", pluralName = "",
						columnOrder = { "engine", "support", "comment", "transactions", "xa", "savepoints"
						}) })
@Entity
@Table(name = "engines")
public class Engines implements CatalogEntity {
	
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue
	@Column(name = "id", columnDefinition = "integer(11)")
	int id;

	@Column(name = "engine", columnDefinition = "varchar(64) default ''", nullable = false)
	String engine;

	@Column(name = "support", columnDefinition = "varchar(8) default ''", nullable = false)
	String support;

	@Column(name = "comment", columnDefinition = "varchar(80) default ''", nullable = false)
	String comment;

	@Column(name = "transactions", columnDefinition = "varchar(3)", nullable = true)
	String transactions;

	@Column(name = "xa", columnDefinition = "varchar(3)", nullable = true)
	String xa;

	@Column(name = "savepoints", columnDefinition = "varchar(3)", nullable = true)
	String savepoints;

	public Engines(String engine, String support, String comment, String transactions, String xa, String savepoints) {
		this.engine = engine;
		this.support = support;
		this.comment = comment;
		this.transactions = transactions;
		this.xa = xa;
		this.savepoints = savepoints;
	}

	public Engines() {
	}

	@Override
	public int getId() {
		return id;
	}

	@InfoSchemaColumn(logicalName = "engine", fieldName = "engine",
			sqlType = java.sql.Types.VARCHAR, sqlWidth = 64,
			views = { @ColumnView(view = InfoView.SHOW, name = ShowSchema.Engines.ENGINE, orderBy = true, ident = true),
					@ColumnView(view = InfoView.INFORMATION, name = "engine", orderBy = true, ident = true) })
	public String getEngine() {
		return engine;
	}

	@InfoSchemaColumn(logicalName = "support", fieldName = "support",
			sqlType = java.sql.Types.VARCHAR, sqlWidth = 8,
			views = { @ColumnView(view = InfoView.SHOW, name = ShowSchema.Engines.SUPPORT),
					@ColumnView(view = InfoView.INFORMATION, name = "support") })
	public String getSupport() {
		return support;
	}

	@InfoSchemaColumn(logicalName = "comment", fieldName = "comment",
			sqlType = java.sql.Types.VARCHAR, sqlWidth = 80,
			views = { @ColumnView(view = InfoView.SHOW, name = ShowSchema.Engines.COMMENT),
					@ColumnView(view = InfoView.INFORMATION, name = "comment") })
	public String getComment() {
		return comment;
	}

	@InfoSchemaColumn(logicalName = "transactions", fieldName = "transactions",
			sqlType = java.sql.Types.VARCHAR, sqlWidth = 3,
			views = { @ColumnView(view = InfoView.SHOW, name = ShowSchema.Engines.TRANSACTIONS),
					@ColumnView(view = InfoView.INFORMATION, name = "transactions") })
	public String getTransactions() {
		return transactions;
	}

	@InfoSchemaColumn(logicalName = "xa", fieldName = "xa",
			sqlType = java.sql.Types.VARCHAR, sqlWidth = 3,
			views = { @ColumnView(view = InfoView.SHOW, name = ShowSchema.Engines.XA),
					@ColumnView(view = InfoView.INFORMATION, name = "xa") })
	public String getXa() {
		return xa;
	}

	@InfoSchemaColumn(logicalName = "savepoints", fieldName = "savepoints",
			sqlType = java.sql.Types.VARCHAR, sqlWidth = 3,
			views = { @ColumnView(view = InfoView.SHOW, name = ShowSchema.Engines.SAVEPOINTS),
					@ColumnView(view = InfoView.INFORMATION, name = "savepoints") })
	public String getSavepoints() {
		return savepoints;
	}

	@Override
	public ColumnSet getShowColumnSet(CatalogQueryOptions cqo)
			throws PEException {
		// no show
		return null;
	}

	@Override
	public ResultRow getShowResultRow(CatalogQueryOptions cqo)
			throws PEException {
		// no show
		return null;
	}

	@Override
	public void removeFromParent() throws Throwable {
		// no parents

	}

	@Override
	public List<? extends CatalogEntity> getDependentEntities(CatalogDAO c)
			throws Throwable {
		// no dependents
		return null;
	}

	@Override
	public void onUpdate() {
		// do nothing
	}

	@Override
	public void onDrop() {
		// do nothing
	}
	
	
	public static List<Engines> getDefaultEngines() {
		List<Engines> list = new ArrayList<Engines>();
		list.add(new Engines("InnoDB", "DEFAULT", "Supports transactions, row-level locking, and foreign keys", "YES", "YES", "YES"));
		list.add(new Engines("MyISAM", "YES", "MyISAM storage engine",	"NO", "NO", "NO"));
		return list;
	}


}
