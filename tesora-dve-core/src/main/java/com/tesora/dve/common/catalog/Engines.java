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
		final List<Engines> list = new ArrayList<Engines>();

		/* Officially supported engines. */
		list.add(new Engines("InnoDB", "DEFAULT", "Supports transactions, row-level locking, and foreign keys", "YES", "YES", "YES"));
		list.add(new Engines("MyISAM", "YES", "MyISAM storage engine", "NO", "NO", "NO"));
		list.add(new Engines("MEMORY", "YES", "Hash based, stored in memory, useful for temporary tables", "NO", "NO", "NO"));
		list.add(new Engines("ARCHIVE", "YES", "Archive storage engine", "NO", "NO", "NO"));
		list.add(new Engines("CSV", "YES", "CSV storage engine", "NO", "NO", "NO"));

		/* Unsupported engines. */
		list.add(new Engines("BLACKHOLE", "NO", "/dev/null storage engine (anything you write to it disappears)", "NO", "NO", "NO"));
		list.add(new Engines("FEDERATED", "NO", "Federated MySQL storage engine", null, null, null));
		list.add(new Engines("PERFORMANCE_SCHEMA", "NO", "Performance Schema", "NO", "NO", "NO"));

		return list;
	}


}
