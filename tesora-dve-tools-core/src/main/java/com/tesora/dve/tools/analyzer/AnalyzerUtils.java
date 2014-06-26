package com.tesora.dve.tools.analyzer;

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

import com.tesora.dve.db.DBNative;
import com.tesora.dve.db.mysql.MysqlNativeType;
import com.tesora.dve.db.mysql.MysqlNativeType.MysqlType;
import com.tesora.dve.tools.analyzer.jaxb.ColumnsType.Column;
import com.tesora.dve.tools.analyzer.jaxb.DatabasesType.Database;
import com.tesora.dve.tools.analyzer.jaxb.IndexesType.Index;
import com.tesora.dve.tools.analyzer.jaxb.KeysType.ForeignKey;
import com.tesora.dve.tools.analyzer.jaxb.KeysType.PrimaryKey;
import com.tesora.dve.tools.analyzer.jaxb.TablesType.Table;

public final class AnalyzerUtils {
	public static List<String> getTableNames(final Database db) {
		final List<String> out = new ArrayList<String>();
		if ((db != null) && (db.getTables() != null)) {
			for (final Table table : db.getTables().getTable()) {
				out.add(table.getName());
			}
		}

		return out;
	}

	public static List<String> buildCreateTableStatements(final Database db, final DBNative dbNative) {
		final List<String> creates = new ArrayList<String>();
		if ((db != null) && (db.getTables() != null)) {
			for (final Table table : db.getTables().getTable()) {
				if (table.isView() == Boolean.TRUE) {
					continue;
				}
				creates.add(buildCreateTableStatement(table, dbNative));
			}
		}

		return creates;
	}

	public static List<String> buildCreateViewStatements(final Database db, final DBNative dbNative) {
		final List<String> creates = new ArrayList<String>();
		if ((db != null) && (db.getTables() != null)) {
			for (final Table table : db.getTables().getTable()) {
				if (table.isView() != Boolean.TRUE) {
					continue;
				}
				creates.add(buildCreateTableStatement(table, dbNative));
			}
		}

		return creates;
	}

	private static void emitType(Column column, StringBuffer buf) {
		final String type = column.getType();
		// handle 'int unsigned', etc.
		if (type.toLowerCase().indexOf(MysqlNativeType.MODIFIER_UNSIGNED.toLowerCase()) > 0) {
			final String[] bits = type.split(" ");
			buf.append(bits[0]);
			emitSize(column, buf);
			buf.append(" ").append(bits[1]);
		} else if (MysqlType.ENUM.toString().equalsIgnoreCase(type)
				|| MysqlType.SET.toString().equalsIgnoreCase(type)) {
			/*
			 * TODO We currently do not store the full ENUM/SET declaration in
			 * our static reports.
			 * It is, however, not necessary for most tasks performed by the
			 * analyzer.
			 */
			buf.append(type).append(" ('").append(column.getSize()).append("')");
		} else {
			buf.append(type);
			emitSize(column, buf);
		}
	}

	private static String buildCreateTableStatement(Table table, final DBNative dbNative) {
		final StringBuffer cBuffer = new StringBuffer(50);
		for (final Column column : table.getColumns().getColumn()) {
			if (cBuffer.length() > 0) {
				cBuffer.append(",\r");
			}

			cBuffer.append("  ").append(dbNative.quoteIdentifier(column.getName())).append(' ');
			emitType(column, cBuffer);
			if ((column.isNullable() != null) && !column.isNullable()) {
				cBuffer.append(" NOT NULL");
			}
			if ((column.isAutoIncr() != null) && column.isAutoIncr()) {
				cBuffer.append(" AUTO_INCREMENT");
			}
		}

		final StringBuffer fkBuffer = new StringBuffer();
		final StringBuffer pkBuffer = new StringBuffer();
		final StringBuffer idxBuffer = new StringBuffer();
		if (table.isView() != Boolean.TRUE) {
			if (table.getKeys() != null) {
				final StringBuffer subBuf = new StringBuffer();

				for (final PrimaryKey pk : table.getKeys().getPrimaryKey()) {
					if (subBuf.length() > 0) {
						subBuf.append(", ");
					}
					subBuf.append(dbNative.quoteIdentifier(pk.getColumn()));
				}
				if (subBuf.length() > 0) {
					pkBuffer.append("  PRIMARY KEY (").append(subBuf).append(')');
				}
			}

			if (table.getIndexes() != null) {
				String currentIndexName = null;
				List<Index> parts = new ArrayList<Index>();

				for (final Index index : table.getIndexes().getIndex()) {
					if (index.getName().equals("PRIMARY")) {
						continue;
					}
					if ((currentIndexName == null) || !currentIndexName.equals(index.getName())) {
						if (currentIndexName != null) {
							if (idxBuffer.length() > 0) {
								idxBuffer.append(",\r");
							}
							idxBuffer.append(emitIndex(parts, dbNative));
						}
						currentIndexName = index.getName();
						parts = new ArrayList<Index>();
						parts.add(index);
					} else {
						parts.add(index);
					}
				}
				if (!parts.isEmpty()) {
					if (idxBuffer.length() > 0) {
						idxBuffer.append(",\r");
					}
					idxBuffer.append(emitIndex(parts, dbNative));
				}
			}

			if (table.getKeys() != null) {
				String currentFKName = null;
				List<ForeignKey> parts = new ArrayList<ForeignKey>();
				for (final ForeignKey fk : table.getKeys().getForeignKey()) {
					if ((currentFKName == null) || !currentFKName.equals(fk.getName())) {
						if (currentFKName != null) {
							if (fkBuffer.length() > 0) {
								fkBuffer.append(",\r");
							}
							fkBuffer.append(emitFK(parts, dbNative));
						}
						currentFKName = fk.getName();
						parts = new ArrayList<ForeignKey>();
						parts.add(fk);
					} else {
						parts.add(fk);
					}
				}
				if (!parts.isEmpty()) {
					if (fkBuffer.length() > 0) {
						fkBuffer.append(",\r");
					}
					fkBuffer.append(emitFK(parts, dbNative));
				}
			}
		}

		final StringBuffer buffer = new StringBuffer(50);
		// If it's a view we're going to start with the create table statement and then append the table bit (our extension)
		// on the end.
		if (table.isView() == Boolean.TRUE) {
			buffer.append(table.getScts()).append(" TABLE (\r");
		} else {
			buffer.append("CREATE TABLE ").append(dbNative.quoteIdentifier(table.getName())).append(" (\r");
		}
		buffer.append(cBuffer);
		if (pkBuffer.length() > 0) {
			buffer.append(",\r").append(pkBuffer);
		}
		if (idxBuffer.length() > 0) {
			buffer.append(",\r").append(idxBuffer);
		}
		if (fkBuffer.length() > 0) {
			buffer.append(",\r").append(fkBuffer);
		}
		buffer.append("\r)");

		return buffer.toString();
	}

	private static void emitSize(Column column, StringBuffer buf) {
		if (column.getSize() != null) {
			buf.append(" (").append(column.getSize()).append(")");
		}
	}

	private static String emitIndex(List<Index> parts, final DBNative dbNative) {
		final StringBuffer buf = new StringBuffer();
		boolean first = true;
		for (final Index index : parts) {
			if (buf.length() > 0) {
				buf.append(", ");
			}
			if (first) {
				if (!index.isNonUnique()) {
					buf.append("UNIQUE ");
				}
				buf.append("KEY ").append(dbNative.quoteIdentifier(index.getName())).append(" (");
				first = false;
			}
			buf.append(dbNative.quoteIdentifier(index.getColumn()));
		}
		buf.append(')');
		return buf.toString();
	}

	private static String emitFK(List<ForeignKey> parts, final DBNative dbNative) {
		final StringBuffer header = new StringBuffer();
		final StringBuffer target = new StringBuffer();

		boolean first = true;
		for (final ForeignKey fk : parts) {
			if (header.length() > 0) {
				header.append(", ");
				target.append(", ");
			}
			if (first) {
				header.append("CONSTRAINT ").append(dbNative.quoteIdentifier(fk.getName())).append(" FOREIGN KEY (");
				target.append(" REFERENCES ").append(dbNative.quoteIdentifier(fk.getRefTable())).append(" (");
				first = false;
			}
			header.append(dbNative.quoteIdentifier(fk.getColumn()));
			target.append(dbNative.quoteIdentifier(fk.getRefColumn()));
		}
		header.append(')');
		target.append(')');

		return header.toString() + target.toString();
	}
}
