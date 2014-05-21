// OS_STATUS: public
package com.tesora.dve.sql.statement.ddl.alter;

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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tesora.dve.db.DBNative;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.resultset.ColumnMetadata;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.modifiers.CharsetTableModifier;
import com.tesora.dve.sql.schema.modifiers.CollationTableModifier;
import com.tesora.dve.sql.schema.types.TextType;
import com.tesora.dve.sql.statement.ddl.alter.AlterTableAction.ClonableAlterTableAction;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;

public class ConvertToAction extends ClonableAlterTableAction {

	public class ConvertColumnAction extends AbstractAlterColumnAction {

		final List<PEColumn> columns;

		protected ConvertColumnAction(final List<PEColumn> columns) {
			this.columns = columns;
		}

		@Override
		public List<PEColumn> getColumns() {
			return this.columns;
		}

		@Override
		public AlterTableAction alterTable(SchemaContext sc, PETable tab) {
			final DBNative nativeDb = Singletons.require(HostService.class).getDBNative();
			for (final PEColumn column : this.columns) {
				if (column.hasStringType()) {
					final Name columnName = column.getName();
					final ColumnMetadata metadata = ConvertToAction.this.columnMetadata.get(columnName.get());
					if (metadata != null) {
						// First change the column type, then update the charset.
						column.setType(TextType.buildType(metadata.getDataType(), metadata.getSize(), nativeDb));
						column.takeCharsetSettings(ConvertToAction.this.charSet, ConvertToAction.this.collation, true);
					} else {
						throw new PECodingException("No native metadata available for column " + columnName.getSQL());
					}
				}
			}
			return null;
		}

		@Override
		public boolean isNoop(SchemaContext sc, PETable tab) {
			return false;
		}

		@Override
		public String isValid(SchemaContext sc, PETable tab) {
			return null;
		}

		@Override
		public AlterTableAction adapt(SchemaContext sc, PETable actual) {
			return null;
		}

		@Override
		public Action getActionKind() {
			return Action.ALTER;
		}

		public boolean hasSQL(SchemaContext sc, PETable pet) {
			return false;
		}
	}

	private final CharsetTableModifier charSet;
	private final CollationTableModifier collation;
	private final Map<String, ColumnMetadata> columnMetadata = new HashMap<String, ColumnMetadata>();

	public ConvertToAction(final String charSet, final String collation) {
		if ((charSet == null) || (collation == null)) {
			throw new IllegalArgumentException("Both CHARACTER SET and COLLATION must be specified.");
		}
		this.charSet = new CharsetTableModifier(new UnqualifiedName(charSet));
		this.collation = new CollationTableModifier(new UnqualifiedName(collation));
	}

	private ConvertToAction(final ConvertToAction other) {
		this.charSet = new CharsetTableModifier(other.charSet.getCharset());
		this.collation = new CollationTableModifier(other.collation.getCollation());
	}

	@Override
	public AlterTableAction alterTable(SchemaContext sc, PETable tab) {
		tab.alterModifier(this.charSet);
		tab.alterModifier(this.collation);

		return new ConvertColumnAction(tab.getColumns(sc));
	}

	@Override
	public boolean isNoop(SchemaContext sc, PETable tab) {
		return false;
	}

	@Override
	public String isValid(SchemaContext sc, PETable tab) {
		return null;
	}

	@Override
	public AlterTableAction adapt(SchemaContext sc, PETable actual) {
		return null;
	}

	@Override
	public AlterTargetKind getTargetKind() {
		return AlterTargetKind.TABLE;
	}

	@Override
	public Action getActionKind() {
		return Action.ALTER;
	}
	
	@Override
	protected ConvertToAction clone() {
		return new ConvertToAction(this);
	}

	public Name getCharSetName() {
		return this.charSet.getCharset();
	}
	
	public Name getCollationName() {
		return this.collation.getCollation();
	}

	public Map<String, ColumnMetadata> getColumnMetadataContainer() {
		return this.columnMetadata;
	}

}
