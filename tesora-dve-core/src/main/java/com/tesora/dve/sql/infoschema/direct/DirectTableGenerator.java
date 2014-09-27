package com.tesora.dve.sql.infoschema.direct;

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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.parser.InvokeParser;
import com.tesora.dve.sql.parser.ParserOptions;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.types.Type;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.UnaryFunction;

public abstract class DirectTableGenerator extends DirectSchemaGenerator {

	protected final InfoView view;
	protected final String tableName;
	protected final String pluralTableName;
	protected final List<DirectColumnGenerator> columns;

	public DirectTableGenerator(InfoView view, String tableName, String pluralTableName,
			DirectColumnGenerator...colDefs) {
		this.view = view;
		this.tableName = tableName;
		this.pluralTableName = pluralTableName;
		this.columns = Arrays.asList(colDefs);
	}
	
	public DirectTableGenerator withExtension() {
		return super.withExtension();
	}
	
	public DirectTableGenerator withPrivilege() {
		return super.withPrivilege();
	}
	
	public abstract DirectInformationSchemaTable generate(SchemaContext sc);

	protected List<PEColumn> buildColumns(final SchemaContext sc) {
		final ParserOptions topts = ParserOptions.TEST.setResolve().setIgnoreMissingUser().setInfoSchemaView();
		return Functional.apply(columns, new UnaryFunction<PEColumn,DirectColumnGenerator>() {

			@Override
			public PEColumn evaluate(DirectColumnGenerator object) {
				Type type = InvokeParser.parseType(sc, topts, object.getType());
				return PEColumn.buildColumn(sc, new UnqualifiedName(object.getName()), type, Collections.EMPTY_LIST, null, Collections.EMPTY_LIST);
			}
			
		});
	}
	
}
