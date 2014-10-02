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

import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.externalservice.ExternalServiceFactory;
import com.tesora.dve.externalservice.ExternalServicePlugin;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.IntermediateResultSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.infoschema.InfoView;
import com.tesora.dve.sql.infoschema.InformationSchemaException;
import com.tesora.dve.sql.infoschema.ShowOptions;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.ddl.SchemaQueryStatement;

public class DirectShowExternalService extends DirectShowSchemaTable {

	public DirectShowExternalService(SchemaContext sc, 
			List<PEColumn> cols, List<DirectColumnGenerator> columnGenerators) {
		super(sc, InfoView.SHOW, cols, new UnqualifiedName("external service"), null, true,true,
				columnGenerators);
	}

	@Override
	public Statement buildShowPlural(SchemaContext sc, List<Name> scoping,
			ExpressionNode likeExpr, ExpressionNode whereExpr,
			ShowOptions options) {
		throw new InformationSchemaException("Illegal operation: show external service does not support multiple targets");
	}

	@Override
	public Statement buildUniqueStatement(SchemaContext sc, Name onName,
			ShowOptions opts) {
		String serviceName = onName.getUnqualified().get();

		ColumnSet md = new ColumnSet();
		md.addColumn("Name", 255, "varchar", java.sql.Types.VARCHAR);
		md.addColumn("Status", 255, "varchar", java.sql.Types.VARCHAR);

		ResultRow rr = new ResultRow();
		boolean isRegistered = ExternalServiceFactory.isRegistered(serviceName);
		if (!isRegistered) {
			throw new InformationSchemaException("Cannot obtain status for external service '"
					+ serviceName + "' because the external service is not registered.");
		}

		try {
			ExternalServicePlugin plugin = ExternalServiceFactory.getInstance(serviceName);
			rr.addResultColumn(serviceName);
			rr.addResultColumn(plugin.status());
		} catch (Exception e) {
			throw new InformationSchemaException("Cannot obtain status for external service '"
					+ serviceName + "'", e);
		}

		List<ResultRow> rows = new ArrayList<ResultRow>();
		rows.add(rr);

		return new SchemaQueryStatement(true,getName().get(),new IntermediateResultSet(md, rows));
	}

}
