package com.tesora.dve.sql.infoschema;

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

import java.util.List;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.sql.infoschema.show.ShowOptions;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.Statement;

public interface ShowSchemaBehavior {

	public Statement buildShowPlural(SchemaContext sc, List<Name> scoping, ExpressionNode likeExpr, ExpressionNode whereExpr, ShowOptions options);

	public Statement buildUniqueStatement(SchemaContext sc, Name objectName);

	public List<CatalogEntity> getLikeSelectEntities(SchemaContext sc, String likeExpr, List<Name> scoping, ShowOptions options, Boolean overrideRequiresPrivilegeValue);

}
