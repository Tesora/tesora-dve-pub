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

import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.persist.PersistedEntity;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;

import java.util.List;

/**
 *
 */
public interface InformationSchemaService {
    InformationSchema getInfoSchema();

    PEDatabase getCatalogSchema();

    ShowView getShowSchema();

    ShowSchemaBehavior lookupShowTable(UnqualifiedName unq);

    MysqlSchema getMysqlSchema();

    List<PersistedEntity> buildEntities(int groupid, int modelid, String charSet, String collation) throws PEException;

    InformationSchemaDatabase buildPEDatabase(SchemaContext sc, UserDatabase udb);
}
