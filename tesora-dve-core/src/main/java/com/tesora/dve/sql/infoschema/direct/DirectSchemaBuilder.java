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

import java.util.EnumMap;

import com.tesora.dve.db.DBNative;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.infoschema.InfoSchemaGenerator;
import com.tesora.dve.sql.infoschema.InformationSchemaBuilder;
import com.tesora.dve.sql.infoschema.InformationSchema;
import com.tesora.dve.sql.infoschema.LogicalInformationSchema;
import com.tesora.dve.sql.infoschema.MysqlSchema;
import com.tesora.dve.sql.infoschema.AbstractInformationSchema;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.show.ShowView;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transexec.TransientExecutionEngine;

public class DirectSchemaBuilder implements InformationSchemaBuilder {

	private final PEDatabase catalogSchema;
	
	public DirectSchemaBuilder(PEDatabase catSchema) {
		this.catalogSchema = catSchema;
	}
	
	@Override
	public void populate(LogicalInformationSchema logicalSchema,
			InformationSchema infoSchema, ShowView showSchema,
			MysqlSchema mysqlSchema, DBNative dbn) throws PEException {
		if (catalogSchema == null) // transient case, but we aren't doing any info schema queries then anyhow
			return;
		TransientExecutionEngine tee = new TransientExecutionEngine(catalogSchema.getName().get(),dbn.getTypeCatalog());
		SchemaContext sc = tee.getPersistenceContext();

		EnumMap<InfoView,AbstractInformationSchema> schemaByView = new EnumMap<InfoView,AbstractInformationSchema>(InfoView.class);
		schemaByView.put(infoSchema.getView(), infoSchema);
		schemaByView.put(showSchema.getView(), showSchema);
		schemaByView.put(mysqlSchema.getView(), mysqlSchema);
		
		tee.setCurrentDatabase(catalogSchema);

		for(InfoSchemaGenerator g : generators) {
			DirectInformationSchemaTable view = g.generate(sc);
			schemaByView.get(view.getView()).viewReplace(sc, view);
		}
	}

	private InfoSchemaGenerator[] generators = new InfoSchemaGenerator[] {
		
			/*
			 * 	
	public InfoSchemaGenerator(InfoView view, String name, String columnDef, String viewDef,
			boolean privileged, boolean extension, String identColumn, String orderByColumn) {

			 */
			new InfoSchemaGenerator(InfoView.INFORMATION,"generation_site",
					"`group` varchar(512), `version` int, `site` varchar(512)",
					"select pg.name as `group`, sg.version as `version`, ss.name as `site` from "
					+"generation_sites gs, storage_site ss, storage_generation sg, persistent_group pg where "
					+"sg.persistent_group_id = pg.persistent_group_id and "
					+"gs.site_id = ss.id and "
					+"gs.generation_id = sg.generation_id order by pg.name, sg.version, ss.name",
					true,true,null,null)
	};
	
}
