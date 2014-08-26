package com.tesora.dve.sql.infoschema;

import java.util.Collections;
import java.util.EnumMap;

import com.tesora.dve.common.catalog.FKMode;
import com.tesora.dve.common.catalog.MultitenantMode;
import com.tesora.dve.common.catalog.TemplateMode;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.show.ShowView;
import com.tesora.dve.sql.parser.ParserOptions;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PEPersistentGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.transexec.TransientExecutionEngine;
import com.tesora.dve.sql.util.Pair;

public class ViewSchemaBuilder implements InformationSchemaBuilder {

	private final PEDatabase catalogSchema;
	
	public ViewSchemaBuilder(PEDatabase catSchema) {
		this.catalogSchema = catSchema;
	}
	
	@Override
	public void populate(LogicalInformationSchema logicalSchema,
			InformationSchemaView infoSchema, ShowView showSchema,
			MysqlView mysqlSchema, DBNative dbn) throws PEException {
		TransientExecutionEngine tee = new TransientExecutionEngine(catalogSchema.getName().get(),dbn.getTypeCatalog());
		SchemaContext sc = tee.getPersistenceContext();

		EnumMap<InfoView,SchemaView> schemaByView = new EnumMap<InfoView,SchemaView>(InfoView.class);
		schemaByView.put(infoSchema.getView(), infoSchema);
		schemaByView.put(showSchema.getView(), showSchema);
		schemaByView.put(mysqlSchema.getView(), mysqlSchema);
		
		tee.setCurrentDatabase(catalogSchema);

		for(InfoSchemaGenerator g : generators) {
			ViewBasedInformationSchemaTableView view = g.generate(sc);
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
