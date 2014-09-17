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

import com.tesora.dve.common.ShowSchema;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.exceptions.PEException;
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

		for(DirectTableGenerator g : generators) {
			DirectInformationSchemaTable view = g.generate(sc);
			schemaByView.get(view.getView()).viewReplace(sc, view);
		}
	}

	private static final String gen_sites_format = 
			"select pg.name as `%s`, sg.version as `%s`, ss.name as `%s` from "
					+"generation_sites gs, storage_site ss, storage_generation sg, persistent_group pg where "
					+"sg.persistent_group_id = pg.persistent_group_id and "
					+"gs.site_id = ss.id and "
					+"gs.generation_id = sg.generation_id order by 1,2,3";
	private DirectTableGenerator[] generators = new DirectTableGenerator[] {
		
			new DirectTableGenerator(InfoView.INFORMATION, "generation_site", null,
					"generation_site",
					String.format(gen_sites_format,"group","version","site"),
					new DirectColumnGenerator("group","varchar(512)"),
					new DirectColumnGenerator("version","int"),
					new DirectColumnGenerator("site","varchar(512)"))
					.withExtension().withPrivilege(),
			new DirectTableGenerator(InfoView.SHOW, "generation site", "generation sites",
					"show_generation_site",
					String.format(gen_sites_format,ShowSchema.GenerationSite.NAME,
							ShowSchema.GenerationSite.VERSION,
							ShowSchema.GenerationSite.SITE),
					new DirectColumnGenerator(ShowSchema.GenerationSite.NAME,"varchar(512)").withIdent(),
					new DirectColumnGenerator(ShowSchema.GenerationSite.VERSION,"int"),
					new DirectColumnGenerator(ShowSchema.GenerationSite.SITE,"varchar(512)"))
					.withExtension().withPrivilege(),
			new DirectTableGenerator(InfoView.INFORMATION,"scopes", null,
					"scopes",
					"select ut.name as `TABLE_NAME`, ud.name as `TABLE_SCHEMA`, ut.state as `TABLE_STATE`, t.ext_tenant_id as `TENANT_NAME`, s.local_name as `SCOPE_NAME` "
					+"from user_table ut inner join user_database ud on ut.user_database_id = ud.user_database_id "
					+"left outer join scope s on s.scope_table_id = ut.table_id "
					+"left outer join tenant t on s.scope_tenant_id = t.tenant_id "
					+"where ud.multitenant_mode = 'adaptive'",
					new DirectColumnGenerator("TABLE_NAME","varchar(255)"),
					new DirectColumnGenerator("TABLE_SCHEMA","varchar(255)"),
					new DirectColumnGenerator("TABLE_STATE","varchar(255)"),
					new DirectColumnGenerator("TENANT_NAME","varchar(255)"),
					new DirectColumnGenerator("SCOPE_NAME","varchar(255)"))
					.withExtension().withPrivilege(),
			new DirectTableGenerator(InfoView.INFORMATION,
					"distributions",null,
					"distributions",
					"select ud.name as `DATABASE_NAME`, ut.name as `TABLE_NAME`, uc.name as `COLUMN_NAME`, "
					+"uc.hash_position as `VECTOR_POSITION`, dm.name as `MODEL_TYPE`, dr.name as `MODEL_NAME` "
					+"from user_table ut inner join user_database ud on ut.user_database_id = ud.user_database_id "
					+"inner join distribution_model dm on ut.distribution_model_id = dm.id "
					+"left outer join user_column uc on uc.user_table_id = ut.table_id and uc.hash_position > 0 "
					+"left outer join range_table_relation rtr on ut.table_id = rtr.table_id "
					+"left outer join distribution_range dr on rtr.range_id = dr.range_id",
					new DirectColumnGenerator("DATABASE_NAME","varchar(255)"),
					new DirectColumnGenerator("TABLE_NAME","varchar(255)"),
					new DirectColumnGenerator("COLUMN_NAME","varchar(255)"),
					new DirectColumnGenerator("VECTOR_POSITION","int(11)"),
					new DirectColumnGenerator("MODEL_TYPE","varchar(255)"),
					new DirectColumnGenerator("MODEL_NAME","varchar(255)"))
					.withExtension(),
			new DirectTableGenerator(InfoView.INFORMATION,
					"character_sets", null,
					"character_sets",
					"select cs.character_set_name as `CHARACTER_SET_NAME`, cs.description as `DESCRIPTION`, cs.maxlen as `MAXLEN` "
					+"from character_sets cs order by cs.character_set_name",
					new DirectColumnGenerator("CHARACTER_SET_NAME","varchar(32)"),
					new DirectColumnGenerator("DESCRIPTION","varchar(60)"),
					new DirectColumnGenerator("MAXLEN","int(11)")),
			new DirectTableGenerator(InfoView.SHOW,
					"charset",null,
					"show_character_sets",
					"select cs.character_set_name as `Charset`, cs.description as `Description`, cs.maxlen as `Maxlen` "
					+"from character_sets cs order by cs.character_set_name",
					new DirectColumnGenerator("Charset","varchar(32)").withIdent().withOrderBy(0),
					new DirectColumnGenerator("Description","varchar(60)"),
					new DirectColumnGenerator("Maxlen","int(11)")),
			new DirectTableGenerator(InfoView.INFORMATION,
					"collations", null,
					"collations",
					// ah, let the stupid mysqlisms begin
					"select c.name as `COLLATION_NAME`, c.character_set_name as `CHARACTER_SET_NAME`, "
					+"cast((c.id+1)-1 as signed integer) as `ID`, "
					+"case c.is_default when 1 then 'Yes' else '' end as `IS_DEFAULT`, "
					+"case c.is_compiled when 1 then 'Yes' else 'No' end as `IS_COMPILED`, "
					+"c.sortlen as `SORTLEN` "
					+"from collations c order by c.character_set_name, c.id",
					new DirectColumnGenerator("COLLATION_NAME","varchar(32)"),
					new DirectColumnGenerator("CHARACTER_SET_NAME","varchar(32)"),
					new DirectColumnGenerator("ID","bigint(11)"),
					new DirectColumnGenerator("IS_DEFAULT","varchar(3)"),
					new DirectColumnGenerator("IS_COMPILED","varchar(3)"),
					new DirectColumnGenerator("SORTLEN","bigint(3)")),
			new DirectTableGenerator(InfoView.SHOW,
					"collation", null,
					"show_collations",
					"select c.name as `Collation`, c.character_set_name as `Charset`, "
					+"cast((c.id+1)-1 as signed integer) as `Id`, "
					+"case c.is_default when 1 then 'Yes' else '' end as `Default`, "
					+"case c.is_compiled when 1 then 'Yes' else 'No' end as `Compiled`, "
					+"c.sortlen as `Sortlen` "
					+"from collations c order by c.character_set_name, c.id",
					new DirectColumnGenerator("Collation","varchar(32)").withIdent().withOrderBy(0),
					new DirectColumnGenerator("Charset","varchar(32)"),
					new DirectColumnGenerator("Id","bigint(11)").withOrderBy(1),
					new DirectColumnGenerator("Default","varchar(3)"),
					new DirectColumnGenerator("Compiled","varchar(3)"),
					new DirectColumnGenerator("Sortlen","bigint(3)")),
			new DirectTableGenerator(InfoView.SHOW,
					"dynamic site policy", "dynamic site policies",
					"show_dyn_site_policies",
					"select d.name as `Name`, case d.strict when 1 then '1' else '0' end as `strict`, "
					+"d.aggregate_class as `aggregate_class`, d.aggregate_count as `aggregate_count`, d.aggregate_provider as `aggregate_provider`, "
					+"d.small_class as `small_class`, d.small_count as `small_count`, d.small_provider as `small_provider`, "
					+"d.medium_class as `medium_class`, d.medium_count as `medium_count`, d.medium_provider as `medium_provider`, "
					+"d.large_class as `large_class`, d.large_count as `large_count`, d.large_provider as `large_provider` "
					+"from dynamic_policy d",
					new DirectColumnGenerator("Name","varchar(255)").withIdent().withOrderBy(0),
					new DirectColumnGenerator("strict","int(11)"),
					new DirectColumnGenerator("aggregate_class","varchar(255)"),
					new DirectColumnGenerator("aggregate_count","int(11)"),
					new DirectColumnGenerator("aggregate_provider","varchar(255)"),
					new DirectColumnGenerator("small_class","varchar(255)"),
					new DirectColumnGenerator("small_count","int(11)"),
					new DirectColumnGenerator("small_provider","varchar(255)"),
					new DirectColumnGenerator("medium_class","varchar(255)"),
					new DirectColumnGenerator("medium_count","int(11)"),
					new DirectColumnGenerator("medium_provider","varchar(255)"),
					new DirectColumnGenerator("large_class","varchar(255)"),
					new DirectColumnGenerator("large_count","int(11)"),
					new DirectColumnGenerator("large_provider","varchar(255)"))
					.withExtension(),
			new DirectTableGenerator(InfoView.INFORMATION,
					"engines",null,
					"engines",
					"select e.engine as `ENGINE`, e.support as `SUPPORT`, e.comment as `COMMENT`, e.transactions as `TRANSACTIONS`, "
					+"e.xa as `XA`, e.savepoints as `SAVEPOINTS` from engines e order by e.engine",
					new DirectColumnGenerator("ENGINE","varchar(64)"),
					new DirectColumnGenerator("SUPPORT","varchar(8)"),
					new DirectColumnGenerator("COMMENT","varchar(80)"),
					new DirectColumnGenerator("TRANSACTIONS","varchar(3)"),
					new DirectColumnGenerator("XA","varchar(3)"),
					new DirectColumnGenerator("SAVEPOINTS","varchar(3)")),
			new DirectTableGenerator(InfoView.SHOW,
					"engines",null,
					"show_engines",
					"select e.engine as `Engine`, e.support as `Support`, e.comment as `Comment`, e.transactions as `Transactions`, "
					+"e.xa as `XA`, e.savepoints as `Savepoints` from engines e order by e.engine",
					new DirectColumnGenerator("Engine","varchar(64)").withIdent().withOrderBy(0),
					new DirectColumnGenerator("Support","varchar(8)"),
					new DirectColumnGenerator("Comment","varchar(80)"),
					new DirectColumnGenerator("Transactions","varchar(3)"),
					new DirectColumnGenerator("XA","varchar(3)"),
					new DirectColumnGenerator("Savepoints","varchar(3)")),
			new DirectTableGenerator(InfoView.INFORMATION,
					"key_column_usage",null,
					"key_column_usage",
					"select 'def' as `CONSTRAINT_CATALOG`, 'def' as `TABLE_CATALOG`, sdb.name as `TABLE_SCHEMA`, sut.name as `TABLE_NAME`, sc.name as `COLUMN_NAME`, "
					+"ukc.position as `ORDINAL_POSITION`, "
					+"coalesce(tdb.name, uk.forward_schema_name) as `REFERENCED_TABLE_SCHEMA`, "
					+"coalesce(tut.name, uk.forward_table_name) as `REFERENCED_TABLE_NAME`, "
					+"coalesce(tc.name, ukc.forward_column_name) as `REFERENCED_COLUMN_NAME` "
					+"from user_key_column ukc "
					+"inner join user_key uk on ukc.key_id = uk.key_id "
					+"inner join user_column sc on ukc.src_column_id = sc.user_column_id "
					+"inner join user_table sut on uk.user_table_id = sut.table_id "
					+"inner join user_database sdb on sut.user_database_id = sdb.user_database_id "
					+"left outer join user_column tc on ukc.targ_column_id = tc.user_column_id "
					+"left outer join user_table tut on uk.referenced_table = tut.table_id "
					+"left outer join user_database tdb on tut.user_database_id = tdb.user_database_id "
					+"where uk.synth = 0",
					new DirectColumnGenerator("CONSTRAINT_CATALOG","varchar(512)"),
					new DirectColumnGenerator("TABLE_CATALOG","varchar(512)"),
					new DirectColumnGenerator("TABLE_SCHEMA","varchar(64)"),
					new DirectColumnGenerator("TABLE_NAME","varchar(64)"),
					new DirectColumnGenerator("COLUMN_NAME","varchar(64)"),
					new DirectColumnGenerator("ORDINAL_POSITION","bigint(10)"),
					new DirectColumnGenerator("REFERENCED_TABLE_SCHEMA","varchar(64)"),
					new DirectColumnGenerator("REFERENCED_TABLE_NAME","varchar(64)"),
					new DirectColumnGenerator("REFERENCED_COLUMN_NAME","varchar(64)")),
			new DirectTableGenerator(InfoView.INFORMATION,
					"table_constraints",null,
					"table_constraints",
					"select 'def' as `CONSTRAINT_CATALOG`, sdb.name as `CONSTRAINT_SCHEMA`, "
					+"uk.constraint_name as `CONSTRAINT_NAME`, sdb.name as `TABLE_SCHEMA`, "
					+"sut.name as `TABLE_NAME`, "
					+"case uk.constraint_type when 'PRIMARY' then 'PRIMARY KEY' when 'FOREIGN' then 'FOREIGN KEY' else uk.constraint_type END as `CONSTRAINT_TYPE` "
					+"from user_key "
					+"uk inner join user_table sut on uk.user_table_id = sut.table_id "
					+"inner join user_database sdb on sut.user_database_id = sdb.user_database_id "
					+"where uk.constraint_type is not null",
					new DirectColumnGenerator("CONSTRAINT_CATALOG","varchar(512)"),
					new DirectColumnGenerator("CONSTRAINT_SCHEMA","varchar(64)"),
					new DirectColumnGenerator("CONSTRAINT_NAME","varchar(64)"),
					new DirectColumnGenerator("TABLE_SCHEMA","varchar(64)"),
					new DirectColumnGenerator("TABLE_NAME","varchar(64)"),
					new DirectColumnGenerator("CONSTRAINT_TYPE","varchar(64)")),
			new DirectTableGenerator(InfoView.INFORMATION,
					"referential_constraints", null,
					"referential_constraints",
					"select 'def' as `CONSTRAINT_CATALOG`, sdb.name as `CONSTRAINT_SCHEMA`, uk.constraint_name as `CONSTRAINT_NAME`, "
					+"'def' as `UNIQUE_CONSTRAINT_CATALOG`, coalesce(tdb.name, uk.forward_schema_name) as `UNIQUE_CONSTRAINT_SCHEMA`, "
					+"uk.fk_update_action as `UPDATE_RULE`, uk.fk_delete_action as `DELETE_RULE`, "
					+"sut.name as `TABLE_NAME`, coalesce(tut.name, uk.forward_table_name) as `REFERENCED_TABLE_NAME` "
					+"from user_key uk "
					+"inner join user_table sut on uk.user_table_id = sut.table_id "
					+"inner join user_database sdb on sut.user_database_id = sdb.user_database_id "
					+"left outer join user_table tut on uk.referenced_table = tut.table_id "
					+"left outer join user_database tdb on tut.user_database_id = tdb.user_database_id "
					+"where uk.constraint_type = 'FOREIGN' and uk.synth = 0",
					new DirectColumnGenerator("CONSTRAINT_CATALOG","varchar(512)"),
					new DirectColumnGenerator("CONSTRAINT_SCHEMA","varchar(64)"),
					new DirectColumnGenerator("CONSTRAINT_NAME","varchar(64)"),
					new DirectColumnGenerator("UNIQUE_CONSTRAINT_CATALOG","varchar(512)"),
					new DirectColumnGenerator("UNIQUE_CONSTRAINT_SCHEMA","varchar(64)"),
					new DirectColumnGenerator("UPDATE_RULE","varchar(64)"),
					new DirectColumnGenerator("DELETE_RULE","varchar(64)"),
					new DirectColumnGenerator("TABLE_NAME","varchar(64)"),
					new DirectColumnGenerator("REFERENCED_TABLE_NAME","varchar(64)")),
			new DirectTableGenerator(InfoView.SHOW,
					"key", "keys",
					"show_keys",
					"select sut.name as `Table`, case uk.constraint_type when 'UNIQUE' then 0 when 'PRIMARY' then 0 else 1 end as `Non_unique`, "
					+"uk.name as `Key_name`, ukc.position as `Seq_in_index`, "
					+"sc.name as `Column_name`, case uk.index_type when 'FULLTEXT' then null else 'A' end as `Collation`,"
					+"ukc.cardinality as `Cardinality`, ukc.length as `Sub_part`, null as `Packed`, "
					+"case sc.nullable when 1 then 'YES' else '' end as `Null`, uk.index_type as `Index_type`, "
					+"'' as `Comment`, coalesce(uk.key_comment,'') as `Index_comment` "
					+"from user_key_column ukc "
					+"inner join user_key uk on uk.key_id = ukc.key_id "
					+"inner join user_table sut on uk.user_table_id = sut.table_id "
					+"inner join user_database sud on sut.user_database_id = sud.user_database_id "
					+"inner join user_column sc on ukc.src_column_id = sc.user_column_id "
					+"where sud.name = @scope1 and sut.name = @scope2 "
					+"and ((uk.constraint_type is null) or (uk.constraint_type != 'FOREIGN')) "
					+"order by uk.key_id, `Seq_in_index`",
					new DirectColumnGenerator("Table","varchar(64)"),
					new DirectColumnGenerator("Non_unique","int"),
					new DirectColumnGenerator("Key_name","varchar(64)"),
					new DirectColumnGenerator("Seq_in_index","int"),
					new DirectColumnGenerator("Column_name","varchar(64)"),
					new DirectColumnGenerator("Collation","varchar(64)"),
					new DirectColumnGenerator("Cardinality","int"),
					new DirectColumnGenerator("Sub_part","int"),
					new DirectColumnGenerator("Packed","varchar(64)"),
					new DirectColumnGenerator("Null","varchar(64)"),
					new DirectColumnGenerator("Index_type","varchar(64)"),
					new DirectColumnGenerator("Comment","varchar(255)"),
					new DirectColumnGenerator("Index_comment","varchar(255)")).withShowImpl(DirectShowKeysSchemaTable.class),
			new DirectTableGenerator(InfoView.SHOW,
					"persistent instance","persistent instances",
					"show_persistent_instance",
					"select pi.name as `Name`, ss.name as `Persistent_Site`, pi.instance_url as `URL`, "
					+"pi.user as `User`, pi.password as `Password`, "
					+"case pi.is_master when 1 then 'YES' else 'NO' end as `Master`, "
					+"pi.status as `Status` "
					+"from site_instance pi left outer join storage_site ss on pi.storage_site_id = ss.id ",
					new DirectColumnGenerator("Name","varchar(255)").withIdent().withOrderBy(0),
					new DirectColumnGenerator("Persistent_Site","varchar(255)"),
					new DirectColumnGenerator("URL","varchar(255)"),
					new DirectColumnGenerator("User","varchar(255)"),
					new DirectColumnGenerator("Password","varchar(255)"),
					new DirectColumnGenerator("Master","varchar(3)"),
					new DirectColumnGenerator("Status","varchar(255)")
					).withExtension().withPrivilege(),
			new DirectTableGenerator(InfoView.INFORMATION,
					"site_instance", null,
					"Site_instance",
					"select pi.name as `NAME`, ss.name as `STORAGE_SITE`, pi.instance_url as `INSTANCE_URL`, "
					+"pi.user as `USER`, pi.password as `PASSWORD`, "
					+"case pi.is_master when 1 then 'YES' else 'NO' end as `IS_MASTER`, "
					+"pi.status as `STATUS` "
					+"from site_instance pi left outer join storage_site ss on pi.storage_site_id = ss.id ",
					new DirectColumnGenerator("NAME","varchar(255)"),
					new DirectColumnGenerator("STORAGE_SITE","varchar(255)"),
					new DirectColumnGenerator("INSTANCE_URL","varchar(255)"),
					new DirectColumnGenerator("USER","varchar(255)"),
					new DirectColumnGenerator("PASSWORD","varchar(255)"),
					new DirectColumnGenerator("IS_MASTER","varchar(3)"),
					new DirectColumnGenerator("STATUS","varchar(255)")
					).withExtension().withPrivilege(),
			new DirectTableGenerator(InfoView.INFORMATION,
					"storage_site",null,
					"storage_site",
					"select ss.name as `NAME`, ss.haType as `HATYPE`, si.instance_url as `MASTERURL` "
					+"from storage_site ss left outer join site_instance si on si.storage_site_id = ss.id and si.is_master = 1 ",
					new DirectColumnGenerator("NAME","varchar(255)"),
					new DirectColumnGenerator("HATYPE","varchar(255)"),
					new DirectColumnGenerator("MASTERURL","varchar(255)")),
			new DirectTableGenerator(InfoView.SHOW,
					"persistent site","persistent sites",
					"show_persistent_site",
					"select ss.name as `Persistent_Site`, ss.haType as `HA_Type`, si.instance_url as `Master_Url` "
					+"from storage_site ss left outer join site_instance si on si.storage_site_id = ss.id and si.is_master = 1 ",
					new DirectColumnGenerator("Persistent_Site","varchar(255)").withIdent().withOrderBy(0),
					new DirectColumnGenerator("HA_Type","varchar(255)"),
					new DirectColumnGenerator("Master_Url","varchar(255)")
					).withExtension().withPrivilege(),
				
					
					
	};
	
}
