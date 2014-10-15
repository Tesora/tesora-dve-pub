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
import java.util.List;

import com.tesora.dve.common.ShowSchema;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.db.mysql.common.ColumnAttributes;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.infoschema.InfoView;
import com.tesora.dve.sql.infoschema.InformationSchemaBuilder;
import com.tesora.dve.sql.infoschema.InformationSchema;
import com.tesora.dve.sql.infoschema.MysqlSchema;
import com.tesora.dve.sql.infoschema.AbstractInformationSchema;
import com.tesora.dve.sql.infoschema.ShowOptions;
import com.tesora.dve.sql.infoschema.ShowView;
import com.tesora.dve.sql.infoschema.direct.ViewShowSchemaTable.TemporaryTableHandler;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.schema.ComplexPETable;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.VariableScope;
import com.tesora.dve.sql.transexec.TransientExecutionEngine;
import com.tesora.dve.sql.schema.VariableScopeKind;
/*
 * Holds all the sql strings that back info schema tables.
 * There are some variables you can use in the queries.  At planning time the variables
 * will be replaced with the current values.  Eventually, when info schema queries are cached,
 * they will be replaced with custom literals that will fill in with the appropriate per conn values
 * in the same manner as the tenant id literal, autoinc literals.
 * 
 * @dbn - the last 'use' value, if any
 * @tn - for table scoped show commands, the table
 * @mdex - current value of the dve_metadata_extensions variable
 * @tenant - tenant id literal
 * @sessid - current session id
 * 
 * Generally to implement a new info schema table, you can develop the query in the standard way
 * in a mysql client and then plunk it into a DirectTableGenerator following the idiom below.
 */
public class DirectSchemaBuilder implements InformationSchemaBuilder {

	private final PEDatabase catalogSchema;
	
	public DirectSchemaBuilder(PEDatabase catSchema) {
		this.catalogSchema = catSchema;
	}
	
	@Override
	public void populate(InformationSchema infoSchema, ShowView showSchema,
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
		
			new ViewTableGenerator(InfoView.INFORMATION, "generation_site", null,
					"generation_site",
					String.format(gen_sites_format,"group","version","site"),
					c("group","varchar(512)"),
					c("version","int"),
					c("site","varchar(512)"))
					.withExtension().withPrivilege(),
			new ViewTableGenerator(InfoView.SHOW, "generation site", "generation sites",
					"show_generation_site",
					String.format(gen_sites_format,ShowSchema.GenerationSite.NAME,
							ShowSchema.GenerationSite.VERSION,
							ShowSchema.GenerationSite.SITE),
					c(ShowSchema.GenerationSite.NAME,"varchar(512)").withIdent(),
					c(ShowSchema.GenerationSite.VERSION,"int"),
					c(ShowSchema.GenerationSite.SITE,"varchar(512)"))
					.withExtension().withPrivilege(),
			new ViewTableGenerator(InfoView.INFORMATION,"scopes", null,
					"scopes",
					"select ut.name as `TABLE_NAME`, ud.name as `TABLE_SCHEMA`, ut.state as `TABLE_STATE`, t.ext_tenant_id as `TENANT_NAME`, s.local_name as `SCOPE_NAME` "
					+"from user_table ut inner join user_database ud on ut.user_database_id = ud.user_database_id "
					+"left outer join scope s on s.scope_table_id = ut.table_id "
					+"left outer join tenant t on s.scope_tenant_id = t.tenant_id "
					+"where ud.multitenant_mode = 'adaptive'",
					c("TABLE_NAME","varchar(255)"),
					c("TABLE_SCHEMA","varchar(255)"),
					c("TABLE_STATE","varchar(255)"),
					c("TENANT_NAME","varchar(255)"),
					c("SCOPE_NAME","varchar(255)"))
					.withExtension().withPrivilege(),
			new ViewTableGenerator(InfoView.INFORMATION,
					"distributions",null,
					"distributions",
					"select ud.name as `DATABASE_NAME`, ut.name as `TABLE_NAME`, uc.name as `COLUMN_NAME`, "
					+"uc.hash_position as `VECTOR_POSITION`, dm.name as `MODEL_TYPE`, dr.name as `MODEL_NAME` "
					+"from user_table ut inner join user_database ud on ut.user_database_id = ud.user_database_id "
					+"inner join distribution_model dm on ut.distribution_model_id = dm.id "
					+"left outer join user_column uc on uc.user_table_id = ut.table_id and uc.hash_position > 0 "
					+"left outer join range_table_relation rtr on ut.table_id = rtr.table_id "
					+"left outer join distribution_range dr on rtr.range_id = dr.range_id",
					c("DATABASE_NAME","varchar(255)"),
					c("TABLE_NAME","varchar(255)"),
					c("COLUMN_NAME","varchar(255)"),
					c("VECTOR_POSITION","int(11)"),
					c("MODEL_TYPE","varchar(255)"),
					c("MODEL_NAME","varchar(255)"))
					.withExtension(),
			new ViewTableGenerator(InfoView.INFORMATION,
					"character_sets", null,
					"character_sets",
					"select cs.character_set_name as `CHARACTER_SET_NAME`, cs.description as `DESCRIPTION`, cs.maxlen as `MAXLEN` "
					+"from character_sets cs order by cs.character_set_name",
					c("CHARACTER_SET_NAME","varchar(32)"),
					c("DESCRIPTION","varchar(60)"),
					c("MAXLEN","int(11)")),
			new ViewTableGenerator(InfoView.SHOW,
					"charset",null,
					"show_character_sets",
					"select cs.character_set_name as `Charset`, cs.description as `Description`, cs.maxlen as `Maxlen` "
					+"from character_sets cs order by cs.character_set_name",
					c("Charset","varchar(32)").withIdent().withOrderBy(0),
					c("Description","varchar(60)"),
					c("Maxlen","int(11)")),
			new ViewTableGenerator(InfoView.INFORMATION,
					"collations", null,
					"collations",
					// ah, let the stupid mysqlisms begin
					"select c.name as `COLLATION_NAME`, c.character_set_name as `CHARACTER_SET_NAME`, "
					+"cast((c.id+1)-1 as signed integer) as `ID`, "
					+"case c.is_default when 1 then 'Yes' else '' end as `IS_DEFAULT`, "
					+"case c.is_compiled when 1 then 'Yes' else 'No' end as `IS_COMPILED`, "
					+"c.sortlen as `SORTLEN` "
					+"from collations c order by c.character_set_name, c.id",
					c("COLLATION_NAME","varchar(32)"),
					c("CHARACTER_SET_NAME","varchar(32)"),
					c("ID","bigint(11)"),
					c("IS_DEFAULT","varchar(3)"),
					c("IS_COMPILED","varchar(3)"),
					c("SORTLEN","bigint(3)")),
			new ViewTableGenerator(InfoView.SHOW,
					"collation", null,
					"show_collations",
					"select c.name as `Collation`, c.character_set_name as `Charset`, "
					+"cast((c.id+1)-1 as signed integer) as `Id`, "
					+"case c.is_default when 1 then 'Yes' else '' end as `Default`, "
					+"case c.is_compiled when 1 then 'Yes' else 'No' end as `Compiled`, "
					+"c.sortlen as `Sortlen` "
					+"from collations c order by c.character_set_name, c.id",
					c("Collation","varchar(32)").withIdent().withOrderBy(0),
					c("Charset","varchar(32)"),
					c("Id","bigint(11)").withOrderBy(1),
					c("Default","varchar(3)"),
					c("Compiled","varchar(3)"),
					c("Sortlen","bigint(3)")),
			new ViewTableGenerator(InfoView.SHOW,
					"dynamic site policy", "dynamic site policies",
					"show_dyn_site_policies",
					"select d.name as `Name`, case d.strict when 1 then '1' else '0' end as `strict`, "
					+"d.aggregate_class as `aggregate_class`, d.aggregate_count as `aggregate_count`, d.aggregate_provider as `aggregate_provider`, "
					+"d.small_class as `small_class`, d.small_count as `small_count`, d.small_provider as `small_provider`, "
					+"d.medium_class as `medium_class`, d.medium_count as `medium_count`, d.medium_provider as `medium_provider`, "
					+"d.large_class as `large_class`, d.large_count as `large_count`, d.large_provider as `large_provider` "
					+"from dynamic_policy d",
					c("Name","varchar(255)").withIdent().withOrderBy(0),
					c("strict","int(11)"),
					c("aggregate_class","varchar(255)"),
					c("aggregate_count","int(11)"),
					c("aggregate_provider","varchar(255)"),
					c("small_class","varchar(255)"),
					c("small_count","int(11)"),
					c("small_provider","varchar(255)"),
					c("medium_class","varchar(255)"),
					c("medium_count","int(11)"),
					c("medium_provider","varchar(255)"),
					c("large_class","varchar(255)"),
					c("large_count","int(11)"),
					c("large_provider","varchar(255)"))
					.withExtension(),
			new ViewTableGenerator(InfoView.INFORMATION,
					"engines",null,
					"engines",
					"select e.engine as `ENGINE`, e.support as `SUPPORT`, e.comment as `COMMENT`, e.transactions as `TRANSACTIONS`, "
					+"e.xa as `XA`, e.savepoints as `SAVEPOINTS` from engines e order by e.engine",
					c("ENGINE","varchar(64)"),
					c("SUPPORT","varchar(8)"),
					c("COMMENT","varchar(80)"),
					c("TRANSACTIONS","varchar(3)"),
					c("XA","varchar(3)"),
					c("SAVEPOINTS","varchar(3)")),
			new ViewTableGenerator(InfoView.SHOW,
					"engines",null,
					"show_engines",
					"select e.engine as `Engine`, e.support as `Support`, e.comment as `Comment`, e.transactions as `Transactions`, "
					+"e.xa as `XA`, e.savepoints as `Savepoints` from engines e order by e.engine",
					c("Engine","varchar(64)").withIdent().withOrderBy(0),
					c("Support","varchar(8)"),
					c("Comment","varchar(80)"),
					c("Transactions","varchar(3)"),
					c("XA","varchar(3)"),
					c("Savepoints","varchar(3)")),
			new ViewTableGenerator(InfoView.INFORMATION,
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
					c("CONSTRAINT_CATALOG","varchar(512)"),
					c("TABLE_CATALOG","varchar(512)"),
					c("TABLE_SCHEMA","varchar(64)"),
					c("TABLE_NAME","varchar(64)"),
					c("COLUMN_NAME","varchar(64)"),
					c("ORDINAL_POSITION","bigint(10)"),
					c("REFERENCED_TABLE_SCHEMA","varchar(64)"),
					c("REFERENCED_TABLE_NAME","varchar(64)"),
					c("REFERENCED_COLUMN_NAME","varchar(64)")),
			new ViewTableGenerator(InfoView.INFORMATION,
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
					c("CONSTRAINT_CATALOG","varchar(512)"),
					c("CONSTRAINT_SCHEMA","varchar(64)"),
					c("CONSTRAINT_NAME","varchar(64)"),
					c("TABLE_SCHEMA","varchar(64)"),
					c("TABLE_NAME","varchar(64)"),
					c("CONSTRAINT_TYPE","varchar(64)")),
			new ViewTableGenerator(InfoView.INFORMATION,
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
					c("CONSTRAINT_CATALOG","varchar(512)"),
					c("CONSTRAINT_SCHEMA","varchar(64)"),
					c("CONSTRAINT_NAME","varchar(64)"),
					c("UNIQUE_CONSTRAINT_CATALOG","varchar(512)"),
					c("UNIQUE_CONSTRAINT_SCHEMA","varchar(64)"),
					c("UPDATE_RULE","varchar(64)"),
					c("DELETE_RULE","varchar(64)"),
					c("TABLE_NAME","varchar(64)"),
					c("REFERENCED_TABLE_NAME","varchar(64)")),
			new ViewTableGenerator(InfoView.SHOW,
					"key", "keys",
					"show_keys",
					"select sut.name as `Table`, case uk.constraint_type when 'UNIQUE' then 0 when 'PRIMARY' then 0 else 1 end as `Non_unique`, "
					+"uk.name as `Key_name`, ukc.position as `Seq_in_index`, "
					+"sc.name as `Column_name`, case uk.index_type when 'FULLTEXT' then null else 'A' end as `Collation`,"
					+"ukc.cardinality as `Cardinality`, ukc.length as `Sub_part`, null as `Packed`, "
					+ ColumnAttributes.buildSQLTest("sc.flags", ColumnAttributes.NOT_NULLABLE, "''", "'YES'") + " as `Null`,"
					+"uk.index_type as `Index_type`, "
					+"'' as `Comment`, coalesce(uk.key_comment,'') as `Index_comment` "
					+"from user_key_column ukc "
					+"inner join user_key uk on uk.key_id = ukc.key_id "
					+"inner join user_table sut on uk.user_table_id = sut.table_id "
					+"inner join user_database sud on sut.user_database_id = sud.user_database_id "
					+"inner join user_column sc on ukc.src_column_id = sc.user_column_id "
					+"where sud.name = @dbn and sut.name = @tn "
					+"and ((uk.constraint_type is null) or (uk.constraint_type != 'FOREIGN')) "
					+"order by uk.key_id, `Seq_in_index`",
					c("Table","varchar(64)"),
					c("Non_unique","int"),
					c("Key_name","varchar(64)"),
					c("Seq_in_index","int"),
					c("Column_name","varchar(64)"),
					c("Collation","varchar(64)"),
					c("Cardinality","int"),
					c("Sub_part","int"),
					c("Packed","varchar(64)"),
					c("Null","varchar(64)"),
					c("Index_type","varchar(64)"),
					c("Comment","varchar(255)"),
					c("Index_comment","varchar(255)")).withTempHandler(new TemporaryTableHandler() {

						@Override
						public List<ResultRow> buildResults(SchemaContext sc,
								TableInstance matching, ShowOptions opts, String likeExpr) {
							ComplexPETable ctab = (ComplexPETable) matching.getAbstractTable();
							List<ResultRow> tempTab = ctab.getShowKeys(sc);
							return tempTab;
						}
						
					}),
					
			new ViewTableGenerator(InfoView.SHOW,
					"persistent instance","persistent instances",
					"show_persistent_instance",
					"select pi.name as `Name`, ss.name as `Persistent_Site`, pi.instance_url as `URL`, "
					+"pi.user as `User`, pi.password as `Password`, "
					+"case pi.is_master when 1 then 'YES' else 'NO' end as `Master`, "
					+"pi.status as `Status` "
					+"from site_instance pi left outer join storage_site ss on pi.storage_site_id = ss.id ",
					c("Name","varchar(255)").withIdent().withOrderBy(0),
					c("Persistent_Site","varchar(255)"),
					c("URL","varchar(255)"),
					c("User","varchar(255)"),
					c("Password","varchar(255)"),
					c("Master","varchar(3)"),
					c("Status","varchar(255)")
					).withExtension().withPrivilege(),
			new ViewTableGenerator(InfoView.INFORMATION,
					"site_instance", null,
					"Site_instance",
					"select pi.name as `NAME`, ss.name as `STORAGE_SITE`, pi.instance_url as `INSTANCE_URL`, "
					+"pi.user as `USER`, pi.password as `PASSWORD`, "
					+"case pi.is_master when 1 then 'YES' else 'NO' end as `IS_MASTER`, "
					+"pi.status as `STATUS` "
					+"from site_instance pi left outer join storage_site ss on pi.storage_site_id = ss.id ",
					c("NAME","varchar(255)"),
					c("STORAGE_SITE","varchar(255)"),
					c("INSTANCE_URL","varchar(255)"),
					c("USER","varchar(255)"),
					c("PASSWORD","varchar(255)"),
					c("IS_MASTER","varchar(3)"),
					c("STATUS","varchar(255)")
					).withExtension().withPrivilege(),
			new ViewTableGenerator(InfoView.INFORMATION,
					"storage_site",null,
					"storage_site",
					"select ss.name as `NAME`, ss.haType as `HATYPE`, si.instance_url as `MASTERURL` "
					+"from storage_site ss left outer join site_instance si on si.storage_site_id = ss.id and si.is_master = 1 ",
					c("NAME","varchar(255)"),
					c("HATYPE","varchar(255)"),
					c("MASTERURL","varchar(255)")),
			new ViewTableGenerator(InfoView.SHOW,
					"persistent site","persistent sites",
					"show_persistent_site",
					"select ss.name as `Persistent_Site`, ss.haType as `HA_Type`, si.instance_url as `Master_Url` "
					+"from storage_site ss left outer join site_instance si on si.storage_site_id = ss.id and si.is_master = 1 ",
					c("Persistent_Site","varchar(255)").withIdent().withOrderBy(0),
					c("HA_Type","varchar(255)"),
					c("Master_Url","varchar(255)")
					).withExtension().withPrivilege(),
				
			new ViewTableGenerator(InfoView.INFORMATION,
					"columns",null,
					"columns",
					"select 'def' as `TABLE_CATALOG`, ud.name as `TABLE_SCHEMA`, ut.name as `TABLE_NAME`, "
					+"uc.name as `COLUMN_NAME`, uc.order_in_table as `ORDINAL_POSITION`, "
					+"uc.default_value as `COLUMN_DEFAULT`, "
					+ ColumnAttributes.buildSQLTest("uc.flags", ColumnAttributes.NOT_NULLABLE, "'NO'", "'YES'") + " as `IS_NULLABLE`, "
					+"uc.native_type_name as `DATA_TYPE`, "
					+"uc.size as `CHARACTER_MAXIMUM_LENGTH`, uc.prec as `NUMERIC_PRECISION`, uc.scale as `NUMERIC_SCALE`, "
					+"uc.charset as `CHARACTER_SET_NAME`, uc.collation as `COLLATION_NAME`, "
					+buildColumnFullTypeName("uc") + " as `COLUMN_TYPE`, "
					+buildColumnKey("uc") + " as `COLUMN_KEY`, "
					+ buildColumnExtra("uc") + " as `EXTRA`, "
					+"'' as `PRIVILEGES`, uc.comment as `COLUMN_COMMENT` "
					+"from user_column uc inner join user_table ut on uc.user_table_id = ut.table_id "
					+"inner join user_database ud on ut.user_database_id = ud.user_database_id "
					+"order by uc.order_in_table",
					c("TABLE_CATALOG","varchar(512)"),
					c("TABLE_SCHEMA","varchar(64)"),
					c("TABLE_NAME","varchar(64)"),
					c("COLUMN_NAME","varchar(64)"),
					c("ORDINAL_POSITION","bigint(21) unsigned"),
					c("COLUMN_DEFAULT","longtext"),
					c("IS_NULLABLE","varchar(3)"),
					c("DATA_TYPE","varchar(64)"),
					c("CHARACTER_MAXIMUM_LENGTH","bigint(21) unsigned"),
					c("NUMERIC_PRECISION","bigint(21) unsigned"),
					c("NUMERIC_SCALE","bigint(21) unsigned"),
					c("CHARACTER_SET_NAME","varchar(32)"),
					c("COLLATION_NAME","varchar(32)"),
					c("COLUMN_TYPE","longtext"),
					c("COLUMN_KEY","varchar(3)"),
					c("EXTRA","varchar(27)"),
					c("PRIVILEGES","varchar(80)"),
					c("COLUMN_COMMENT","varchar(1024)")),

			// mt modes:
			// if @tenant not null:
			//    if scope exists, do not show ___mtid
			//    if container_tenant exists, do not show ___mtid
			//    if @tenant == -1, do not show ___mtid (global container)
			// else
			//    show ___mtid
			// match: coalesce(t.ext_ten_id,ud.name) = @dbn
			// match: coalesce(s.local_name,ut.name) = @tn
			new ViewTableGenerator(InfoView.SHOW,
					"column","columns",
					"show_columns",
					"select uc.name as `Field`, "
					+ buildColumnFullTypeName("uc") + " as `Type`, "
					+"uc.collation as `Collation`, "
					+ ColumnAttributes.buildSQLTest("uc.flags", ColumnAttributes.NOT_NULLABLE, "'NO'", "'YES'") + " as `Null`, "
					+ buildColumnKey("uc") + " as `Key`, "
					+"uc.default_value as `Default`, "
					+ buildColumnExtra("uc") + " as `Extra`, "
					+"'' as `Privileges`, ifnull(uc.comment,'') as `Comment` "
					+"from user_column uc inner join user_table ut on uc.user_table_id = ut.table_id "
					+"inner join user_database ud on ut.user_database_id = ud.user_database_id "
					+"left outer join scope s on ut.table_id = s.scope_table_id and s.scope_tenant_id = @tenant "
					+"left outer join tenant t on t.tenant_id = @tenant "
					+"left outer join container c on ut.container_id = c.container_id "
					+"left outer join container_tenant ct on ct.container_id = c.container_id and ct.ctid = @tenant "
					+"where (coalesce(t.ext_tenant_id,ud.name) = @dbn and coalesce(s.local_name,ut.name) = @tn) "
					+"and (uc.name != '___mtid' or not(s.local_name is not null or ct.ctid is not null or @tenant = -1) or @mdex is not null) " 
					+"order by uc.order_in_table",
					c("Field","varchar(64)").withIdent(),
					c("Type","varchar(64)"),
					c("Collation","varchar(32)").withFull(),
					c("Null","varchar(3)"),
					c("Key","varchar(3)"),
					c("Default","longtext"),
					c("Extra","varchar(27)"),
					c("Privileges","varchar(80)").withFull(),
					c("Comment","varchar(1024)").withFull()).withTempHandler(new TemporaryTableHandler() {

						@Override
						public List<ResultRow> buildResults(SchemaContext sc,
								TableInstance matching, ShowOptions opts, String likeExpr) {
							ComplexPETable ctab = (ComplexPETable) matching.getAbstractTable();
							List<ResultRow> tempTab = ctab.getShowColumns(sc,likeExpr);
							return tempTab;
						}
						
					}),
					new ViewTableGenerator(InfoView.SHOW,
				"persistent group","persistent groups",
				"show_persistent_group",
				"select pg.name as `Name`, max(sg.generation_id) as `Latest_generation` "
				+"from persistent_group pg inner join storage_generation sg on pg.persistent_group_id = sg.persistent_group_id "
				+"group by pg.Name order by pg.Name",
				c("Name","varchar(255)").withIdent(),
				c("Latest_generation","bigint"))
			.withExtension().withPrivilege(),
			new ViewTableGenerator(InfoView.INFORMATION,
				"storage_group",null,
				"storage_group",
				"select pg.name as `NAME`, max(sg.generation_id) as `LAST_GENERATION` "
				+"from persistent_group pg inner join storage_generation sg on pg.persistent_group_id = sg.persistent_group_id "
				+"group by pg.Name order by pg.Name",
				c("NAME","varchar(255)"),
				c("LAST_GENERATION","bigint"))
				.withExtension().withPrivilege(),
			new ViewTableGenerator(InfoView.SHOW,
					"range","ranges",
					"show_range",
					"select dr.name as `Range`, pg.name as `Persistent_Group`, dr.signature as `Signature` "
					+"from distribution_range dr inner join persistent_group pg on dr.persistent_group_id = pg.persistent_group_id "
					+"order by dr.name ",
					c("Range","varchar(255)").withIdent(),
					c("Persistent_Group","varchar(255)"),
					c("Signature","varchar(255)"))
					.withExtension().withPrivilege(),
			new ViewTableGenerator(InfoView.INFORMATION,
					"range_distribution",null,
					"range_distribution",
					"select dr.name as `NAME`, pg.name as `STORAGE_GROUP`, dr.signature as `SIGNATURE` "
					+"from distribution_range dr inner join persistent_group pg on dr.persistent_group_id = pg.persistent_group_id "
					+"order by dr.name ",
					c("NAME","varchar(255)"),
					c("STORAGE_GROUP","varchar(255)"),
					c("SIGNATURE","varchar(255)"))
					.withExtension().withPrivilege(),
			new ViewTableGenerator(InfoView.SHOW,
					"template","templates",
					"show_template",
					"select t.name as `Template_Name`, t.dbmatch as `DB_Match`, t.template_comment as `Comment`, t.definition as `Template` "
					+"from template t order by t.name",
					c("Template_Name","varchar(255)").withIdent(),
					c("DB_Match","varchar(255)"),
					c("Comment","varchar(255)"),
					c("Template","longtext")).withExtension(),
			new ViewTableGenerator(InfoView.INFORMATION,
					"templates",null,
					"templates",
					"select t.name as `NAME`, t.dbmatch as `DBMATCH`, t.definition as `DEFINITION`, t.template_comment as `TEMPLATE_COMMENT` "
					+"from template t order by t.name",
					c("NAME","varchar(255)"),
					c("DBMATCH","varchar(255)"),
					c("DEFINITION","longtext"),
					c("TEMPLATE_COMMENT","varchar(255)")).withExtension(),
			new ViewTableGenerator(InfoView.SHOW,
					"rawplan","rawplans",
					"show_rawplan",
					"select r.name as `Plan_Name`, "
					+"case r.enabled when 1 then 'YES' else 'NO' end as `Enabled`, "
					+"r.cachekey as `Cache_Key`, r.plan_comment as `Comment`, r.definition as `Plan` "
					+"from rawplan r order by r.name ",
					c("Plan_Name","varchar(255)").withIdent(),
					c("Enabled","varchar(3)"),
					c("Cache_Key","longtext"),
					c("Comment","varchar(255)"),
					c("Plan","longtext")).withExtension(),
			new ViewTableGenerator(InfoView.INFORMATION,
					"rawplans",null,
					"rawplans",
					"select r.name as `NAME`, ud.name as `PLAN_SCHEMA`, "
					+"case r.enabled when 1 then 'YES' else 'NO' end as `IS_ENABLED`, "
					+"r.cachekey as `CACHE_KEY`, r.plan_comment as `PLAN_COMMENT`, "
					+"r.definition as `DEFINITION` "
					+"from rawplan r inner join user_database ud on r.user_database_id = ud.user_database_id "
					+"order by r.name ",
					c("NAME","varchar(255)").withIdent(),
					c("PLAN_SCHEMA","varchar(255)"),
					c("IS_ENABLED","varchar(3)"),
					c("CACHE_KEY","longtext"),
					c("PLAN_COMMENT","varchar(255)"),
					c("DEFINITION","longtext")).withExtension(),
			new ViewTableGenerator(InfoView.SHOW,
					"container_tenant","container_tenants",
					"show_container_tenant",
					"select c.name as `Container`, ct.discriminant as `Discriminant`, ct.ctid as `ID` "
					+"from container_tenant ct inner join container c on ct.container_id = c.container_id "
					+"order by c.name, ct.ctid ",
					c("Container","varchar(255)"),
					c("Discriminant","longtext").withIdent(),
					c("ID","int(11)")).withExtension().withPrivilege(),
			new ViewTableGenerator(InfoView.SHOW,
					"containers",null,
					"show_containers",
					"select c.name as `Container`, ut.name as `Base_Table`, pg.name as `Persistent_Group` "
					+"from container c left outer join user_table ut on c.base_table_id = ut.table_id "
					+"inner join persistent_group pg on c.storage_group_id = pg.persistent_group_id "
					+"order by c.name ",
					c("Container","varchar(255)").withIdent(),
					c("Base_Table","varchar(255)"),
					c("Persistent_Group","varchar(255)")).withExtension(),
			new ViewTableGenerator(InfoView.SHOW,
					"container",null,
					"show_container",
					"select c.name as `Container`, ut.name as `Table`, "
					+"case ut.table_id when c.base_table_id then 'base' else 'member' end as `Type` "
					+"from container c inner join user_table ut on c.container_id = ut.container_id " 
					+"order by ut.name",
					c("Container","varchar(255)").withIdent(),
					c("Table","varchar(255)"),
					c("Type","varchar(6)")).withExtension(),
			new ViewTableGenerator(InfoView.INFORMATION,
					"container",null,
					"container",
					"select c.name as `CONTAINER_NAME`, ut.name as `BASE_TABLE`, pg.name as `STORAGE_GROUP` "
					+"from container c left outer join user_table ut on c.base_table_id = ut.table_id "
					+"inner join persistent_group pg on c.storage_group_id = pg.persistent_group_id "
					+"order by c.name ",
					c("CONTAINER_NAME","varchar(255) NOT NULL"),
					c("BASE_TABLE","varchar(255)"),
					c("STORAGE_GROUP","varchar(255) NOT NULL")).withExtension(),
			new ViewTableGenerator(InfoView.SHOW,
					"events",null,
					"show_events",
					null,
					c("Db","varchar(64)"),
					c("Name","varchar(64)").withIdent(),
					c("Time zone","varchar(64)"),
					c("Type","varchar(9)"),
					c("Execute at","datetime"),
					c("Interval value","varchar(256)"),
					c("Starts","datetime"),
					c("Ends","datetime"),
					c("Status","varchar(18)"),
					c("Originator","bigint(10)"),
					c("character_set_client","varchar(32)"),
					c("collation_connection","varchar(32)"),
					c("Database Collation","varchar(32)")),
			new ViewTableGenerator(InfoView.INFORMATION,
					"events",null,
					"events",
					null,
					c("EVENT_CATALOG","varchar(64)"),
					c("EVENT_SCHEMA","varchar(64)"),
					c("EVENT_NAME","varchar(64)"),
					c("DEFINER","varchar(77)"),
					c("TIME_ZONE","varchar(64)"),
					c("EVENT_BODY","varchar(8)"),
					c("EVENT_DEFINITION","longtext"),
					c("EVENT_TYPE","varchar(9)"),
					c("EXECUTE_AT","datetime"),
					c("INTERVAL_VALUE","varchar(256)"),
					c("INTERVAL_FIELD","varchar(18)"),
					c("SQL_MODE","varchar(8192)"),
					c("STARTS","datetime"),
					c("ENDS","datetime"),
					c("STATUS","varchar(18)"),
					c("ON_COMPLETION","varchar(12)"),
					c("CREATED","datetime"),
					c("LAST_ALTERED","datetime"),
					c("LAST_EXECUTED","datetime"),
					c("EVENT_COMMENT","varchar(64)"),
					c("ORIGINATOR","bigint(10)"),
					c("CHARACTER_SET_CLIENT","varchar(32)"),
					c("COLLATION_CONNECTION","varchar(32)"),
					c("DATABASE_COLLATION","varchar(32)")),
					
			new ViewTableGenerator(InfoView.SHOW,
					"table","tables",
					"show_table",
					"select coalesce(s.local_name,if(@tenant is null,ut.name,null)) as `Tables`, ut.table_type as `Table_type`, "
					+"d.name as `Distribution_Model`, pg.name as `Persistent_group` "
					+"from user_table ut inner join distribution_model d on ut.distribution_model_id = d.id "
					+"inner join persistent_group pg on ut.persistent_group_id = pg.persistent_group_id "
					+"inner join user_database ud on ut.user_database_id = ud.user_database_id "
					+"left join scope s on s.scope_table_id = ut.table_id and s.scope_tenant_id = @tenant "
					+"left join tenant t on t.tenant_id = @tenant "
					+"where coalesce(t.ext_tenant_id,ud.name) = @dbn and coalesce(s.local_name,if(@tenant is null, ut.name, null)) is not null "
					+"order by `Tables`",
					c("Tables","varchar(255)").withIdent(),
					c("Table_type","varchar(64)").withFull(),
					c("Distribution_Model","varchar(255)").withExtension(),
					c("Persistent_group","varchar(255)").withExtension()),
			new ViewTableGenerator(InfoView.INFORMATION,
					"tables",null,
					"tables",
					"select ud.name as `TABLE_SCHEMA`, ut.name as `TABLE_NAME`, ut.table_type as `TABLE_TYPE`, "
					+"ut.engine as `ENGINE`, pg.name as `STORAGE_GROUP`, ut.row_format as `ROW_FORMAT`, "
					+"ut.collation as `TABLE_COLLATION`, ut.create_options as `CREATE_OPTIONS`, "
					+"ut.comment as `TABLE_COMMENT` "
					+"from user_table ut inner join persistent_group pg on ut.persistent_group_id = pg.persistent_group_id "
					+"inner join user_database ud on ud.user_database_id = ut.user_database_id "
					+"order by ud.name, ut.name ",
					c("TABLE_SCHEMA","varchar(64)"),
					c("TABLE_NAME","varchar(64)"),
					c("TABLE_TYPE","varchar(64)"),
					c("ENGINE","varchar(64)"),
					c("STORAGE_GROUP","varchar(255)"),
					c("ROW_FORMAT","varchar(10)"),
					c("TABLE_COLLATION","varchar(32)"),
					c("CREATE_OPTIONS","varchar(255)"),
					c("TABLE_COMMENT","varchar(2048)")),
			new ViewTableGenerator(InfoView.INFORMATION,
					"views",null,
					"views",
					"select 'def' as `TABLE_CATALOG`, ud.name as `TABLE_SCHEMA`, ut.name as `TABLE_NAME`, "
					+"v.definition as `VIEW_DEFINITION`, v.check_option as `CHECK_OPTION`, "
					+"'NO' as `IS_UPDATABLE`, " + buildDefiner("u") + " as `DEFINER`, v.security as `SECURITY_TYPE`, "
					+"v.character_set_client as `CHARACTER_SET_CLIENT`, v.collation_connection as `COLLATION_CONNECTION`, "
					+"v.mode as `MODE` "
					+"from user_view v inner join user_table ut on v.table_id = ut.table_id "
					+"inner join user_database ud on ut.user_database_id = ud.user_database_id "
					+"inner join user u on v.user_id = u.id ",
					c("TABLE_CATALOG","varchar(512)"),
					c("TABLE_SCHEMA","varchar(64)"),
					c("TABLE_NAME","varchar(64)"),
					c("VIEW_DEFINITION","longtext"),
					c("CHECK_OPTION","varchar(8)"),
					c("IS_UPDATABLE","varchar(3)"),
					c("DEFINER","varchar(77)"),
					c("SECURITY_TYPE","varchar(7)"),
					c("CHARACTER_SET_CLIENT","varchar(32)"),
					c("COLLATION_CONNECTION","varchar(32)"),
					c("MODE","varchar(7)")),
			new ViewTableGenerator(InfoView.INFORMATION,
					"plugins",null,
					"plugins",
					null,
					c("PLUGIN_NAME","varchar(64)"),
					c("PLUGIN_VERSION","varchar(20)"),
					c("PLUGIN_STATUS","varchar(10)"),
					c("PLUGIN_TYPE","varchar(80)"),
					c("PLUGIN_TYPE_VERSION","varchar(20)"),
					c("PLUGIN_LIBRARY","varchar(64)"),
					c("PLUGIN_LIBRARY_VERSION","varchar(20)"),
					c("PLUGIN_AUTHOR","varchar(64)"),
					c("PLUGIN_DESCRIPTION","longtext"),
					c("PLUGIN_LICENSE","varchar(80)"),
					c("LOAD_OPTION","varchar(64)")),
			new ViewTableGenerator(InfoView.SHOW,
					"plugins",null,
					"show_plugins",
					null,
					c("Name","varchar(64)").withIdent(),
					c("Status","varchar(10)"),
					c("Type","varchar(80)"),
					c("Library","varchar(64)"),
					c("License","varchar(80)")),
			new ViewTableGenerator(InfoView.INFORMATION,
					"files",null,
					"files",
					null,
					c("FILE_ID",             "bigint(4)"),
					c("FILE_NAME",           "varchar(64)"),
					c("FILE_TYPE",           "varchar(20)"),
					c("TABLESPACE_NAME",     "varchar(64)"),
					c("TABLE_CATALOG",       "varchar(64)"),
					c("TABLE_SCHEMA",        "varchar(64)"),
					c("TABLE_NAME",          "varchar(64)"),
					c("LOGFILE_GROUP_NAME",  "varchar(64)"),
					c("LOGFILE_GROUP_NUMBER","bigint(4)"),
					c("ENGINE",              "varchar(64)"),
					c("FULLTEXT_KEYS",       "varchar(64)"),
					c("DELETED_ROWS",        "bigint(4)"),
					c("UPDATE_COUNT",        "bigint(4)"),
					c("FREE_EXTENTS",        "bigint(4)"),
					c("TOTAL_EXTENTS",       "bigint(4)"),
					c("EXTENT_SIZE",         "bigint(4)"),
					c("INITIAL_SIZE",        "bigint(21)"),
					c("MAXIMUM_SIZE",        "bigint(21)"),
					c("AUTOEXTEND_SIZE",     "bigint(21)"),
					c("CREATION_TIME",       "datetime"),
					c("LAST_UPDATE_TIME",    "datetime"),
					c("LAST_ACCESS_TIME",    "datetime"),
					c("RECOVER_TIME",        "bigint(4)"),
					c("TRANSACTION_COUNTER", "bigint(4)"),
					c("VERSION",             "bigint(21)"),
					c("ROW_FORMAT",          "varchar(10)"),
					c("TABLE_ROWS",          "bigint(21)"),
					c("AVG_ROW_LENGTH",      "bigint(21)"),
					c("DATA_LENGTH",         "bigint(21)"),
					c("MAX_DATA_LENGTH",     "bigint(21)"),
					c("INDEX_LENGTH",        "bigint(21)"),
					c("DATA_FREE",           "bigint(21)"),
					c("CREATE_TIME",         "datetime"),
					c("UPDATE_TIME",         "datetime"),
					c("CHECK_TIME",          "datetime"),
					c("CHECKSUM",            "bigint(21)"),
					c("STATUS",              "varchar(20)"),
					c("EXTRA",               "varchar(255)")),
			new ViewTableGenerator(InfoView.INFORMATION,
					"partitions",null,
					"partitions",
					null,
					c("TABLE_CATALOG",                "varchar(512)"),
					c("TABLE_SCHEMA",                 "varchar(64)"),
					c("TABLE_NAME",                   "varchar(64)"),
					c("PARTITION_NAME",               "varchar(64)"),
					c("SUBPARTITION_NAME",            "varchar(64)"),
					c("PARTITION_ORDINAL_POSITION",   "bigint(21)"),
					c("SUBPARTITION_ORDINAL_POSITION","bigint(21)"),
					c("PARTITION_METHOD",             "varchar(18)"),
					c("SUBPARTITION_METHOD",          "varchar(12)"),
					c("PARTITION_EXPRESSION",         "longtext"),
					c("SUBPARTITION_EXPRESSION",      "longtext"),
					c("PARTITION_DESCRIPTION",        "longtext"),
					c("TABLE_ROWS",                   "bigint(21)"),
					c("AVG_ROW_LENGTH",               "bigint(21)"),
					c("DATA_LENGTH",                  "bigint(21)"),
					c("MAX_DATA_LENGTH",              "bigint(21)"),
					c("INDEX_LENGTH",                 "bigint(21)"),
					c("DATA_FREE",                    "bigint(21)"),
					c("CREATE_TIME",                  "datetime"),
					c("UPDATE_TIME",                  "datetime"),
					c("CHECK_TIME",                   "datetime"),
					c("CHECKSUM",                     "bigint(21)"),
					c("PARTITION_COMMENT",            "varchar(80)"),
					c("NODEGROUP",                    "varchar(12)"),
					c("TABLESPACE_NAME",              "varchar(64)")),
			new ViewTableGenerator(InfoView.INFORMATION,
					"routines",null,
					"routines",
					null,
					c("SPECIFIC_NAME",           "varchar(64)"),
					c("ROUTINE_CATALOG",         "varchar(512)"),
					c("ROUTINE_SCHEMA",          "varchar(64)"),
					c("ROUTINE_NAME",            "varchar(64)"),
					c("ROUTINE_TYPE",            "varchar(9)"),
					c("DATA_TYPE",               "varchar(64)"),
					c("CHARACTER_MAXIMUM_LENGTH","int(21)"),
					c("CHARACTER_OCTET_LENGTH",  "int(21)"),
					c("NUMERIC_PRECISION",       "int(21)"),
					c("NUMERIC_SCALE",           "int(21)"),
					c("CHARACTER_SET_NAME",      "varchar(64)"),
					c("COLLATION_NAME",          "varchar(64)"),
					c("DTD_IDENTIFIER",          "longtext"),
					c("ROUTINE_BODY",            "varchar(8)"),
					c("ROUTINE_DEFINITION",      "longtext"),
					c("EXTERNAL_NAME",           "varchar(64)"),
					c("EXTERNAL_LANGUAGE",       "varchar(64)"),
					c("PARAMETER_STYLE",         "varchar(8)"),
					c("IS_DETERMINISTIC",        "varchar(3)"),
					c("SQL_DATA_ACCESS",         "varchar(64)"),
					c("SQL_PATH",                "varchar(64)"),
					c("SECURITY_TYPE",           "varchar(7)"),
					c("CREATED",                 "datetime"),
					c("LAST_ALTERED",            "datetime"),
					c("SQL_MODE",                "varchar(8192)"),
					c("ROUTINE_COMMENT",         "longtext"),
					c("DEFINER",                 "varchar(77)"),
					c("CHARACTER_SET_CLIENT",    "varchar(32)"),
					c("COLLATION_CONNECTION",    "varchar(32)"),
					c("DATABASE_COLLATION",      "varchar(32)")),
			new ViewTableGenerator(InfoView.INFORMATION,
					"triggers",null,"triggers",null,
					c("TRIGGER_CATALOG",           "varchar(512)"),
					c("TRIGGER_SCHEMA",            "varchar(64)"),
					c("TRIGGER_NAME",              "varchar(64)"),
					c("EVENT_MANIPULATION",        "varchar(6)"),
					c("EVENT_OBJECT_CATALOG",      "varchar(512)"),
					c("EVENT_OBJECT_SCHEMA",       "varchar(64)"),
					c("EVENT_OBJECT_TABLE",        "varchar(64)"),
					c("ACTION_ORDER",              "bigint(4)"),
					c("ACTION_CONDITION",          "longtext"),
					c("ACTION_STATEMENT",          "longtext"),
					c("ACTION_ORIENTATION",        "varchar(9)"),
					c("ACTION_TIMING",             "varchar(6)"),
					c("ACTION_REFERENCE_OLD_TABLE","varchar(64)"),
					c("ACTION_REFERENCE_NEW_TABLE","varchar(64)"),
					c("ACTION_REFERENCE_OLD_ROW",  "varchar(3)"),
					c("ACTION_REFERENCE_NEW_ROW",  "varchar(3)"),
					c("CREATED",                   "datetime"),
					c("SQL_MODE",                  "varchar(8192)"),
					c("DEFINER",                   "varchar(77)"),
					c("CHARACTER_SET_CLIENT",      "varchar(32)"),
					c("COLLATION_CONNECTION",      "varchar(32)"),
					c("DATABASE_COLLATION",        "varchar(32)")),
			new ViewTableGenerator(InfoView.SHOW,
					"trigger","triggers","triggers",null,
					c("Trigger","varchar(64)").withIdent(),
					c("Event","varchar(6)"),
					c("Table","varchar(64)"),
					c("Statement","longtext"),
					c("Timing","varchar(6)"),
					c("Created","datetime"),
					c("sql_mode","varchar(8192)"),
					c("Definer","varchar(77)"),
					c("character_set_client","varchar(32)"),
					c("collation_connection","varchar(32)"),
					c("Database Collation","varchar(32)")),
			new ViewTableGenerator(InfoView.SHOW,
					"procedure status",null,
					"procedure_status",null,
					c("Db","varchar(64)"),
					c("Name","varchar(255)").withIdent(),
					c("Type","varchar(10)"),
					c("Definer","varchar(77)"),
					c("Modified","datetime"),
					c("Created","datetime"),
					c("Security_type","varchar(7)"),
					c("Comment","longtext"),
					c("character_set_client","varchar(32)"),
					c("collation_connection","varchar(32)"),
					c("Database Collation","varchar(32)")),
			new ViewTableGenerator(InfoView.SHOW,
					"function status",null,
					"function_status",null,
					c("Db","varchar(64)"),
					c("Name","varchar(255)").withIdent(),
					c("Type","varchar(10)"),
					c("Definer","varchar(77)"),
					c("Modified","datetime"),
					c("Created","datetime"),
					c("Security_type","varchar(7)"),
					c("Comment","longtext"),
					c("character_set_client","varchar(32)"),
					c("collation_connection","varchar(32)"),
					c("Database Collation","varchar(32)")),
			new ViewTableGenerator(InfoView.SHOW,
					"generation","generations",
					"show_storage_generation",
					"select sg.generation_id as `id`, pg.name as `Persistent_Group`, sg.version as `Version`, "
					+"case sg.locked when 1 then 'YES' else 'NO' end as `Locked` "
					+"from storage_generation sg inner join persistent_group pg on sg.persistent_group_id = pg.persistent_group_id "
					+"order by `id`",
					c("id","int(11)").withIdent(),
					c("Persistent_Group","varchar(255)"),
					c("Version","int(11)"),
					c("Locked","varchar(3)")).withPrivilege().withExtension(),
			new ViewTableGenerator(InfoView.INFORMATION,
					"storage_generation",null,
					"storage_generation",
					"select sg.generation_id as `ID`, pg.name as `STORAGE_GROUP`, sg.version as `VERSION`, "
					+"case sg.locked when 1 then 'YES' else 'NO' end as `LOCKED` "
					+"from storage_generation sg inner join persistent_group pg on sg.persistent_group_id = pg.persistent_group_id "
					+"order by `ID`",
					c("ID","int(11)"),
					c("STORAGE_GROUP","varchar(255)"),
					c("VERSION","int(11)"),
					c("LOCKED","varchar(3)")).withPrivilege().withExtension(),
			new ViewTableGenerator(InfoView.SHOW,
					"model","models",
					"show_models",
					"select dm.name as `Model` from distribution_model dm order by dm.name",
					c("Model","varchar(255)").withIdent()).withExtension(),
			new ViewTableGenerator(InfoView.INFORMATION,
					"distribution_model",null,
					"distribution_model",
					"select dm.name as `NAME` from distribution_model dm order by dm.name",
					c("NAME","varchar(255)")).withExtension(),
			new ViewTableGenerator(InfoView.INFORMATION,
					"schemata",null,
					"schemata",
					"select 'def'as `CATALOG_NAME`, ud.name as `SCHEMA_NAME`, pg.name as `DEFAULT_PERSISTENT_GROUP`, "
					+"ud.template as `TEMPLATE`, ud.template_mode as `TEMPLATE_MODE`, ud.multitenant_mode as `MULTITENANT`, "
					+"ud.fk_mode as `FKMODE`, ud.default_character_set_name as `DEFAULT_CHARACTER_SET_NAME`, "
					+"ud.default_collation_name as `DEFAULT_COLLATION_NAME` "
					+"from user_database ud left outer join persistent_group pg on ud.default_group_id = pg.persistent_group_id "
					+"order by `SCHEMA_NAME`",
					c("CATALOG_NAME","varchar(64)"),
					c("SCHEMA_NAME","varchar(255)"),
					c("DEFAULT_PERSISTENT_GROUP","varchar(255)"),
					c("TEMPLATE","varchar(255)"),
					c("TEMPLATE_MODE","varchar(255)"),
					c("MULTITENANT","varchar(255)"),
					c("FKMODE","varchar(255)"),
					c("DEFAULT_CHARACTER_SET_NAME","varchar(255)"),
					c("DEFAULT_COLLATION_NAME","varchar(255)")),
			new ViewTableGenerator(InfoView.SHOW,
					"database","databases",
					"show_databases",
					"select coalesce(t.ext_tenant_id,ud.name) as `Database`, pg.name as `Default_Persistent_Group`, ud.template as `Template`, "
					+"ud.template_mode as `Template_Mode`, ud.multitenant_mode as `Multitenant_Mode`, ud.fk_mode as `Foreign_Key_Mode`, "
					+"ud.default_character_set_name as `Default_Character_Set_Name`, ud.default_collation_name as `Default_Collation_Name` "
					+"from user_database ud left outer join tenant t on (ud.multitenant_mode != 'off' and t.user_database_id = ud.user_database_id) "
					+"left outer join persistent_group pg on ud.default_group_id = pg.persistent_group_id "
					+"order by `Database` ",
					c("Database","varchar(255)").withIdent(),
					c("Default_Persistent_Group","varchar(255)").withExtension(),
					c("Template","varchar(255)").withExtension(),
					c("Template_Mode","varchar(255)").withExtension(),
					c("Multitenant_Mode","varchar(255)").withExtension(),
					c("Foreign_Key_Mode","varchar(255)").withExtension(),
					c("Default_Character_Set_Name","varchar(255)").withExtension(),
					c("Default_Collation_Name","varchar(255)").withExtension()),
			new ViewTableGenerator(InfoView.SHOW,
					"multitenant database","multitenant databases",
					"show_mt_databases",
					"select ud.name as `Database`, pg.name as `Default_Persistent_Group`, ud.template as `Template`, "
					+"ud.template_mode as `Template_Mode`, ud.multitenant_mode as `Multitenant_Mode`, ud.fk_mode as `Foreign_Key_Mode`, "
					+"ud.default_character_set_name as `Default_Character_Set_Name`, ud.default_collation_name as `Default_Collation_Name` "
					+"from user_database ud left outer join persistent_group pg on ud.default_group_id = pg.persistent_group_id "
					+"where ud.multitenant_mode != 'off' "
					+"order by `Database` ",
					c("Database","varchar(255)").withIdent(),
					c("Default_Persistent_Group","varchar(255)").withExtension(),
					c("Template","varchar(255)").withExtension(),
					c("Template_Mode","varchar(255)").withExtension(),
					c("Multitenant_Mode","varchar(255)").withExtension(),
					c("Foreign_Key_Mode","varchar(255)").withExtension(),
					c("Default_Character_Set_Name","varchar(255)").withExtension(),
					c("Default_Collation_Name","varchar(255)").withExtension()),
			new ViewTableGenerator(InfoView.SHOW,
					"template on database","template on databases",
					"show_template_on_database",
					"select ud.name as `Database`, ud.template as `Template`, ud.template_mode as `Template_Mode` "
					+"from user_database ud order by ud.name",
					c("Database","varchar(255)").withIdent(),
					c("Template","varchar(255)"),
					c("Template_Mode","varchar(255)")).withExtension(),
					
			new ViewTableGenerator(InfoView.SHOW,
					"tenant","tenants",
					"show_tenants",
					"select t.ext_tenant_id as `Tenant`, ud.name as `Database`, "
					+"case t.suspended when 1 then 'YES' else 'NO' end as `Suspended`, "
					+"t.description as `Description` "
					+"from tenant t inner join user_database ud on t.user_database_id = ud.user_database_id "
					+"order by t.ext_tenant_id ",
					c("Tenant","varchar(255)").withIdent(),
					c("Database","varchar(255)"),
					c("Suspended","varchar(3)"),
					c("Description","varchar(255)")).withExtension().withPrivilege(),
			new ViewTableGenerator(InfoView.INFORMATION,
					"tenant",null,
					"tenant",
					"select t.ext_tenant_id as `NAME`, ud.name as `DATABASE`, "
					+"case t.suspended when 1 then 'YES' else 'NO' end as `SUSPENDED`, "
					+"t.description as `DESCRIPTION` "
					+"from tenant t inner join user_database ud on t.user_database_id = ud.user_database_id "
					+"order by t.ext_tenant_id ",
					c("NAME","varchar(255)"),
					c("DATABASE","varchar(255)"),
					c("SUSPENDED","varchar(3)"),
					c("DESCRIPTION","varchar(255)")).withExtension().withPrivilege(),
			new ViewTableGenerator(InfoView.MYSQL,
					"db",null,
					"mysql_db",
					// Db, Host, User
					"select u.accessSpec as `Host`, coalesce(t.ext_tenant_id,ud.name) as `Db`,  u.name as `User` "
					+"from user u inner join priviledge p on u.id = p.user_id "
					+"left outer join user_database ud on ud.user_database_id = p.user_database_id "
					+"left outer join tenant t on t.tenant_id = p.tenant_id "
					+"order by `Db` ",
					c("Host","varchar(60)"),
					c("Db","varchar(64)"),
					c("User","varchar(16)")),
			 new ViewTableGenerator(InfoView.SHOW,
					 "create database",null,
					 "show_create_database",
					 "select ud.name as `Database`, "
					 +"concat('CREATE DATABASE',if(@ine is not null,'/*!32312 IF NOT EXISTS*/',''),' `',ud.name,'` /*!40100 DEFAULT CHARACTER SET ',ud.default_character_set_name,' */') as `Create Database` "
					 +"from user_database ud where ud.name = @dbn",
					 c("Database","varchar(64)").withIdent(),
					 c("Create Database","varchar(1024)")),
			new ViewTableGenerator(InfoView.INFORMATION,
					"temporary_tables",null,
					"temporary_tables",
					"select tt.session_id as `SESSION_ID`, tt.db as `TABLE_SCHEMA`, tt.name as `TABLE_NAME`, tt.table_engine as `ENGINE` "
					+"from user_temp_table tt where tt.session_id = @sessid "
					+"order by tt.id ",
					c("SESSION_ID","int(11)"),
					c("TABLE_SCHEMA","varchar(255)"),
					c("TABLE_NAME","varchar(255)"),
					c("ENGINE","varchar(255)")),
			new ViewTableGenerator(InfoView.INFORMATION,
					"global_temporary_tables",null,
					"global_temporary_tables",
					"select tt.server_id as `SERVER_NAME`, tt.session_id as `SESSION_ID`, tt.db as `TABLE_SCHEMA`, "
					+"tt.name as `TABLE_NAME`, tt.table_engine as `ENGINE` "
					+"from user_temp_table tt order by tt.id ",
					c("SERVER_NAME","varchar(255)"),
					c("SESSION_ID","int(11)"),
					c("TABLE_SCHEMA","varchar(255)"),
					c("TABLE_NAME","varchar(255)"),
					c("ENGINE","varchar(255)")),
			new ViewTableGenerator(InfoView.INFORMATION,
					"group_provider",null,
					"group_provider",
					"select p.name as `NAME`, p.plugin as `PLUGIN`, case p.enabled when 1 then 'YES' else 'NO' end as `ENABLED` "
					+"from provider p",
					c("NAME","varchar(255)"),
					c("PLUGIN","varchar(255)"),
					c("ENABLED","varchar(3)")).withExtension().withPrivilege(),
			new ViewTableGenerator(InfoView.SHOW,
					"dynamic site provider","dynamic site providers",
					"show_dynamic_site_provider",
					"select p.name as `Group_Provider`, p.plugin as `Plugin`, case p.enabled when 1 then 'YES' else 'NO' end as `Enabled` "
					+"from provider p order by p.name ",
					c("Group_Provider","varchar(255)").withIdent(),
					c("Plugin","varchar(255)"),
					c("Enabled","varchar(3)")).withExtension().withPrivilege(),
			new ViewTableGenerator(InfoView.INFORMATION,
					"server",null,
					"server",
					"select s.name as `NAME`, s.ipAddress as `IPADDRESS` "
					+"from server s order by s.name ",
					c("NAME","varchar(255)"),
					c("IPADDRESS","varchar(255)")),
			new ViewTableGenerator(InfoView.SHOW,
					"server","servers",
					"server",
					"select s.name as `NAME`, s.ipAddress as `IPADDRESS` "
					+"from server s order by s.name ",
					c("NAME","varchar(255)").withIdent(),
					c("IPADDRESS","varchar(255)")),
			new ViewTableGenerator(InfoView.MYSQL,
					"user",null,
					"user",
					"select u.accessSpec as `Host`, u.name as `User`, u.password as `Password`, "
					+"case u.grantPriv when 1 then 'Y' else 'N' end as `Grant_priv` "
					+"from user u order by u.id ",
					c("Host","char(60)"),
					c("User","char(16)"),
					c("Password","char(41)"),
					// todo - this needs to be enum('Y','N')
					c("Grant_priv","char(1)")),
			new ViewTableGenerator(InfoView.INFORMATION,
					"variable_definitions",null,
					"variable_definitions",
					"select v.name as `NAME`, v.value as `VALUE`, v.value_type as `TYPE`, "
					+"v.scopes as `SCOPES`, v.options as `OPTIONS`, v.description as `DESCRIPTION` "
					+"from varconfig v order by v.name",
					c("NAME","varchar(255)"),
					c("VALUE","varchar(255)"),
					c("TYPE","varchar(255)"),
					c("SCOPES","varchar(255)"),
					c("OPTIONS","varchar(255)"),
					c("DESCRIPTION","varchar(255)")).withExtension(),
					new ViewTableGenerator(InfoView.INFORMATION,
					"external_service",null,
					"external_service",
					"select e.name as `NAME`, e.plugin as `PLUGIN`, "
					+"case e.auto_start when 1 then 'YES' else 'NO' end as `AUTO_START`,"
					+"e.connect_user as `CONNECT_USER`,"
					+"case e.uses_datastore when 1 then 'YES' else 'NO' end as `USES_DATASTORE`, "
					+"e.config as `CONFIG` "
					+"from external_service e order by e.id ",
					c("NAME","varchar(255)"),
					c("PLUGIN","varchar(255)"),
					c("AUTO_START","varchar(3)"),
					c("CONNECT_USER","varchar(255)"),
					c("USES_DATASTORE","varchar(3)"),
					c("CONFIG","longtext")).withExtension().withPrivilege(),
			new ViewTableGenerator(InfoView.SHOW,
					"external services",null,
					"show_external_services",
					"select e.name as `name`, e.plugin as `plugin`, "
					+"case e.auto_start when 1 then 'YES' else 'NO' end as `auto_start`,"
					+"e.connect_user as `connect_user`,"
					+"case e.uses_datastore when 1 then 'YES' else 'NO' end as `uses_datastore`, "
					+"e.config as `config` "
					+"from external_service e order by e.id ",
					c("name","varchar(255)").withIdent(),
					c("plugin","varchar(255)"),
					c("auto_start","varchar(3)"),
					c("connect_user","varchar(255)"),
					c("uses_datastore","varchar(3)"),
					c("config","longtext")).withExtension().withPrivilege(),
			new ViewTableGenerator(InfoView.SHOW,
					"triggers",null,
					"show_triggers",
					"select t.trigger_name as `Trigger`, t.trigger_event as `Event`, ut.name as `Table`, "
					+"t.trigger_body as `Statement`, t.trigger_time as `Timing`, NULL as `Created`, "
					+"t.sql_mode as `sql_mode`, " + buildDefiner("u") + " as `Definer`, "
					+"t.character_set_client as `character_set_client`, "
					+"t.collation_connection as `collation_connection`, "
					+"t.database_collation as `Database Collation` "
					+"from user_trigger t inner join user_table ut on t.table_id = ut.table_id "
					+"inner join user u on t.user_id = u.id "
					+"inner join user_database ud on ut.user_database_id = ud.user_database_id "
					+"where ud.name = @dbn ",
					c("Trigger","varchar(64)").withIdent().withOrderBy(0),
					c("Event","varchar(6)"),
					c("Table","varchar(64)"),
					c("Statement","longtext"),
					c("Timing","varchar(6)"),
					c("Created","datetime"),
					c("sql_mode","varchar(8192)"),
					c("Definer","varchar(77)"),
					c("character_set_client","varchar(32)"),
					c("collation_connection","varchar(32)"),
					c("Database Collation","varchar(32)")),
			new ViewTableGenerator(InfoView.SHOW,
					"create trigger",null,
					"show_create_trigger",
					"select t.trigger_name as `Trigger`, t.sql_mode as `sql_mode`, "
					+"concat('CREATE DEFINER=`',u.name,'`@`',u.accessSpec,'` ',t.origsql) as `SQL Original Statement`, "
					+"t.character_set_client as `character_set_client`, "
					+"t.collation_connection as `collation_connection`, "
					+"t.database_collation as `Database Collation` "
					+"from user_trigger t inner join user_table ut on t.table_id = ut.table_id "
					+"inner join user_database ud on ut.user_database_id = ud.user_database_id "
					+"inner join user u on t.user_id = u.id "
					+"where ud.name = @dbn ",
					c("Trigger","varchar(64)").withIdent(),
					c("sql_mode","varchar(8192)"),
					c("SQL Original Statement","longtext"),
					c("character_set_client","varchar(32)"),
					c("collation_connection","varchar(32)"),
					c("Database Collation","varchar(32)")),	
			new ViewTableGenerator(InfoView.INFORMATION,
					"triggers",null,
					"triggers",
					"select 'def' as `TRIGGER_CATALOG`, ud.name as `TRIGGER_SCHEMA`, t.trigger_name as `TRIGGER_NAME`, "
					+"t.trigger_event as `EVENT_MANIPULATION`, 'def' as `EVENT_OBJECT_CATALOG`, ud.name as `EVENT_OBJECT_SCHEMA`, "
					+"ut.name as `EVENT_OBJECT_TABLE`, 0 as `ACTION_ORDER`, NULL as `ACTION_CONDITION`, "
					+"t.trigger_body as `ACTION_STATEMENT`, 'ROW' as `ACTION_ORIENTATION`, t.trigger_time as `ACTION_TIMING`, "
					+"NULL as `ACTION_REFERENCE_OLD_TABLE`, NULL as `ACTION_REFERENCE_NEW_TABLE`, "
					+"'OLD' as `ACTION_REFERENCE_OLD_ROW`, 'NEW' as `ACTION_REFERENCE_NEW_ROW`, NULL as `CREATED`, "
					+"t.sql_mode as `SQL_MODE`, " + buildDefiner("u") + " as `DEFINER`, "
					+"t.character_set_client as `CHARACTER_SET_CLIENT`, "
					+"t.collation_connection as `COLLATION_CONNECTION`, "
					+"t.database_collation as `DATABASE_COLLATION` "
					+"from user_trigger t inner join user_table ut on t.table_id = ut.table_id "
					+"inner join user_database ud on ut.user_database_id = ud.user_database_id "
					+"inner join user u on t.user_id = u.id ",
					c("TRIGGER_CATALOG","varchar(512)"),
					c("TRIGGER_SCHEMA","varchar(64)"),
					c("TRIGGER_NAME","varchar(64)"),
					c("EVENT_MANIPULATION","varchar(6)"),
					c("EVENT_OBJECT_CATALOG","varchar(512)"),
					c("EVENT_OBJECT_SCHEMA","varchar(64)"),
					c("EVENT_OBJECT_TABLE","varchar(64)"),
					c("ACTION_ORDER","bigint(4)"),
					c("ACTION_CONDITION","longtext"),
					c("ACTION_STATEMENT","longtext"),
					c("ACTION_ORIENTATION","varchar(9)"),
					c("ACTION_TIMING","varchar(6)"),
					c("ACTION_REFERENCE_OLD_TABLE","varchar(64)"),
					c("ACTION_REFERENCE_NEW_TABLE","varchar(64)"),
					c("ACTION_REFERENCE_OLD_ROW","varchar(3)"),
					c("ACTION_REFERENCE_NEW_ROW","varchar(3)"),
					c("CREATED","datetime"),
					c("SQL_MODE","varchar(8192)"),
					c("DEFINER","varchar(77)"),
					c("CHARACTER_SET_CLIENT","varchar(32)"),
					c("COLLATION_CONNECTION","varchar(32)"),
					c("DATABASE_COLLATION","varchar(32)")),
			new DirectTableGenerator(InfoView.SHOW,
					"create table",null,
					c("Table","varchar(255)").withIdent(),
					c("Create Table","longtext")) {

						@Override
						public DirectInformationSchemaTable generate(
								SchemaContext sc) {
							return new DirectShowCreateTable(sc, buildColumns(sc), columns);
						}
				
			},
			new DirectTableGenerator(InfoView.SHOW,
					"external service",null,
					c("Name","varchar(255)").withIdent(),
					c("Status","varchar(255)")) {

						@Override
						public DirectInformationSchemaTable generate(
								SchemaContext sc) {
							return new DirectShowExternalService(sc,buildColumns(sc), columns);
						}
				
			}.withExtension().withPrivilege(),
			new DirectTableGenerator(InfoView.SHOW,
					"table status",null,
					c("Name","varchar(64)"),
					c("Engine","varchar(64)"),
					c("Version","bigint"),
					c("Row_format","varchar(10)"),
					c("Rows","bigint"),
					c("Avg_row_length","bigint"),
					c("Data_length","bigint"),
					c("Max_data_length","bigint"),
					c("Index_length","bigint"),
					c("Data_free","bigint"),
					c("Auto_increment","bigint"),
					c("Create_time","datetime"),
					c("Update_time","datetime"),
					c("Check_time","datetime"),
					c("Collation","varchar(32)"),
					c("Checksum","bigint"),
					c("Create_options","varchar(255)"),
					c("Comment","varchar(2048)")) {

						@Override
						public DirectInformationSchemaTable generate(
								SchemaContext sc) {
							return new DirectShowTableStatus(sc,buildColumns(sc),columns);
						}
				
			},
			new DirectTableGenerator(InfoView.SHOW,
					"status",null,
					c("Variable_name","varchar(64)").withIdent(),
					c("Value","varchar(1024)")) {

						@Override
						public DirectInformationSchemaTable generate(
								SchemaContext sc) {
							return new DirectShowStatusInformation(sc,buildColumns(sc),columns);
						}
				
			},
			new DirectTableGenerator(InfoView.INFORMATION,
					"session_variables",null,
					c("VARIABLE_NAME","varchar(64)"),
					c("VARIABLE_VALUE","varchar(1024)")) {

						@Override
						public DirectInformationSchemaTable generate(
								SchemaContext sc) {
							return new DirectVariablesTable(sc,InfoView.INFORMATION,buildColumns(sc),
									new UnqualifiedName("session_variables"),
									new VariableScope(VariableScopeKind.SESSION),
									columns);
						}
				
			},
			new DirectTableGenerator(InfoView.INFORMATION,
					"global_variables",null,
					c("VARIABLE_NAME","varchar(64)"),
					c("VARIABLE_VALUE","varchar(1024)")) {

						@Override
						public DirectInformationSchemaTable generate(
								SchemaContext sc) {
							return new DirectVariablesTable(sc,InfoView.INFORMATION,buildColumns(sc),
									new UnqualifiedName("global_variables"),
									new VariableScope(VariableScopeKind.GLOBAL),
									columns);
						}
				
			},
			new DirectTableGenerator(InfoView.SHOW,
					"variables",null,
					c("Scope","varchar(64)"),
					c("Variable_name","varchar(64)").withIdent(),
					c("Value","varchar(1024)")) {

						@Override
						public DirectInformationSchemaTable generate(
								SchemaContext sc) {
							return new DirectShowVariablesTable(sc,buildColumns(sc),columns);
						}				
			}
	};
	
	// helper functions
	private static String buildColumnFullTypeName(String uc) {
		String flags = uc + ".flags";
		StringBuilder buf = new StringBuilder();
		buf.append(String.format("case when %s.es_universe is not null then concat(%s.native_type_name,'(',%s.es_universe,')') else concat(",
				uc,uc,uc));
		buf.append(ColumnAttributes.buildSQLTest(flags, ColumnAttributes.SIZED_TYPE,
				String.format("if(%s.size = 0,%s.native_type_name,concat(%s.native_type_name,'(',%s.size,')'))",uc,uc,uc,uc),
				ColumnAttributes.buildSQLTest(flags, ColumnAttributes.PS_TYPE,
						String.format("if(%s.prec = 0 and %s.scale = 0,%s.native_type_name,concat(%s.native_type_name,'(',%s.prec,',',%s.scale,')'))",
								uc,uc,uc,uc,uc,uc),
						String.format("%s.native_type_name",uc)))).append(",");
		buf.append(ColumnAttributes.buildSQLTest(flags, ColumnAttributes.UNSIGNED, "' unsigned'", "''")).append(",");
		buf.append(ColumnAttributes.buildSQLTest(flags, ColumnAttributes.ZEROFILL, "'  zerofill'", "''")).append(") end");
		return buf.toString();
		
	}
	
	private static String buildColumnKey(String uc) {
		String flags = uc + ".flags";
		return ColumnAttributes.buildSQLTest(flags, ColumnAttributes.PRIMARY_KEY_PART, "'PRI'",
							ColumnAttributes.buildSQLTest(flags, ColumnAttributes.UNIQUE_KEY_PART, "'UNI'",
									ColumnAttributes.buildSQLTest(flags, ColumnAttributes.KEY_PART, "'MUL'", "''")));
	}
	
	private static String buildColumnExtra(String uc) {
		String flags = uc + ".flags";
		return ColumnAttributes.buildSQLTest(flags, ColumnAttributes.AUTO_INCREMENT, "'auto_increment'", 
				ColumnAttributes.buildSQLTest(flags, ColumnAttributes.ONUPDATE, "'on update CURRENT_TIMESTAMP'", "''"));
	}
	
	private static String buildDefiner(String userTable) {
		return String.format("concat(%s.name,'@',%s.accessSpec)",userTable,userTable);
	}
	
	private static DirectColumnGenerator c(String name, String decl) {
		return new DirectColumnGenerator(name,decl);
	}
}
