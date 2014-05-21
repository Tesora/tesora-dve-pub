// OS_STATUS: public
package com.tesora.dve.sql.transform;

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

import org.junit.Ignore;
import org.junit.Test;

import com.tesora.dve.distribution.BroadcastDistributionModel;
import com.tesora.dve.distribution.StaticDistributionModel;
import com.tesora.dve.sql.schema.PEPersistentGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transexec.TransientExecutionEngine;
import com.tesora.dve.sql.transform.execution.ExecutionType;
import com.tesora.dve.sql.util.TestName;

public class NestedQueryTransformTest extends TransformTest {

	public NestedQueryTransformTest() {
		super("NestedQueryTransformTest");
	}
	
	private static final String[] leftySchema = new String[] {
		"create table `titles` (`id` int unsigned not null, `name` varchar(50) not null) static distribute on (`id`)",
		"create table `states` (`id` int unsigned not null, `name` varchar(50) not null, `tag` varchar(50)) static distribute on (`id`)",
		"create table `laws` (`id` int unsigned not null, `state_id` int unsigned not null, `title_id` int unsigned not null, " 
			+ "`status` varchar(16) not null default 'unpublished', `version` int unsigned not null, `law` varchar(100)) "
			+ "static distribute on (`id`)",
		"create table `counties` (`id` int unsigned not null, `name` varchar(50) not null, `state_id` int unsigned not null) " +
			"static distribute on (`id`)",
		"create table `courts` (`id` int unsigned not null, `county_id` int unsigned not null, `address` varchar(50) not null) " +
			"static distribute on (`id`)"
	};
	
	@Ignore
	@Test
	public void testNestedA() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI, leftySchema);
		stmtTest(db,
				"select t.name from `titles` t join (select l.title_id as id from `laws` l where l.status = 'published') q on t.id = q.id where t.name like '%custody%'",
				SelectStatement.class,
				null);
	}
	
	@Test
	public void testPE266() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table orders (`o_id` int, `o_entry_id` int, `o_carrier_id` int, `o_w_id` int, `o_d_id` int, `o_c_id` int, primary key (`o_id`))");
		PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,
				"select o_id, o_entry_id, coalesce(o_carrier_id,0) from orders where o_w_id= 1 and o_d_id = 1 and o_c_id = 1013 and o_id = (select max(o_id) from orders where o_w_id = 1 and o_d_id = 1 and o_c_id = 1013)",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
							group,"temp1",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }, new String[][] { },
						  "SELECT max( `orders`.`o_id` )  AS func_4",
						  "FROM `orders`",
						  "WHERE  (`orders`.`o_w_id` = 1 and `orders`.`o_d_id` = 1 and `orders`.`o_c_id` = 1013)"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							TransientExecutionEngine.AGGREGATION,"temp2",group,BroadcastDistributionModel.MODEL_NAME,new String[] { }, new String[][] { },
						  "SELECT max( temp1.func_4 )  AS func",
						  "FROM temp1"
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
							null,
						  "SELECT `orders`.`o_id` AS o1o0_5,`orders`.`o_entry_id` AS o1o1_6,coalesce( `orders`.`o_carrier_id`,0 )  AS func_7",
						  "FROM `orders`, temp2",
						  "WHERE `orders`.`o_w_id` = 1 and `orders`.`o_d_id` = 1 and `orders`.`o_c_id` = 1013 and `orders`.`o_id` = temp2.func"
						)
					)
					);
	}
    @Test
    public void testRandomScalarSubqueryWithGrandAggRedistToSG() throws Throwable {
        SchemaContext db = buildSchema(TestName.MULTI,
                "create range first_range (int) persistent group g1",
                "CREATE TABLE `first` (`id` int(10) unsigned NOT NULL,`name` varchar(60) NOT NULL DEFAULT '',`amount` int(10) unsigned NOT NULL, PRIMARY KEY (`id`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8  BROADCAST DISTRIBUTE",
                "CREATE TABLE `second` (`fid` int(10) NOT NULL,`flag` int(10) NOT NULL, PRIMARY KEY(`fid`,`flag`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8  RANDOM DISTRIBUTE"
        );

        String sql = "SELECT a.`id` AS id ,a.`name` AS name FROM `first` AS a WHERE a.`amount` <= (SELECT count(*) FROM `second` )";

        PEPersistentGroup group = db.getCurrentDatabase().getDefaultStorage(db);

        stmtTest(
                db,
                sql,
                SelectStatement.class,
                bes(
                        new ProjectingExpectedStep(ExecutionType.SELECT,
                                "SELECT count( * )  AS func_7 FROM `second`",
                                group,"temp1",TransientExecutionEngine.AGGREGATION,StaticDistributionModel.MODEL_NAME,new String[] { }),
                        new ProjectingExpectedStep(ExecutionType.SELECT,
                                "SELECT CONVERT( sum( temp1.func_7 ) ,SIGNED )  AS func FROM temp1",
                                TransientExecutionEngine.AGGREGATION,"temp2",group,BroadcastDistributionModel.MODEL_NAME,new String[] { }),
                        new ProjectingExpectedStep(ExecutionType.SELECT,
                                "SELECT a.`id` AS id,a.`name` AS name FROM `first` AS a, temp2 WHERE a.`amount` <= temp2.func",
                                null)
                )
        );
    }

	@Test
	public void testPE1476() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table B (`id` int unsigned not null, `desc` varchar(50), flags int) broadcast distribute;",
				"create table D (`id` int unsigned not null, `desc` varchar(50), flags int) range distribute on (`id`) using openrange");

		stmtTest(db,
				"select d.desc from D d where d.id in (select b.id from B b)",
				SelectStatement.class,
				bes(
				new ProjectingExpectedStep(ExecutionType.SELECT,
						null,
						"SELECT d.`desc` AS dd1_3",
						"FROM `D` AS d, `B` AS b",
						"WHERE d.`id` = b.`id`"
				)
				));
	}

	@Test
	public void testMultiModelNested() throws Exception {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table B (`id` int unsigned not null, `desc` varchar(50), flags int) broadcast distribute;",
				"create table C (`id` int unsigned not null, `desc` varchar(50), flags int) random distribute;",
				"create table D (`id` int unsigned not null, `desc` varchar(50), flags int) range distribute on (`id`) using openrange");

		stmtTest(db,
				"select desc from D where id in (select b.id from B b where b.flags in (select c.flags from C c where c.id > 0))",
				SelectStatement.class,
				bes(
				new ProjectingExpectedStep(ExecutionType.SELECT,
						null,
						"SELECT `D`.`desc` AS D1d1_3",
						"FROM `D`, `B` AS b",
						"WHERE `D`.`id` = b.`id` AND b.`flags` in ( (",
						"SELECT c.`flags`",
						"FROM `C` AS c",
						"WHERE c.`id` > 0) )"
				)
				));
	}
}
