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

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.tesora.dve.distribution.BroadcastDistributionModel;
import com.tesora.dve.distribution.StaticDistributionModel;
import com.tesora.dve.sql.transexec.TransientExecutionEngine;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.execution.ExecutionType;
import com.tesora.dve.sql.transform.strategy.ViewRewriteTransformFactory;
import com.tesora.dve.sql.util.TestName;

public class ViewTransformTest extends TransformTest {

	public ViewTransformTest() {
		super("ViewTransformTest");
	}
	
	@Test
	public void testSimpleBCastPassthrough() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table A (id int, fid int, primary key (id)) broadcast distribute",
				"create view VA as select id, fid from A TABLE (id int, fid int)");
		stmtTest(db,
				"select id, fid from VA where id != fid",
				SelectStatement.class,
				bes(
						new ProjectingExpectedStep(ExecutionType.SELECT,
						"SELECT `VA`.`id` AS V1i0,`VA`.`fid` AS V1f1 FROM `VA` WHERE `VA`.`id` != `VA`.`fid`",
							null)
					)
					);
	}
	
	@Test
	public void testSimpleSelectRandomEmulate() throws Throwable {
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table A (id int, fid int, primary key (id)) random distribute",
				"create table B (id int, stuff varchar(32), primary key (id)) random distribute",
				"create view AB as select a.id aid, a.fid, b.stuff from A a inner join B b on a.id = b.id TABLE (aid int, fid int, stuff varchar(32))");
		String sql = "select ab.aid, ab.stuff from AB ab where ab.fid > 2";
		viewTransTest(db,sql,
				"SELECT a.`id`,b.`stuff` FROM `A` AS a INNER JOIN `B` AS b ON a.`id` = b.`id` WHERE a.`fid` > 2");
		PEStorageGroup group = db.getCurrentDatabase().getDefaultStorage(db);
		stmtTest(db,sql,SelectStatement.class,
				bes(
						bpes(
							bes(
								new ProjectingExpectedStep(ExecutionType.SELECT,
								"SELECT a.`id` AS ai0_2 FROM `A` AS a WHERE a.`fid` > 2",
									group,"temp3",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"ai0_2" }),
								new ProjectingExpectedStep(ExecutionType.SELECT,
								"SELECT DISTINCT temp3.ai0_2 AS t4a0 FROM temp3",
									TransientExecutionEngine.LARGE,"temp4",group,BroadcastDistributionModel.MODEL_NAME,new String[] { }),
								new ProjectingExpectedStep(ExecutionType.SELECT,
								"SELECT b.`stuff` AS bs1_6,b.`id` AS bi0_7 FROM `B` AS b, temp4 WHERE temp4.t4a0 = b.`id`",
									group,"temp5",TransientExecutionEngine.LARGE,StaticDistributionModel.MODEL_NAME,new String[] {"bi0_7" })
							)
						),
						new ProjectingExpectedStep(ExecutionType.SELECT,
						"SELECT temp3.ai0_2 AS t6a0_7,temp5.bs1_6 AS t7b0_8 FROM temp3 INNER JOIN temp5 ON temp3.ai0_2 = temp5.bi0_7",
							null)
					));
	}	
	
	protected void viewTransTest(SchemaContext db, String in, String out)  throws Exception {
		List<Statement> stmts = parse(db, in, Collections.emptyList());
		assertEquals(stmts.size(), 1);
		DMLStatement first = (DMLStatement) stmts.get(0);
		ViewRewriteTransformFactory.applyViewRewrites(db, first);
		String xformed = first.getSQL(db);
		if (isNoisy()) {
			System.out.println("In: '" + in + "'");
			System.out.println("Out: '" + xformed + "'");
		}
		if (out != null) {
			assertEquals(out,xformed);
		} else {
			System.out.println(xformed);
		}
	}

	// so we want to test the view used in a simple select, a complex select, at the head of the ftr, in the middle
	// not in the ftr at all (nested), etc.
	@Test
	public void testViewRewrite() throws Throwable {
		String body1 = "(aid int, afid int, astuff varchar(32), primary key (aid))";
		String body2 = "(bid int, bfid int, bstuff varchar(32), primary key (bid))";
		SchemaContext db = buildSchema(TestName.MULTI,
				"create table AA " + body1 + " random distribute",
				"create table BA " + body1 + " broadcast distribute",
				"create table SA " + body1 + " static distribute on (aid)",
				"create table RA " + body1 + " range distribute on (aid) using openrange",
				"create table AB " + body2 + " random distribute",
				"create table BB " + body2 + " broadcast distribute",
				"create table SB " + body2 + " static distribute on (bid)",
				"create table RB " + body2 + " range distribute on (bid) using openrange",
				"create view VAB as select a.aid as id, s.bfid as fid, a.astuff as stuff from AA a inner join SB s on a.aid = s.bid where a.astuff like '%wow%' "
				+"TABLE(id int, fid int, stuff varchar(32))"
						
				);
		viewTransTest(db,"select * from VAB", 
				"SELECT * FROM `AA` AS a INNER JOIN `SB` AS s ON a.`aid` = s.`bid` WHERE a.`astuff` like '%wow%'"
				);
		viewTransTest(db,"select count(*), stuff from VAB where id != fid group by stuff",
				"SELECT count( * ) ,a.`astuff` FROM `AA` AS a INNER JOIN `SB` AS s ON a.`aid` = s.`bid` WHERE a.`aid` != s.`bfid` AND a.`astuff` like '%wow%' GROUP BY a.`astuff` ASC"
				);
		viewTransTest(db,"select a.fid from VAB a where a.id % 2 = 0",
				"SELECT s.`bfid` FROM `AA` AS a_2 INNER JOIN `SB` AS s ON a_2.`aid` = s.`bid` WHERE a_2.`aid` % 2 = 0 AND a_2.`astuff` like '%wow%'"
				);
		viewTransTest(db,"select v.fid from VAB v inner join AB a on v.id = a.bid where a.bstuff = 'boy howdy'",
				"SELECT s.`bfid` FROM `AA` AS a_3 INNER JOIN `SB` AS s ON a_3.`aid` = s.`bid` INNER JOIN `AB` AS a ON a_3.`aid` = a.`bid` WHERE a.`bstuff` = 'boy howdy' AND a_3.`astuff` like '%wow%'"
				);
		viewTransTest(db,"select v.fid from AB a inner join VAB v on v.id = a.bid where v.stuff = 'boy howdy'",
				"SELECT v.`fid` FROM `AB` AS a INNER JOIN ( SELECT a_3.`aid` AS id,s.`bfid` AS fid,a_3.`astuff` AS stuff FROM `AA` AS a_3 INNER JOIN `SB` AS s ON a_3.`aid` = s.`bid` WHERE a_3.`astuff` like '%wow%' AND a_3.`astuff` = 'boy howdy') AS v ON v.`id` = a.`bid` WHERE v.`stuff` = 'boy howdy'"
				);
	}	
}
