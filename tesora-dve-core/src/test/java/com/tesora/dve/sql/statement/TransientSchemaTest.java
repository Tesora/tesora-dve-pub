package com.tesora.dve.sql.statement;

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.tesora.dve.common.catalog.TemplateMode;
import com.tesora.dve.server.connectionmanager.TestHost;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.SchemaTest;
import com.tesora.dve.sql.jg.CollapsedJoinGraph;
import com.tesora.dve.sql.jg.UncollapsedJoinGraph;
import com.tesora.dve.sql.parser.InvokeParser;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.SchemaSourceFactory;
import com.tesora.dve.sql.statement.dml.MultiTableDMLStatement;
import com.tesora.dve.sql.transexec.TransientExecutionEngine;
import com.tesora.dve.sql.util.TestName;
import com.tesora.dve.standalone.PETest;

public class TransientSchemaTest extends SchemaTest {

	@BeforeClass
	public static void setup() throws Exception {
		TestHost.startServicesTransient(PETest.class);
	}

	@AfterClass
	public static void teardown() throws Exception {
		TestHost.stopServices();
	}

	protected String projectName;
	protected String ttkern;
	
	public TransientSchemaTest(String projectName, String tempTableKern) {
		this.projectName = projectName;
		this.ttkern = tempTableKern;
	}
	
	public TransientSchemaTest(String projName) {
		this(projName,"temp");
	}
	
	public String[] getPESchemaDeclarations(TestName tn, String templateDecl) {
		List<String> decls = new ArrayList<String>();
		if (tn.isNativeCombo())
			throw new SchemaException(Pass.SECOND, "No support for native combo tests in transient schema tests: " + tn);
		if (tn.isPESingle()) {
			decls.add("Create persistent site site1 url='jdbc:mysql://s1/db1' user='floyd' password='floyd'");
			decls.add("create persistent group g1 add site1");
			addTemplateDeclaration(templateDecl, decls);
			String mtmode = (tn.isMT() ? " multitenant " : " ");			
			decls.add("create " + mtmode + "database mydb default persistent group g1 " + getTemplateDeclaration());
		} else {
			decls.add("create persistent site site1 url='jdbc:mysql://s1/db1' user='floyd' password='floyd'");
			decls.add("create persistent site site2 url='jdbc:mysql://s2/db1' user='floyd' password='floyd'");
			decls.add("create persistent group g1 add site1, site2;");
			decls.add("create persistent site site3 url='jdbc:mysql://s3/db1' user='floyd' password='floyd';");
			decls.add("create persistent site site4 url='jdbc:mysql://s4/db1' user='floyd' password='floyd';");
			decls.add("create persistent group t1 add site3, site4;");
			addTemplateDeclaration(templateDecl, decls);
			String mtmode = (tn.isMT() ? " multitenant " : " ");
			decls.add("create" + mtmode + "database mydb default persistent group g1 " + getTemplateDeclaration());
		}
		decls.add("create range openrange (int) persistent group g1;");
		decls.add("use mydb;");

		return decls.toArray(new String[0]);
	}
	
	private static void addTemplateDeclaration(final String templateDecl, final List<String> declarations) {
		if (templateDecl != null) {
			declarations.add(templateDecl);
		}
	}

	public String getTemplateDeclaration() {
		return "using template " + TemplateMode.OPTIONAL;
	}
	
	public SchemaContext buildSchema(TestName tn, String ...schema) throws Throwable {
		return buildSchema(null,tn,schema);
	}
	
	public SchemaContext buildSchema(String templateDecl, TestName tn,String ...schema) throws Throwable {
		SchemaContext pc = buildDatabase(tn,templateDecl, schema);
		pc.beginSaveContext();
		try {
			pc.getCurrentPEDatabase().persistTree(pc);
		} finally {
			pc.endSaveContext();
		}
		return pc;
	}
		
	public SchemaContext buildDatabase(String[] peschema, String[] sqlschema, TestName tn) throws Throwable {
		String[] sql = add(peschema, sqlschema);
		return buildDatabaseExecute(sql,tn);
	}
	
	public SchemaContext buildDatabase(TestName tn, String[] sqlschema) throws Throwable {
		return buildDatabase(tn,null,sqlschema);
	}
	
	public SchemaContext buildDatabase(TestName tn, String templateDecl, String[] sqlschema) throws Throwable {
		return buildDatabase(getPESchemaDeclarations(tn,templateDecl), sqlschema, tn);
	}

	public SchemaContext buildDatabase(TestName tn) throws Throwable {
		return buildDatabase(tn,(String)null);
	}
	
	public SchemaContext buildDatabase(TestName tn, String templateDecl) throws Throwable {
		return buildDatabase(getPESchemaDeclarations(tn, templateDecl), null, tn);
	}
	
	// mini execution engine to convert sql creates into the transient schema
	public SchemaContext buildDatabaseExecute(String[] in, TestName tn) throws Throwable {
		return buildExecutionEngine(in, tn).getPersistenceContext();
	}
	
	private String[] add(String[] left, String[] right) {
		ArrayList<String> all = new ArrayList<String>();
		all.addAll(Arrays.asList(left));
		if (right != null)
			all.addAll(Arrays.asList(right));
		String[] sql = all.toArray(new String[0]);
		return sql;
	}
	
	public final TransientExecutionEngine newExecutionEngine(TestName tn) {
		SchemaSourceFactory.reset();
		return new TransientExecutionEngine(ttkern);
	}
	
	public TransientExecutionEngine buildExecutionEngine(String[] in, TestName tn) throws Throwable {
		TransientExecutionEngine tee = newExecutionEngine(tn);
		tee.parse(in);
		return tee;
	}	
	
	public List<Statement> parse(SchemaContext pc, String stmt) throws Exception {
		return parse(pc,stmt,Collections.emptyList());
	}
	
	public List<Statement> parse(SchemaContext pc, String stmt, List<Object> params) throws Exception {
		SchemaSourceFactory.reset();
		pc.refresh(true);
		return InvokeParser.parse(stmt, pc, params);
	}

	// join graph test infrastructure
	protected UncollapsedJoinGraph buildUncollapsed(SchemaContext db, String sql) throws Throwable {
		Statement s = parse(db,sql).get(0);
		return new UncollapsedJoinGraph(db,(MultiTableDMLStatement) s);
	}
	
	protected CollapsedJoinGraph buildCollapsed(SchemaContext db, String sql) throws Throwable {
		return new CollapsedJoinGraph(db,buildUncollapsed(db,sql));
	}

	protected CollapsedJoinGraph buildCollapsed(SchemaContext db, UncollapsedJoinGraph ujg) throws Throwable {
		return new CollapsedJoinGraph(db,ujg);
	}
	
	protected void joinGraphTest(SchemaContext db, String sql, int nparts, int njoins) throws Throwable {
		CollapsedJoinGraph cjg = buildCollapsed(db,sql);
		if (nparts == -1) {
			System.out.println(sql);
			System.out.println(cjg.describe(db));
			System.out.println("nparts=" + cjg.getPartitions().size());
			System.out.println("njoins=" + cjg.getJoins().size());
		} else {
			assertEquals("partitions should match for '" + sql + "'",nparts,cjg.getPartitions().size());
			assertEquals("joins should match",njoins,cjg.getJoins().size());
		}
	}
	

}
