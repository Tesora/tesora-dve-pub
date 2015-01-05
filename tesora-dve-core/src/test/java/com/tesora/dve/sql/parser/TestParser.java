package com.tesora.dve.sql.parser;

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

import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tesora.dve.db.DBNative;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.transexec.spi.TransientEngine;
import com.tesora.dve.sql.transexec.spi.TransientEngineFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.tesora.dve.server.connectionmanager.TestHost;
import com.tesora.dve.sql.ParserException;
import com.tesora.dve.sql.expression.Visitor;
import com.tesora.dve.sql.expression.VisitorContext;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.Subquery;
import com.tesora.dve.sql.node.structural.JoinedTable;
import com.tesora.dve.sql.node.structural.SortingSpecification;
import com.tesora.dve.sql.parser.ParserInvoker.LineInfo;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.ddl.DDLStatement;
import com.tesora.dve.sql.statement.ddl.PECreateStatement;
import com.tesora.dve.sql.statement.session.SessionStatement;
import com.tesora.dve.sql.transform.execution.ExecutionPlan;
import com.tesora.dve.sql.transform.execution.ExecutionStep;
import com.tesora.dve.sql.transform.execution.HasPlanning;
import com.tesora.dve.standalone.PETest;

public abstract class TestParser {

	@BeforeClass
	public static void setup() throws Exception {
		TestHost.startServicesTransient(PETest.class);
	}
	
	@AfterClass
	public static void teardown() throws Exception {
		TestHost.stopServices();
	}

	static final boolean noisy = Boolean.valueOf(System.getProperty("parser.debug")).booleanValue();
	
	public static boolean isNoisy() {
		return noisy;
	}
	
	public static void echo(String what) {
		if (noisy)
			System.out.println(what);
	}
	
	protected abstract ParserInvoker getDefaultInvoker() throws Exception;
		
	protected void parseOneStatement(String stmt, ParserInvoker pi) throws Throwable {
		LineInfo li = new LineInfo(0, pi.getOptions(), -1);
		pi.parseOneLineInternal(li, stmt);
	}
	
	protected void parseOneSqlFile(String fileName) throws Throwable {
		parseOneSqlFile(fileName, getDefaultInvoker());
	}
	
	protected void parseOneSqlFile(String fileName, ParserInvoker invoker) throws Throwable {
		new FileParser().parseOneSqlFile(fileName, invoker);
	}

	protected void parseOneMysqlLogFile(String fileName) throws Throwable {
		parseOneMysqlLogFile(fileName, getDefaultInvoker());
	}
	
	protected void parseOneMysqlLogFile(String fileName, ParserInvoker invoker) throws Throwable {
		new FileParser().parseOneMysqlLogFile(fileName, invoker);
	}

	protected void parseOnePELogFile(String fileName, ParserInvoker invoker) throws Throwable {
		new FileParser().parseOneFilePELog(getClass(), fileName, invoker);
	}
	
	public static class FirstPassInvoker extends ParserInvoker {

		public FirstPassInvoker(ParserOptions opts) {
			super(opts);
		}

		@Override
		public String parseOneLine(LineInfo info, String line) throws Exception {
			return InvokeParser.parseOneLine(InvokeParser.buildInputState(line,null), info.getOptions());			
		}

	}	

	private static class CorrectnessVisitor extends Visitor {
		
		// make sure everything has a parent
		private LanguageNode root;

		public static void verifyCorrectness(LanguageNode r) {
			if (r instanceof DDLStatement)
				return;
			CorrectnessVisitor cv = new CorrectnessVisitor(r);
			cv.visit(r);
		}
		
		public CorrectnessVisitor(LanguageNode r) {
			super();
			root = r;
		}
		
		@Override
		public <T extends LanguageNode> T visitNode(T t, VisitorContext vc) {
			if (t != null && t != root) {
				LanguageNode parent = t.getParent();
				if (parent == null) {
					StringBuilder buf = new StringBuilder();
                    Singletons.require(DBNative.class).getEmitter().emitTraversable(null,null,t, buf);
					String nodeDesc = buf.toString();
					buf = new StringBuilder();
                    Singletons.require(DBNative.class).getEmitter().emitTraversable(null,null,root, buf);
					fail("Unparented node: " + nodeDesc + " of: " + buf.toString());
				}
			}
			if (t instanceof SessionStatement)
				// session statements are unlikely to be rewritten; so let's skip them for now
				return t;
			return super.visitNode(t, vc);
		}
	}

	protected static class SecondPassInvoker extends ParserInvoker {

		public SecondPassInvoker(ParserOptions opts) {
			super(opts);
		}

		@Override
		public String parseOneLine(LineInfo info, String line) throws Exception {
			Statement s = null;
			s = InvokeParser.parse(InvokeParser.buildInputState(line,null), info.getOptions(), null).getStatements().get(0);
			CorrectnessVisitor.verifyCorrectness(s);
			// probably not correct
			return s.getSQL(null);
		}

	}

	protected static class SimulatingInvoker extends ParserInvoker {

		private TransientEngine tee;
		private int consecutiveExceptions;
		private FeatureVisitor features;
		
		public SimulatingInvoker(ParserOptions opts) throws Throwable {
			super(opts);
			// TODO Auto-generated constructor stub
			tee = Singletons.require(TransientEngineFactory.class).create("foo");
			tee.parse(metaschema_decl);
			SchemaContext pc = tee.getPersistenceContext();
			// we'll be accessing the backing classes, so go ahead and create them now
			pc.beginSaveContext();
			try {
				pc.getCurrentPEDatabase().persistTree(pc);
			} finally {
				pc.endSaveContext();
			}
			consecutiveExceptions = 0;
			features = new FeatureVisitor();
		}

		public SchemaContext getSchemaContext() {
			return tee.getPersistenceContext();
		}
		
		public Map<String,Integer> getFeatureCounts() {
			return features.getCounts(); 
		}
		
		private static final String[] metaschema_decl = new String[] {
			"create persistent site site1 'jdbc:mysql://s1/db1';",
			"create persistent site site2 'jdbc:mysql://s2/db1';",
			"create persistent group g1 add site1, site2;",
			"create project drupal default persistent group g1",
			"use project drupal",
			"create database drudb default persistent group g1; ",
			"use drudb"
		};

		protected void onSQL(String sql) throws Exception {
			// does nothing
		}
		
		@Override
		public String parseOneLine(LineInfo info, String line) throws Exception {
			// parse and then plan; we're not that interested in the plan results - we just want to
			// verify that we can plan without puking
			ParserOptions resopts = info.getOptions().setResolve();
			List<Statement> stmts = null;
			try {
				stmts = InvokeParser.parse(InvokeParser.buildInputState(line,null), resopts, tee.getPersistenceContext()).getStatements();
				consecutiveExceptions = 0;
			} catch (ParserException se) {
				if (se.getMessage().startsWith("No such Table")) {
					consecutiveExceptions++;
					if (consecutiveExceptions > 5)
						throw se;
					else
						return null;
				}
				throw se;
			}

			StringBuilder buf = new StringBuilder();
			PEDatabase pdb = tee.getPersistenceContext().getCurrentPEDatabase();
			for(Statement s : stmts) {
				CorrectnessVisitor.verifyCorrectness(s);
				features.visit(s);
				PlanningResult pr = Statement.getExecutionPlan(tee.getPersistenceContext(),s); 
				ExecutionPlan ep = pr.getPlans().get(0);
				boolean first = true;
				// make sure that we get sql back out without a problem!
				for(HasPlanning hp : ep.getSequence().getSteps()) {
					ExecutionStep es = (ExecutionStep) hp;
					String sql = es.getSQL(tee.getPersistenceContext(),pr.getValues(),ep,null);
					if ("".equals(sql) || sql == null)
						fail("No sql generated");
					if (first)
						first = false;
					else
						buf.append("; ");
					onSQL(sql);
					buf.append(sql);
				}
				// special case create table statements so that we can add them to the schema
				if (s instanceof PECreateStatement) {
					PECreateStatement<?,?> pecs = (PECreateStatement<?,?>)s;
					if (pecs.getCreated() instanceof PETable)
						pdb.getSchema().addTable(tee.getPersistenceContext(),(PETable)pecs.getCreated().get());
				}
			}

			// TODO Auto-generated method stub
			return buf.toString();
		}
	}

	protected static class FeatureVisitor extends Visitor {
		
		private HashMap<String, Integer> counts;
		
		public FeatureVisitor() {
			super();
			counts = new HashMap<String,Integer>();
		}

		public Map<String, Integer> getCounts() {
			return counts;
		}
		
		private void bump(String what) {
			Integer e = counts.get(what);
			if (e == null)
				e = new Integer(1);
			else
				e = new Integer(e.intValue() + 1);
			counts.put(what, e);
		}
		
		@Override
		public JoinedTable visitJoinedTable(JoinedTable jt, VisitorContext vc) {
			super.visitJoinedTable(jt,vc);
			if (jt.getJoinType().isInnerJoin()) {
				if (jt.getJoinType().isCrossJoin())
					bump("cross join");
				else
					bump("inner join");
			} else
				bump("outer join");
			return jt;
		}
		
		@Override
		public SortingSpecification visitSortingSpecification(SortingSpecification ss, VisitorContext vc) {
			super.visitSortingSpecification(ss, vc);
			if (Boolean.TRUE.equals(ss.isOrdering()))
				bump("order by");
			else if (Boolean.FALSE.equals(ss.isOrdering()))
				bump("group by");
			return ss;
		}
		
		@Override
		public ExpressionNode visitFunctionCall(FunctionCall fc, VisitorContext vc) {
			super.visitFunctionCall(fc,vc);
			if (fc.getFunctionName().isAggregate())
				bump("aggregate");
			return fc;
		}
		
		@Override
		public ExpressionNode visitSubquery(Subquery sq, VisitorContext vc) {
			super.visitSubquery(sq, vc);
			bump("subquery");
			return sq;
		}
	}
}
