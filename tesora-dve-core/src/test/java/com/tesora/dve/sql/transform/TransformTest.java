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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.distribution.IColumnDatum;
import com.tesora.dve.distribution.IKeyValue;
import com.tesora.dve.distribution.RandomDistributionModel;
import com.tesora.dve.distribution.RangeDistributionModel;
import com.tesora.dve.distribution.StaticDistributionModel;
import com.tesora.dve.queryplan.TempTableDeclHints;
import com.tesora.dve.sql.SchemaTest;
import com.tesora.dve.sql.parser.InitialInputState;
import com.tesora.dve.sql.parser.InvokeParser;
import com.tesora.dve.sql.parser.ParserOptions;
import com.tesora.dve.sql.parser.PlanningResult;
import com.tesora.dve.sql.parser.PreparePlanningResult;
import com.tesora.dve.sql.schema.ConnectionValues;
import com.tesora.dve.sql.schema.DistributionKeyTemplate;
import com.tesora.dve.sql.schema.DistributionVector.Model;
import com.tesora.dve.sql.schema.PEPersistentGroup;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.TempTable;
import com.tesora.dve.sql.schema.cache.SchemaSourceFactory;
import com.tesora.dve.sql.schema.mt.AdaptiveMultitenantSchemaPolicyContext;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.TransientSchemaTest;
import com.tesora.dve.sql.transexec.TransientExecutionEngine;
import com.tesora.dve.sql.transform.execution.AbstractProjectingExecutionStep;
import com.tesora.dve.sql.transform.execution.AdhocResultsSessionStep;
import com.tesora.dve.sql.transform.execution.ConnectionValuesMap;
import com.tesora.dve.sql.transform.execution.DMLExplainRecord;
import com.tesora.dve.sql.transform.execution.DeleteExecutionStep;
import com.tesora.dve.sql.transform.execution.DirectExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionPlan;
import com.tesora.dve.sql.transform.execution.NestedExecutionPlan;
import com.tesora.dve.sql.transform.execution.RootExecutionPlan;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.ExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionType;
import com.tesora.dve.sql.transform.execution.FilterExecutionStep;
import com.tesora.dve.sql.transform.execution.HasPlanning;
import com.tesora.dve.sql.transform.execution.ParallelExecutionStep;
import com.tesora.dve.sql.transform.execution.ProjectingExecutionStep;
import com.tesora.dve.sql.transform.execution.RedistributionExecutionStep;
import com.tesora.dve.sql.transform.execution.SetVariableExecutionStep;
import com.tesora.dve.sql.transform.execution.SetVariableExecutionStep.VariableValueSource;
import com.tesora.dve.sql.transform.execution.TransactionExecutionStep;
import com.tesora.dve.sql.transform.execution.TriggerExecutionStep;
import com.tesora.dve.sql.transform.execution.UpdateExecutionSequence;
import com.tesora.dve.sql.transform.execution.UpdateExecutionStep;
import com.tesora.dve.sql.util.BinaryProcedure;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.sql.util.UnaryProcedure;

public abstract class TransformTest extends TransientSchemaTest {

	// help with setting out expecteds - fail if not set
	private static final boolean failOnUnsetExpected = Boolean.getBoolean("failOnUnsetExpected");
	private static final boolean printVerifyBlock = Boolean.getBoolean("printVerifyBlock");

	public static final Map<String,Object> NULL_FAKE_KEY = Collections.emptyMap();
	
	public static final String[] emptyDV = new String[] {};
	public static final String[][] emptyIndexes = new String[][] {};
	
	public TransformTest(String name) {
		super(name,"temp");
	}
	
	// debugging help
	protected String buildFirstPass(String in) throws Exception {
		return InvokeParser.parseOneLine(InvokeParser.buildInputState(in,null), ParserOptions.TEST.setFailEarly());
	}
	
	protected ExecutionPlan stmtTest(SchemaContext db, String in, Class<?> stmtClass, ExpectedSequence expected) throws Exception {
		return stmtTest(db,in,null,stmtClass, expected);
	}
	
	@SuppressWarnings("unchecked")
	protected ExecutionPlan stmtTest(SchemaContext db, String in, List<Object> params, Class<?> stmtClass, ExpectedSequence expected)  throws Exception {
		List<Object> actualParams = Collections.EMPTY_LIST;
		if (params != null) actualParams = params;
		List<Statement> stmts = parse(db, in, actualParams);
		assertEquals(stmts.size(), 1);
		Statement first = stmts.get(0);
		assertInstanceOf(first,stmtClass);
		PlanningResult pr = Statement.getExecutionPlan(db,first);
		RootExecutionPlan ep = (RootExecutionPlan) pr.getPlans().get(0);
		ConnectionValuesMap cvm = pr.getValues();
		if (isNoisy()) {
			System.out.println("In: '" + in + "'");
			ep.display(db, cvm, System.out, null);
		}
		verifyExecutionPlan(db,cvm,ep,expected);
		return ep;
	}

	protected static void verifyExecutionPlan(SchemaContext sc, ConnectionValuesMap cvm, ExecutionPlan toVerify, ExpectedSequence expected) {
		if (expected != null) {
			List<ExpectedStep> expectedExecOrder = new ArrayList<ExpectedStep>();
			expected.accumulateExecutionOrder(expectedExecOrder);
			final List<ExecutionStep> actualExecOrder = new ArrayList<ExecutionStep>();
			toVerify.getSequence().visitInTestVerificationOrder(new UnaryProcedure<HasPlanning>() {

				@Override
				public void execute(HasPlanning object) {
					if (object instanceof ExecutionStep)
						actualExecOrder.add((ExecutionStep)object);
				}

			});
			assertEquals("should have same number of steps",expectedExecOrder.size(), actualExecOrder.size());
			for(int i = 0; i < expectedExecOrder.size(); i++)
				expectedExecOrder.get(i).verify(sc,cvm,toVerify,actualExecOrder.get(i));
		} else {
			if (printVerifyBlock) {
				if (!isNoisy()) {
					try {
						throw new Exception("stack trace");
					} catch (Exception e) {
						e.fillInStackTrace();
						System.out.println("From test " + e.getStackTrace()[2].getMethodName());
					}
				}
				printExecutionPlanVerify(sc,cvm,toVerify);
			}
			if (failOnUnsetExpected)
				fail("Fill in expected result");			
		}
	}
	
	protected void cachePlanTest(SchemaContext db, String in, boolean hit, ExpectedSequence expected) throws Exception {
		db.refresh(true);
		PlanningResult results = InvokeParser.buildPlan(db, InvokeParser.buildInputState(in,db), ParserOptions.NONE.setDebugLog(true).setResolve().setFailEarly(),
				new VerifyingPlanCacheCallback(hit));
		List<ExecutionPlan> plans = results.getPlans();
		ConnectionValuesMap cv = results.getValues();
		assertEquals(plans.size(),1);
		ExecutionPlan ep = plans.get(0);
		if (isNoisy()) {
			System.out.println("In: '" + in + "'");
			ep.display(db,cv,System.out,null);
		}
		if (expected != null)
			expected.verify(db, cv, ep, ep.getSequence());
		else {
			if (printVerifyBlock)
				printExecutionPlanVerify(db, cv, ep);
			if (failOnUnsetExpected)
				fail("Fill in expected result");
		}
	}
	
	protected RootExecutionPlan prepareTest(SchemaContext db, String in, int numParams, ExpectedSequence expected)  throws Exception {
		SchemaSourceFactory.reset();
		db.refresh(true);
		PreparePlanningResult ppr = 
				(PreparePlanningResult) InvokeParser.preparePlan(db, new InitialInputState(in), 
				ParserOptions.NONE.setDebugLog(true).setResolve().setPrepare().setActualLiterals(), "42");
		List<Object> fakeParams = new ArrayList<Object>();
		for(int i = 0; i < numParams; i++)
			fakeParams.add("fp" + i);
		Pair<RootExecutionPlan,ConnectionValuesMap> res = ppr.getCachedPlan().rebuildPlan(db, fakeParams);
		RootExecutionPlan ep = res.getFirst();
		ConnectionValuesMap cv = res.getSecond();
		if (isNoisy()) {
			System.out.println("In: '" + in + "'");
			ep.display(db,cv,System.out,null);
		}
		if (expected != null)
			expected.verify(db,cv,ep,ep.getSequence());
		else {
			if (printVerifyBlock)
				printExecutionPlanVerify(db,cv,ep);
			if (failOnUnsetExpected)
				fail("Fill in expected result");
		}
		return ep;
	}
	
	protected void perfStmtTest(SchemaContext db, String in, int count) throws Exception {
		for(int i = 0; i < count; i++) {
			List<Statement> stmts = parse(db, in);
			assertEquals(stmts.size(), 1);
			Statement first = stmts.get(0);
			PlanningResult pr = Statement.getExecutionPlan(db,first);
			ExecutionPlan ep = pr.getPlans().get(0);
			// exercise the plan slightly
			ep.getUpdateCount(db,db.getValues());
		}
	}
	
	protected static HashMap<String, Object> buildFakeKey(Object[] args) {
		HashMap<String, Object> ret = new HashMap<String, Object>();
		for(int i = 0; i < args.length; i++) 
			ret.put((String)args[i], args[++i]);
		return ret;
	}

	protected static HashMap<String, Object> buildFakeKey(IKeyValue kv) {
		HashMap<String, Object> ret = new HashMap<String, Object>();
		for(Map.Entry<String, ? extends IColumnDatum> me : kv.getValues().entrySet()) { 
			ret.put(me.getKey(), me.getValue().getValue());
		}
		return ret;
	}
	
	protected static void verifyKey(Map<String, Object> fake, IKeyValue kv) {
		String anything = doVerifyKey(fake, kv);
		if (anything != null)
			fail(anything);
	}
	
	protected static String doVerifyKey(Map<String, Object> fake, IKeyValue kv) {
		// first make sure we have the same keys
		Set<String> fakeKeys = new HashSet<String>(fake.keySet());
		Set<String> actualKeys = new HashSet<String>(kv.getValues().keySet());
		if (!fakeKeys.equals(actualKeys)) {
			// figure out what's missing
			for(String k : fakeKeys)
				if (!actualKeys.contains(k))
					return "Expected to find column " + k + " in actual key, but did not";
			for(String k : actualKeys)
				if (!fakeKeys.contains(k))
					return "Found extra column " + k + " in actual key";
		}
		for(String k : fakeKeys) {
			Object fo = fake.get(k);
			Object ro = kv.getValues().get(k).getValue();
			if (!fo.equals(ro))
				return "Expected " + describeObject(fo) + ", but found " + describeObject(ro);
		}
		return null;
	}
		
	private static String describeObject(Object in) {
		return "'" + in + "' (" + (in == null ? "null" : in.getClass().getSimpleName()) + ")";
	}

	protected static void printExecutionStepVerify(SchemaContext pc, ConnectionValuesMap cvm, ExecutionPlan containing, HasPlanning hp, int nesting, boolean hasNext, StringBuilder buf) {
		StringBuffer prefix = new StringBuffer();
		for(int i = 0; i < nesting; i++)
			prefix.append("\t");		
		if (hp instanceof ExecutionSequence) {
			buf.append(prefix).append("bes(").append(PEConstants.LINE_SEPARATOR);
			ExecutionSequence es = (ExecutionSequence) hp;
			for(Iterator<HasPlanning> iter = es.getSteps().iterator(); iter.hasNext();) {
				printExecutionStepVerify(pc,cvm,containing,iter.next(),nesting+1,iter.hasNext(),buf);
			}
			buf.append(prefix).append(")");
		} else if (hp instanceof ParallelExecutionStep) {
			buf.append(prefix).append("bpes(").append(PEConstants.LINE_SEPARATOR);
			ParallelExecutionStep pes = (ParallelExecutionStep) hp;
			for(Iterator<ExecutionSequence> iter = pes.getSequences().iterator(); iter.hasNext();) {
				printExecutionStepVerify(pc,cvm,containing,iter.next(),nesting+1,iter.hasNext(),buf);
			}
			buf.append(prefix).append(")");
		} else if (hp instanceof AbstractProjectingExecutionStep) {
			AbstractProjectingExecutionStep aes =  (AbstractProjectingExecutionStep) hp;
			buf.append(prefix).append("new ProjectingExpectedStep(");
			if (aes.getExecutionType() == ExecutionType.SELECT) {
				buf.append("ExecutionType.SELECT,").append(PEConstants.LINE_SEPARATOR);
			} else {
				buf.append("ExecutionType.UNION,").append(PEConstants.LINE_SEPARATOR);
			}
			if (aes instanceof ProjectingExecutionStep) {
				buf.append(prefix).append("\tnull,").append(PEConstants.LINE_SEPARATOR);
			} else {
				ConnectionValues cv = cvm.getValues(containing);
				RedistributionExecutionStep redist = (RedistributionExecutionStep) aes;
				buf.append(prefix).append("\t").append(characterizeGroup(redist.getPEStorageGroup().getPEStorageGroup(pc,cv))).append(",");
				buf.append('"').append(redist.getRedistTable(pc,cv)).append('"').append(",").append(characterizeGroup(redist.getTargetGroup(pc,cv).getPEStorageGroup(pc,cv))).append(",");				
				Model targDistModel = redist.getDistKey().getModel(pc);
				buf.append(targDistModel.getCodeName()).append(".MODEL_NAME,").append(PEConstants.LINE_SEPARATOR);
				buf.append(prefix).append("\t");
				if (redist.getDistKey().getColumnNames().isEmpty())
					buf.append("emptyDV,");
				else {
					buf.append("new String[] {");
					Functional.join(redist.getDistKey().getColumnNames(), buf, ",", new BinaryProcedure<String,StringBuilder>() {

						@Override
						public void execute(String aobj, StringBuilder bobj) {
							bobj.append('"').append(aobj).append('"');
						}

					});
					buf.append(" },");
				}
				buf.append(PEConstants.LINE_SEPARATOR);

				PETable tab = redist.getTargetTable();
                if (tab instanceof TempTable){ //we have a temp table, so generate match for expected index hints.
                    buf.append(prefix).append("\t");
					TempTableDeclHints hints = ((TempTable) tab).finalizeHints(pc);
                    if (hints.getIndexes().isEmpty()) {
                    	buf.append("emptyIndexes,");
                    } else {
                    	buf.append("new String[][] {");
						Iterator<List<String>> entryIter = hints.getIndexes().iterator();
                    	while (entryIter.hasNext()){
							List<String> entry = entryIter.next();
                    		buf.append("{");
							Iterator<String> iter = entry.iterator();
                    		while (iter.hasNext()){
                    			String indexCol = iter.next();
                    			buf.append("\"");
                    			buf.append(indexCol);
                    			buf.append("\"");
                    			if (iter.hasNext())
                    				buf.append(",");
                    		}
                    		buf.append("}");
                    		if (entryIter.hasNext())
                    			buf.append(",");
                    	}
                    	buf.append(" },");
                    }
                    buf.append(PEConstants.LINE_SEPARATOR);
                }

			}
            appendSQL(aes,buf,pc,cvm,containing,prefix);
            if (aes instanceof ProjectingExecutionStep) {
            	ProjectingExecutionStep pes = (ProjectingExecutionStep) aes;
            	appendInMemoryLimit(pes,buf,pc,prefix);
            }
            // should do something about reasons here
            addExplainHint(aes.getExplainHint(),buf,pc,prefix);
		} else if (hp instanceof DeleteExecutionStep) {
			DeleteExecutionStep des = (DeleteExecutionStep) hp;
			appendDirectExpectedStepParams("DeleteExpectedStep",des,buf,prefix,pc,cvm,containing);
		} else if (hp instanceof UpdateExecutionStep) {
			UpdateExecutionStep ues = (UpdateExecutionStep) hp;
			appendDirectExpectedStepParams("UpdateExpectedStep",ues,buf,prefix,pc,cvm,containing);
		} else if (hp instanceof SetVariableExecutionStep) {
			SetVariableExecutionStep sves = (SetVariableExecutionStep) hp;
			VariableValueSource vvs = sves.getValueSource();
			String value = null;
			if (vvs.isConstant())
				value = vvs.getConstantValue(cvm.getValues(containing));
			buf.append("new SessionVariableExpectedStep(\"" + sves.getScopeName() + "\",\"" + sves.getVariableName() + "\"," + (value == null ? "null" : "\"" + value + "\"") + ")");
		} else if (hp instanceof TransactionExecutionStep) {
			TransactionExecutionStep tes = (TransactionExecutionStep) hp;
			buf.append("new TransactionExpectedStep(group,\"" + tes.getSQL(pc, cvm,containing,null) + "\")");
		} else if (hp instanceof FilterExecutionStep) {
			FilterExecutionStep fes = (FilterExecutionStep) hp;
			buf.append("new FilterExpectedStep(\"").append(fes.getFilter().describe()).append("\",");
			printExecutionStepVerify(pc,cvm,containing,fes.getSource(),0,false,buf);
			buf.append(")");
		} else if (hp instanceof TriggerExecutionStep) {
			TriggerExecutionStep tes = (TriggerExecutionStep) hp;
			buf.append(prefix).append("new TriggerExpectedStep(group,").append(PEConstants.LINE_SEPARATOR);
			printExecutionStepVerify(pc,cvm,containing,tes.getRowQuery(),nesting+1,true,buf);
			printExecutionStepVerify(pc,cvm,containing,tes.getActualStep(),nesting+1,true,buf);
			if (tes.getBeforeStep() == null)
				buf.append(prefix).append("  null,").append(PEConstants.LINE_SEPARATOR);
			else
				printExecutionStepVerify(pc,cvm,containing,tes.getBeforeStep(),nesting+1,true,buf);
			if (tes.getActualStep() == null)
				buf.append(prefix).append("  null").append(PEConstants.LINE_SEPARATOR);
			else
				printExecutionStepVerify(pc,cvm,containing,tes.getAfterStep(),nesting+1,false,buf);
			buf.append(")");
		} else if (hp instanceof NestedExecutionPlan) {
			NestedExecutionPlan nep = (NestedExecutionPlan) hp;
			printExecutionPlanVerify(pc,cvm,nep,nesting+1,hasNext,buf);
		} else {
			buf.append("null");
			//			throw new IllegalArgumentException("Unsupported step kind for emit verify check: " + hp.getClass().getName());
		}
		if (hasNext)
			buf.append(",");
		buf.append(PEConstants.LINE_SEPARATOR);
	}
	
	private static void appendDirectExpectedStepParams(String execClass, DirectExecutionStep des, StringBuilder buf, StringBuffer prefix, SchemaContext pc, ConnectionValuesMap cvm, ExecutionPlan containing) {
		buf.append(prefix).append("new ").append(execClass).append("(").append(PEConstants.LINE_SEPARATOR);
		buf.append(prefix).append("\t")
			.append(characterizeGroup(des.getPEStorageGroup().getPEStorageGroup(pc,cvm.getValues(containing))))
			.append(",").append(PEConstants.LINE_SEPARATOR);
		appendSQL(des,buf,pc,cvm,containing,prefix);
	}

	private static void appendSQL(ExecutionStep es, StringBuilder buf, SchemaContext pc, ConnectionValuesMap cvm, ExecutionPlan containing, StringBuffer prefix) {
        List<String> parts = new ArrayList<String>();
        es.displaySQL(pc, cvm,containing,parts, " ", null);
        for(Iterator<String> iter = parts.iterator(); iter.hasNext();) {
        	buf.append(prefix).append("  ").append('"').append(iter.next().trim()).append('"');
        	if (iter.hasNext()) buf.append(",");
        	buf.append(PEConstants.LINE_SEPARATOR);
        }
        buf.append(prefix).append(")");
	}
	
	private static void appendInMemoryLimit(ProjectingExecutionStep pes, StringBuilder buf, SchemaContext pc, StringBuffer prefix) {
		if (!pes.usesInMemoryLimit()) return;
		buf.append(PEConstants.LINE_SEPARATOR).append(prefix)
			.append(".withInMemoryLimit()");
	}
	
	private static void addExplainHint(DMLExplainRecord record,StringBuilder buf,SchemaContext pc,StringBuffer prefix) {
		if (record == null) return;
		buf.append(PEConstants.LINE_SEPARATOR).append(prefix)
			.append(".withExplain(new DMLExplainRecord(DMLExplainReason.").append(record.getReason().name());
		if (record.getRowEstimate() > -1)
			buf.append(",null,").append(record.getRowEstimate()).append("L");
		buf.append("))");
	}

	
	private static String characterizeGroup(PEStorageGroup pesg) {
		if (pesg.isTempGroup()) {
			for(int i = 0; i < transientGroups.length; i++) {
				if (transientGroups[i].equals(pesg)) {
					return transientGroups[i].getCodeName();
				}
			}
			throw new IllegalStateException("Unknown transient group kind: " + pesg);
		} else {
			return "group";
		}
	}
	
	protected static void printExecutionPlanVerify(SchemaContext pc, ConnectionValuesMap cvm, ExecutionPlan ep) {
		StringBuilder buf = new StringBuilder();
		printExecutionPlanVerify(pc,cvm,ep,0,false,buf);
		System.out.println(buf.toString());
	}
	
	protected static void printExecutionPlanVerify(SchemaContext pc, ConnectionValuesMap cvm, ExecutionPlan ep, int nesting, boolean hasNext, StringBuilder buf) {
		printExecutionStepVerify(pc,cvm,ep,ep.getSequence(),nesting,false,buf);
	}
	
	public static class ExpectedSequence extends ExpectedStep {
		
		protected ExpectedStep[] steps;
		
		
		public ExpectedSequence(ExpectedStep ...steps) {
			super(ExecutionSequence.class,(PEStorageGroup)null,(String[])null);
			this.steps = steps;
		}
		
		public void verify(SchemaContext sc, ConnectionValuesMap cvm, ExecutionPlan containing, ExecutionSequence es) {
			List<HasPlanning> actualSteps = es.getSteps();
			assertEquals(steps.length, actualSteps.size());
			for(int i = 0; i < steps.length; i++) {
				steps[i].verify(sc,cvm,containing,(ExecutionStep)actualSteps.get(i));
			}
		}
		
		public void accumulateExecutionOrder(List<ExpectedStep> acc) {
			for(ExpectedStep es : steps)
				es.accumulateExecutionOrder(acc);
		}

	}

	public static class ExpectedUpdateSequence extends ExpectedSequence {
		
		public ExpectedUpdateSequence(ExpectedStep ...steps) {
			super(steps);
		}
		
		// doesn't have any of it's own sql - is here for grouping
		@Override
		public void verify(SchemaContext sc, ConnectionValuesMap cvm, ExecutionPlan containing,ExecutionStep es) {
			SchemaTest.assertInstanceOf(es,UpdateExecutionSequence.class);
			UpdateExecutionSequence ues = (UpdateExecutionSequence) es;
			verify(sc,cvm,containing,ues);
		}
	}
	
	public static class ExpectedStep {
		
		protected Class<?> stepClass;
		protected String[] sql;
		protected PEStorageGroup sourceGroup;
		
		public ExpectedStep(Class<?> stepClass, PEStorageGroup sourceGroup, String... sql) {
			this.stepClass = stepClass;
			this.sql = sql;
			this.sourceGroup = sourceGroup;
		}
		
		protected String buildAssertTag(String what) {
			return what + " '" + sql + "'";
		}

		protected void verifySQL(SchemaContext sc, ConnectionValuesMap cvm, ExecutionPlan containing,  ExecutionStep es) {
			if (sql.length == 1) {
				String gen = es.getSQL(sc,cvm,containing,null).trim();
				assertEquals("expect same sql",this.sql[0], gen);
			} else {
				ArrayList<String> parts = new ArrayList<String>();
				es.displaySQL(sc, cvm,containing,parts, "", null);
				assertEquals("expect same number of pretty printed parts for '" + es.getSQL(sc,cvm,containing,null) + "'",sql.length,parts.size());
				for(int i = 0; i < sql.length; i++) {
					assertEquals("expect same sql for part " + i,sql[i],parts.get(i).trim());
				}
			}
		}
		
		public void verify(SchemaContext sc, ConnectionValuesMap cvm, ExecutionPlan containing, ExecutionStep es) {
			SchemaTest.assertInstanceOf(es, stepClass);
			verifySQL(sc,cvm, containing, es);
			if (sourceGroup != null) {
				PEStorageGroup gs = es.getPEStorageGroup().getPEStorageGroup(sc,cvm.getValues(containing));
				if (!sourceGroup.equals(gs)) {
					fail("Expected source group " + sourceGroup 
							+ " (" + sourceGroup.getClass().getSimpleName() 
							+ "@" + System.identityHashCode(sourceGroup) 
							+ ") but found " + gs
							+ " (" + gs.getClass().getSimpleName()
							+ "@" + System.identityHashCode(gs)
							+ ") at sql "
							+ es.getSQL(sc, cvm,containing,null)
							
							);
				}
			}
		}
		
		public void accumulateExecutionOrder(List<ExpectedStep> acc) {
			acc.add(this);
		}
	}
	
	public static class DirectExpectedStep extends ExpectedStep {

		protected Map<String,Object> fakeKey;
		
		public DirectExpectedStep(Class<?> stepClass,
				PEStorageGroup sourceGroup, String...sql) {
			super(stepClass,sourceGroup,sql);
		}
		
		public DirectExpectedStep withFakeKey(Map<String,Object> fake) {
			this.fakeKey = fake;
			return this;
		}
		
		@Override
		public void verify(SchemaContext sc, ConnectionValuesMap cvm, ExecutionPlan containing, ExecutionStep es) {
			super.verify(sc, cvm, containing, es);
			DirectExecutionStep des = (DirectExecutionStep) es;
			if (fakeKey != null) {
				IKeyValue ikv = des.getKeyValue(sc,cvm.getValues(containing));
				if (fakeKey == NULL_FAKE_KEY)
					assertNull("no key value should be specified",ikv);
				else
					verifyKey(fakeKey,ikv);
			}
		}
	}
	
	public static class ProjectingExpectedStep extends DirectExpectedStep {
		
		// target can either be a persistent persistent group or a temp group
		protected PEStorageGroup target;
		protected String redistTable;
		protected String redistModel;
		protected String[] dv;
        protected List<String[]> indices;
		protected Map<String,Object> key;
		protected boolean usesInMemoryLimit;
		protected ExecutionType executionType;
		protected DMLExplainRecord explainReason;
		
		public ProjectingExpectedStep(String sql, PEStorageGroup sourceGroup) {
			this(sourceGroup,sql);
		}
		
		public ProjectingExpectedStep(PEStorageGroup sourceGroup, String...sql) {
			this(ExecutionType.SELECT, sourceGroup,sql);
		}
		
		public ProjectingExpectedStep(ExecutionType exec, String sql, PEStorageGroup sourceGroup) {
			this(exec,sourceGroup,sql);
		}
		
		public ProjectingExpectedStep(ExecutionType exec, PEStorageGroup sourceGroup, String...sql) {
			this(exec, sourceGroup, null, (PEStorageGroup)null, null, null, sql);
		}

        public ProjectingExpectedStep(ExecutionType exec, String sql, PEStorageGroup sourceGroup,
                String redistTo, PEStorageGroup target,
                String model, String[] dv) {
        	this(exec,sourceGroup,redistTo,target,model,dv,sql);
        }
		
        public ProjectingExpectedStep(ExecutionType exec, PEStorageGroup sourceGroup,
                                      String redistTo, PEStorageGroup target,
                                      String model, String[] dv, String...sql) {
            this(exec,sourceGroup,redistTo,target,model,dv,null,sql);
        }

        
		public ProjectingExpectedStep(ExecutionType exec, String sql, PEStorageGroup sourceGroup,  
				String redistTo, PEStorageGroup target,
				String model, String[] dv, String[][] indices) {
			this(exec, sourceGroup, redistTo, target, model, dv, indices, sql);
		}
        
		public ProjectingExpectedStep(ExecutionType exec, PEStorageGroup sourceGroup,  
				String redistTo, PEStorageGroup target,
				String model, String[] dv, String[][] indices, String...sql) {
			super((redistTo == null ? ProjectingExecutionStep.class : RedistributionExecutionStep.class), sourceGroup,sql);
			redistTable = redistTo;
			this.target = target;
			this.redistModel = model;
			this.dv = dv;
			withIndices(indices);
		}
		
		public ProjectingExpectedStep(String sql, PEStorageGroup sourceGroup, Map<String,Object> fakeKey, 
				String redistTo, PEStorageGroup target,
				String model, String[] dv) {
			this(ExecutionType.SELECT,sql,sourceGroup,redistTo,target,model,dv);
		}

		public ProjectingExpectedStep(String sql, PEStorageGroup sourceGroup, String redistTo, PEStorageGroup target, String model,
				String[] dv) {
			this(ExecutionType.SELECT, sql,sourceGroup,redistTo,target,model,dv);
		}

        public ProjectingExpectedStep withInMemoryLimit() {
			this.usesInMemoryLimit = true;
			return this;
		}
		
        public ProjectingExpectedStep withIndices(String[][] indices) {
            if (indices == null)
                this.indices = null;
            else {
                this.indices = new ArrayList<String[]>();
                for (String[] entry : indices)
                    this.indices.add(entry);
            }
            return this;
        }

        public ProjectingExpectedStep withDistVec(String[] indv) {
        	this.dv = indv;
        	return this;
        }
        
        public ProjectingExpectedStep withExplain(DMLExplainRecord reason) {
        	this.explainReason = reason;
        	return this;
        }
        
		@Override
		public void verify(SchemaContext sc, ConnectionValuesMap cvm, ExecutionPlan containing, ExecutionStep es) {
			super.verify(sc,cvm, containing, es);
			AbstractProjectingExecutionStep ses = (AbstractProjectingExecutionStep) es;
			/*
			 * hint testing disabled for now
			String stepSQL = "'" + es.getSQL(sc,null) + "'";
			if (reasonable && explainReason != null) {
				assertNotNull("should have an explain hint for " + stepSQL,ses.getExplainHint());
				assertEquals("should have same explain reason for step " + stepSQL, explainReason.getReason(),ses.getExplainHint().getReason());
				if (explainReason.getRowEstimate() > -1)
					assertEquals("should have same row count estimate for step " + stepSQL, explainReason.getRowEstimate(),ses.getExplainHint().getRowEstimate());
			}
			*/
			
			ConnectionValues cv = cvm.getValues(containing);
			if (ses instanceof RedistributionExecutionStep) {
				RedistributionExecutionStep redist = (RedistributionExecutionStep) ses;
				if (redistTable == null)
					fail("Found RedistributionExecutionStep but expecting ProjectingExecutionStep");
				assertEquals(buildAssertTag("redist table should match"),redist.getRedistTable(sc,cv), redistTable);
				if (target != null) {
					PEStorageGroup targ = redist.getTargetGroup(sc,cv).getPEStorageGroup(sc,cv);
					assertNotNull("target group must exist", targ);
					if (!target.equals(targ)) {
						fail("Expected " + target + " but found " + targ + " on step " + es.getSQL(sc, cvm,containing,null));
					}
				} else {
					// no checking
				}
				assertEquals(buildAssertTag("redist model should match"),redistModel,redist.getTargetDistributionModel(sc));
				if (dv != null) {
					assertNotNull(redist.getDistKey());
					DistributionKeyTemplate kt = redist.getDistKey();
					List<String> tempColumns = kt.getColumnNames();
					assertEquals(dv.length,tempColumns.size());
					for(int i = 0; i < dv.length; i++)
						assertEquals("should have same dist vect",dv[i], tempColumns.get(i));
				}
                if (indices != null){
                    PETable tab = redist.getTargetTable();
                    if (tab instanceof TempTable){
						TempTableDeclHints hints = ((TempTable) tab).finalizeHints(sc);
						List<List<String>> sesIndices = hints.getIndexes();
                        assertEquals("should have same number of index hints for sql '" + es.getSQL(sc, cvm,containing,null) + "'",indices.size(),sesIndices.size());
                        for (int i=0;i< indices.size();i++){
							assertEquals("should have same index hint for sql '" + es.getSQL(sc, cvm,containing,null) + "'", Arrays.asList(indices.get(i)), sesIndices.get(i));
                        }
                    }
                }

			} else {
				ProjectingExecutionStep pes = (ProjectingExecutionStep) ses;
				if (redistTable != null)
					fail("Found ProjectingExecutionStep but expecting RedistributionExecutionStep");
				assertEquals("should have same in mem limit flag for step '" + es.getSQL(sc, cvm,containing,null) + "'", this.usesInMemoryLimit, pes.usesInMemoryLimit());				
			}			
		}
	}
	
	public static class AdhocResultsSessionExpectedStep extends ExpectedStep {

		public AdhocResultsSessionExpectedStep() {
			super(AdhocResultsSessionStep.class, (PEStorageGroup)null, (String[])null);
		}

		@Override
		public void verify(SchemaContext db, ConnectionValuesMap cvm, ExecutionPlan containing, ExecutionStep es) {
			SchemaTest.assertInstanceOf(es, stepClass);
		}
	}
	
	public static class UpdateExpectedStep extends DirectExpectedStep {

		public UpdateExpectedStep(PEStorageGroup sourceGroup, String... sql) {
			super(UpdateExecutionStep.class, sourceGroup, sql);
		}

	}
	
	public static class DeleteExpectedStep extends DirectExpectedStep {

		public DeleteExpectedStep(PEStorageGroup sourceGroup, String... sql) {
			super(DeleteExecutionStep.class, sourceGroup, sql);
		}
	}
	
	public static class ParallelExpectedStep extends ExpectedStep {

		private ExpectedSequence[] parts;
		
		public ParallelExpectedStep(ExpectedSequence ...seqs) {
			super(ParallelExecutionStep.class, (PEStorageGroup)null, (String[])null);
			parts = seqs;			
		}
		
		@Override
		public void verify(SchemaContext sc, ConnectionValuesMap cvm, ExecutionPlan containing, ExecutionStep es) {
			SchemaTest.assertInstanceOf(es, stepClass);
			ParallelExecutionStep pes = (ParallelExecutionStep) es;
			List<ExecutionSequence> sequences = pes.getSequences();
			assertEquals(parts.length, sequences.size());
			for(int i = 0; i < parts.length; i++)
				parts[i].verify(sc,cvm,containing,sequences.get(i));
		}
		
		public void accumulateExecutionOrder(List<ExpectedStep> acc) {
			for(ExpectedSequence es : parts)
				es.accumulateExecutionOrder(acc);
		}

	}

	public static class SessionVariableExpectedStep extends ExpectedStep {

		private String scopeName;
		private String variableName;
		private String constantValue;
		
		public SessionVariableExpectedStep(String scope, String variable, String constant) {
			super(SetVariableExecutionStep.class, null, (String[])null);
			this.scopeName = scope;
			this.variableName = variable;
			this.constantValue = constant;
		}

		@Override
		protected void verifySQL(SchemaContext sc, ConnectionValuesMap cvm, ExecutionPlan containing, ExecutionStep es) {
		}

		@Override
		public void verify(SchemaContext db, ConnectionValuesMap cvm, ExecutionPlan containing, ExecutionStep es) {
			super.verify(db,cvm,containing,es);
			SetVariableExecutionStep sves = (SetVariableExecutionStep) es;
			assertEquals("should have same scope name",scopeName,sves.getScopeName());
			assertEquals("should have same variable name",variableName,sves.getVariableName());
			VariableValueSource vvs = sves.getValueSource();
			assertTrue("should be a constant source",vvs.isConstant());
			String value = vvs.getConstantValue(cvm.getValues(containing));
			assertEquals("should have same constant value",constantValue,value);
		}
		
	}
	
	public static class TransactionExpectedStep extends ExpectedStep {

		public TransactionExpectedStep(PEStorageGroup sourceGroup, String sql) {
			super(TransactionExecutionStep.class, sourceGroup, sql);
		}
		
	}
	
	public static class FilterExpectedStep extends ExpectedStep {

		private final DirectExpectedStep source;
		private final String filterDescription;
		
		
		public FilterExpectedStep(String filterDescription, DirectExpectedStep src) {
			super(FilterExecutionStep.class, src.sourceGroup, (String[])null);
			this.source = src;
			this.filterDescription = filterDescription;
		}
		
		@Override
		protected void verifySQL(SchemaContext sc, ConnectionValuesMap cv, ExecutionPlan containing, ExecutionStep es) {
		}

		@Override
		public void verify(SchemaContext db, ConnectionValuesMap cv, ExecutionPlan containing, ExecutionStep es) {
			super.verify(db,cv,containing,es);
			FilterExecutionStep fes = (FilterExecutionStep) es;
			assertEquals("should have same filter description",filterDescription, fes.getFilter().describe());
			ExecutionStep haveSrc = fes.getSource();
			source.verify(db, cv,containing,haveSrc);
		}
	}
	
	public static class TriggerExpectedStep extends ExpectedStep {

		private final ExpectedStep actual;
		private final ExpectedStep before;
		private final ExpectedStep after;
		private final ExpectedStep rowQuery;
		
		public TriggerExpectedStep(PEStorageGroup sourceGroup, ExpectedStep rowQuery, ExpectedStep actual, ExpectedStep before, ExpectedStep after) {
			super(TriggerExecutionStep.class, sourceGroup, (String)null);
			this.rowQuery = rowQuery;
			this.actual = actual;
			this.before = before;
			this.after = after;
		}
		
		protected String buildAssertTag(String what) {
			return "trigger: " + what;
		}

		// no sql
		@Override
		protected void verifySQL(SchemaContext sc, ConnectionValuesMap cv, ExecutionPlan containing, ExecutionStep es) {
		}

		@Override
		public void verify(SchemaContext sc, ConnectionValuesMap cv, ExecutionPlan containing,ExecutionStep es) {
			super.verify(sc, cv, containing, es);
			TriggerExecutionStep trigStep = (TriggerExecutionStep) es;
			rowQuery.verify(sc, cv, containing, trigStep.getRowQuery());
			actual.verify(sc, cv, containing, trigStep.getActualStep());
			verifyTriggerBody(sc,cv,containing, "before",before,trigStep.getBeforeStep());
			verifyTriggerBody(sc,cv,containing,"after",after,trigStep.getAfterStep());
		}
		
		private void verifyTriggerBody(SchemaContext sc, ConnectionValuesMap cvm, ExecutionPlan containing, String which, ExpectedStep expected, HasPlanning actual) {
			if (expected == null && actual != null)
				fail("Found " + which + " step, but none expected");
			else if (actual == null && expected != null)
				fail("Missing " + which + " step");
			else if (actual != null && expected != null) {
				if (actual instanceof ExecutionPlan) 
					verifyExecutionPlan(sc,cvm,(ExecutionPlan)actual,(ExpectedSequence) expected);
				else
					expected.verify(sc, cvm, containing, (ExecutionStep) actual);
			}
		}

		/*
		public void accumulateExecutionOrder(List<ExpectedStep> acc) {
			if (before != null)
				before.accumulateExecutionOrder(acc);
			actual.accumulateExecutionOrder(acc);
			if (after != null)
				after.accumulateExecutionOrder(acc);
		}
	*/
		
	}
	
	// shortcuts for building expected steps
	public static ParallelExpectedStep bpes(ExpectedSequence ...seqs) {
		return new ParallelExpectedStep(seqs);
	}
	
	public static ExpectedSequence bes(ExpectedStep ...steps) {
		return new ExpectedSequence(steps);
	}

	public static final String MTID = AdaptiveMultitenantSchemaPolicyContext.TENANT_COLUMN;
	
	public static final String STATIC = StaticDistributionModel.MODEL_NAME;
	public static final String RANGE = RangeDistributionModel.MODEL_NAME;
	public static final String RANDOM = RandomDistributionModel.MODEL_NAME;
	
	private static final TransientExecutionEngine.TransientDynamicGroup transientGroups[] = new TransientExecutionEngine.TransientDynamicGroup[] {
		TransientExecutionEngine.AGGREGATION, TransientExecutionEngine.SMALL, TransientExecutionEngine.MEDIUM, TransientExecutionEngine.LARGE
	};

	private class VerifyingPlanCacheCallback implements com.tesora.dve.sql.schema.cache.PlanCacheUtils.PlanCacheCallback {

		private boolean shouldHit;
		
		public VerifyingPlanCacheCallback(boolean expectHit) {
			shouldHit = expectHit;
		}
		
		@Override
		public void onHit(String stmt) {
			if (shouldHit) return;
			fail("Hit plan cache but shouldn't have for " + stmt);
		}

		@Override
		public void onMiss(String stmt) {
			if (!shouldHit) return;
			fail("Did not hit plan cache but should have for " + stmt);
		}
		
	}
	
	public static PEPersistentGroup getGroup(SchemaContext db) {
		return db.getCurrentDatabase().getDefaultStorage(db);
	}
	

}
