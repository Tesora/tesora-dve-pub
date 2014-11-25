package com.tesora.dve.sql.raw;

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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.common.TreeMapFactory;
import com.tesora.dve.common.catalog.ConstraintType;
import com.tesora.dve.common.catalog.IndexType;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.Traversal;
import com.tesora.dve.sql.node.expression.ConstantExpression;
import com.tesora.dve.sql.node.expression.DelegatingLiteralExpression;
import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.VariableInstance;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.parser.CandidateParser;
import com.tesora.dve.sql.parser.ExtractedLiteral;
import com.tesora.dve.sql.parser.InvokeParser;
import com.tesora.dve.sql.parser.ParserOptions;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.raw.jaxb.DMLStepType;
import com.tesora.dve.sql.raw.jaxb.DMLType;
import com.tesora.dve.sql.raw.jaxb.DistKeyColumnValue;
import com.tesora.dve.sql.raw.jaxb.DistKeyValue;
import com.tesora.dve.sql.raw.jaxb.DistVectColumn;
import com.tesora.dve.sql.raw.jaxb.DistributionType;
import com.tesora.dve.sql.raw.jaxb.DynamicGroupType;
import com.tesora.dve.sql.raw.jaxb.DynamicPersistentGroupType;
import com.tesora.dve.sql.raw.jaxb.GroupType;
import com.tesora.dve.sql.raw.jaxb.KeyType;
import com.tesora.dve.sql.raw.jaxb.LiteralType;
import com.tesora.dve.sql.raw.jaxb.ParameterType;
import com.tesora.dve.sql.raw.jaxb.ProjectingStepType;
import com.tesora.dve.sql.raw.jaxb.Rawplan;
import com.tesora.dve.sql.raw.jaxb.StepType;
import com.tesora.dve.sql.raw.jaxb.TargetTableType;
import com.tesora.dve.sql.raw.jaxb.TransactionActionType;
import com.tesora.dve.sql.raw.jaxb.TransactionStepType;
import com.tesora.dve.sql.raw.jaxb.UpdateStepType;
import com.tesora.dve.sql.schema.DistributionKey;
import com.tesora.dve.sql.schema.DistributionKeyTemplate;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.DistributionVector.Model;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PEKey;
import com.tesora.dve.sql.schema.PEKeyColumn;
import com.tesora.dve.sql.schema.PEKeyColumnBase;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.RangeDistribution;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.TempTable;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.ValueManager;
import com.tesora.dve.sql.schema.VectorRange;
import com.tesora.dve.sql.schema.cache.NonMTCachedPlan;
import com.tesora.dve.sql.schema.cache.PlanCacheKey;
import com.tesora.dve.sql.schema.cache.RegularCachedPlan;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.DeleteStatement;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.statement.dml.UpdateStatement;
import com.tesora.dve.sql.transform.VariableInstanceCollector;
import com.tesora.dve.sql.transform.execution.DMLExplainReason;
import com.tesora.dve.sql.transform.execution.DMLExplainRecord;
import com.tesora.dve.sql.transform.execution.DeleteExecutionStep;
import com.tesora.dve.sql.transform.execution.RootExecutionPlan;
import com.tesora.dve.sql.transform.execution.ExecutionStep;
import com.tesora.dve.sql.transform.execution.ProjectingExecutionStep;
import com.tesora.dve.sql.transform.execution.RedistributionExecutionStep;
import com.tesora.dve.sql.transform.execution.TransactionExecutionStep;
import com.tesora.dve.sql.transform.execution.UpdateExecutionStep;
import com.tesora.dve.sql.transform.strategy.TempGroupManager.TempGroupPlaceholder;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.Pair;

public class RawToExecConverter {

	public static RegularCachedPlan convert(SchemaContext sc,Rawplan rp, PEDatabase pdb) {
		RawToExecConverter converter = new RawToExecConverter(sc,rp,pdb);
		NonMTCachedPlan out = new NonMTCachedPlan(converter.getOrigStatement().getDerivedInfo()
				.getAllTableKeys(),converter.getParamTypes(),converter.getPlanCacheKey(),converter.getOrigStatement().getLockType());
		out.take(sc,converter.getOrigStatement(), converter.getPlan());
		return out;
	}
	
	private static final DMLExplainRecord rawExplain = DMLExplainReason.RAWPLAN.makeRecord();
	
	private final Rawplan raw;
	private final PEDatabase ondb;
	private final SchemaContext immutableContext;
	private SchemaContext variablesContext;
	private final RawDB rawdb;
	
	private DMLStatement stmt;
	private RootExecutionPlan plan;
	private String shrunk;
	private List<ExtractedLiteral.Type> types;
	private HashMap<String,TempGroupPlaceholder> declaredDynGroups;
	private HashMap<String,PEStorageGroup> usedPersGroups;
	private HashMap<String,DelegatingLiteralExpression> literalsForParameters;
	private HashMap<String,LiteralType> typesForParameters;
	private ValueManager origManager;
	
	public RawToExecConverter(SchemaContext incntxt, Rawplan rp, PEDatabase pdb) {
		raw = rp;
		ondb = pdb;
		immutableContext = incntxt;
		rawdb = new RawDB(pdb);
		declaredDynGroups = new HashMap<String,TempGroupPlaceholder>();
		usedPersGroups = new HashMap<String,PEStorageGroup>();
		literalsForParameters = new HashMap<String,DelegatingLiteralExpression>();
		typesForParameters = new HashMap<String,LiteralType>();
		buildShrunk();
		declareLiterals();
		declareDynGroups();
		buildPlan();
	}

	private Pair<SchemaContext, Statement> parse(String in, boolean step) {
		SchemaContext cc = SchemaContext.makeImmutableIndependentContext(immutableContext);
		if (origManager != null) {
			cc.setValueManager(origManager);
			cc.setValues(variablesContext.getValues());
		}
		cc.setCurrentDatabase(rawdb);
		ParserOptions options = ParserOptions.NONE.setDebugLog(true).setResolve().setFailEarly().setActualLiterals();
		if (step)
			options = options.setRawPlanStep();
		List<Statement> parsed = InvokeParser.parse(in, cc, Collections.emptyList(), options);
		immutableContext.setCurrentDatabase(ondb);
		return new Pair<SchemaContext,Statement>(cc,parsed.get(0));
	}
	
	public List<ExtractedLiteral.Type> getParamTypes() {
		return types;
	}
	
	public PlanCacheKey getPlanCacheKey() {
		return new PlanCacheKey(shrunk,ondb); 
	}
	
	public DMLStatement getOrigStatement() {
		return stmt;
	}
	
	public RootExecutionPlan getPlan() {
		return plan;
	}
	
	// to convert we need to:
	// [1] parse the match stmt
	// [2] declare the temp groups
	// [3] process the steps in turn
	
	private void buildShrunk() {
		String orig = raw.getInsql();
		// to build the cache key replace params with the original contents, then shrink that.
		String working = orig;
		MultiMap<Integer, ParameterType> sorted = new MultiMap<Integer, ParameterType>(new TreeMapFactory<Integer, Collection<ParameterType>>());
		for(ParameterType pt : raw.getParameter()) {
			sorted.put(pt.getName().length(), pt);
		}
		List<Integer> keys = Functional.toList(sorted.keySet());
		Collections.reverse(keys);
		int counter = 0;
		for(Integer i : keys) {
			for(ParameterType pt : sorted.get(i)) {
				typesForParameters.put(pt.getName(),pt.getType());
				String repl = null;
				String ith = Integer.toString(counter);
				if (pt.getType() == LiteralType.INTEGRAL)
					repl = ith;
				else if (pt.getType() == LiteralType.STRING)
					repl = "'" + ith + "'";
				else if (pt.getType() == LiteralType.DECIMAL)
					repl = ith + ".0";
				else if (pt.getType() == LiteralType.HEX)
					repl = "x'" + ith + "'";
				String lookFor = "@" + pt.getName();
				int index = working.indexOf(lookFor);
				if (index == -1)
					throw new SchemaException(Pass.PLANNER, "Unable to find use of parameter in query: " + lookFor);
				else
					index = working.indexOf(lookFor, index + 1);
				if (index != -1)
					throw new SchemaException(Pass.PLANNER, "Found multiple uses of parameter: " + lookFor);
				working = working.replace(lookFor,repl);				
			}
		}
		
		CandidateParser cp = new CandidateParser(working);
		if (!cp.shrink())
			throw new SchemaException(Pass.PLANNER, "Unable to build cache key for raw plan input");
		shrunk = cp.getShrunk();
		types = Functional.apply(cp.getLiterals(),ExtractedLiteral.typeAccessor);
	}
	
	private void declareLiterals() {
		Pair<SchemaContext,Statement> parseResult = parse(raw.getInsql(), false);
		stmt = (DMLStatement) parseResult.getSecond();
		variablesContext = parseResult.getFirst();
		ProjectionInfo pi = null;
		if (stmt instanceof SelectStatement) {
			pi = ((SelectStatement)stmt).getProjectionMetadata(variablesContext);
		}
		if (variablesContext.getValues() == null) 
			variablesContext.getValueManager().getValues(variablesContext, false);
		plan = new RootExecutionPlan(pi, variablesContext.getValueManager(), stmt.getStatementType());	
		origManager = plan.getValueManager();
		List<VariableInstance> variables = VariableInstanceCollector.getVariables(stmt);
		TreeMap<SourceLocation,VariableInstance> sorted = new TreeMap<SourceLocation,VariableInstance>();
		for(VariableInstance vi : variables) {
			sorted.put(vi.getSourceLocation(),vi);
		}
		for(VariableInstance vi : sorted.values()) {
			String name = vi.getVariableName().getUnquotedName().get();
			LiteralType type = typesForParameters.get(name);
			int tokType = EnumConverter.literalTypeToTokenType(type);
			int position = literalsForParameters.size();
			DelegatingLiteralExpression dle = new DelegatingLiteralExpression(tokType,
					vi.getSourceLocation(),variablesContext.getValues(),position,null);
			dle.setPosition(position, true);
			literalsForParameters.put(name, dle);
			origManager.addLiteralValue(variablesContext,position,null,dle);
		}
	}
	
	private void declareDynGroups() {
		for(DynamicGroupType dgt : raw.getDyngroup()) {
			if (dgt.getPg() != null) {
				PEStorageGroup pesg = findGroup(dgt.getPg());
				try {
					usedPersGroups.put(dgt.getName(), pesg.anySite(variablesContext));
				} catch (PEException pe) {
					throw new SchemaException(Pass.PLANNER, "Unable to build dynamic pers group " + dgt.getName(), pe);
				}
			} else {
				TempGroupPlaceholder tgph = new TempGroupPlaceholder(variablesContext,EnumConverter.convert(dgt.getSize()));
				declaredDynGroups.put(dgt.getName(), tgph);
			}
		}		
	}
	
	private void buildPlan() {
		
		for(StepType st : raw.getStep()) {
			if (st instanceof DMLStepType) {
				DMLStepType dmlst = (DMLStepType) st;
				if (dmlst.getAction() == DMLType.PROJECTING) {
					buildProjectingStep(dmlst);
				} else if (dmlst.getAction() == DMLType.DELETE) {
					buildDeleteStep(dmlst);
				} else if (dmlst.getAction() == DMLType.UPDATE) {
					buildUpdateStep(dmlst);
				}
			} else {
				buildTransactionStep(st);
			}
		}
		
	}
		
	@SuppressWarnings("unchecked")
	private void buildProjectingStep(DMLStepType in) {
		ProjectingStepType pst = (ProjectingStepType) in;
		Pair<SchemaContext,Statement> parseResults = parse(in.getSrcsql(), true);
		ProjectingStatement ps = (ProjectingStatement) parseResults.getSecond();
		ps = forwardVariables(ps);
		DistributionKey dk = buildDistKey(in.getDistkey());
		PEStorageGroup srcGroup = findGroup(in.getSrcgrp());
		DistributionVector srcDV = EngineConstant.BROADEST_DISTRIBUTION_VECTOR.getValue(ps, variablesContext);
		
		TargetTableType ttt = pst.getTarget();
		if (ttt == null) {
			try {
				ProjectingExecutionStep pes = 
						ProjectingExecutionStep.build(variablesContext, ondb, srcGroup, srcDV, dk, ps, rawExplain);
				plan.getSequence().append(pes);
			} catch (PEException pe) {
				throw new SchemaException(Pass.PLANNER, "Unable to build simple projection step",pe);
			}
		} else if (!ttt.isTemp()) {
			throw new SchemaException(Pass.PLANNER, "No support for redisting to userland table");
		} else {
			DistributionType dt = ttt.getDistvect();
			VectorRange anyRange = null;
			if (dt.getRange() != null) {
				RangeDistribution rd = parseResults.getFirst().findRange(new UnqualifiedName(dt.getRange()), new UnqualifiedName(ttt.getGroup()));
				if (rd == null) 
					throw new SchemaException(Pass.PLANNER, "Unknown range: " + dt.getRange() + " on storage group " + ttt.getGroup());
				anyRange = new VectorRange(parseResults.getFirst(),rd);
			}
			List<Integer> dvColOffsets = new ArrayList<Integer>();
			Model model = EnumConverter.convert(ttt.getDistvect().getModel());
			if (model.getUsesColumns()) {
				TreeMap<Integer,String> dvcols = new TreeMap<Integer,String>();
				for(DistVectColumn dvc : ttt.getDistvect().getColumn()) {
					dvcols.put(dvc.getPosition(),dvc.getName());
				}
				List<ExpressionNode> proj = ps.getProjections().get(0);
				HashMap<String,Integer> namesForOffsets = new HashMap<String,Integer>();
				for(int i = 0; i < proj.size(); i++) {
					ExpressionNode en = proj.get(i);
					if (en instanceof ExpressionAlias) {
						ExpressionAlias ea = (ExpressionAlias) en;
						namesForOffsets.put(ea.getAlias().get(), i);
					}
				}
				for(String s : dvcols.values())
					dvColOffsets.add(namesForOffsets.get(s));
			}
			TempTable tt = null;
			try {
				tt = TempTable.buildFromSelect(variablesContext, ps, dvColOffsets, Collections.EMPTY_LIST, 
					EnumConverter.convert(ttt.getDistvect().getModel()), anyRange, findGroup(ttt.getGroup()),-1);
			} catch (PEException pe) {
				throw new SchemaException(Pass.PLANNER,"Unable to build raw plan temp table",pe);
			}
			((RawSchema)rawdb.getSchema()).addTempTable(variablesContext, tt, ttt.getName());
			
			DistributionKeyTemplate dkt = new DistributionKeyTemplate(tt);
			int counter = 1;
			List<ExpressionNode> projection = ps.getProjections().get(0);
			for(Integer off : dvColOffsets) {
				dkt.addColumn(projection.get(off.intValue()), counter++);
			}
			
			// if keys were specified, add them now
			for(KeyType kt : ttt.getKey()) {
				// we add a key on the dist vect; if one already exists just use that
				UnqualifiedName keyName = new UnqualifiedName(kt.getName());
				PEKey exists = tt.lookupKey(variablesContext, keyName);
				boolean already = false;
				if (exists == null) {
					List<PEKeyColumnBase> cols = new ArrayList<PEKeyColumnBase>();
					for(String cn : kt.getColumn()) {					
						PEColumn pec = tt.lookup(variablesContext, cn);
						if (pec == null)
							throw new SchemaException(Pass.PLANNER, "No such column on temp table: " + cn);
						cols.add(new PEKeyColumn(pec,null,-1L));
					}
					IndexType indexType = null;
					if (kt.getType() != null)
						indexType = IndexType.fromPersistent(kt.getType());				
					exists = new PEKey(new UnqualifiedName(kt.getName()),indexType,cols,null);
				} else {
					already = true;
				}
				if (kt.getConstraint() != null) { 
					ConstraintType ct = ConstraintType.valueOf(kt.getConstraint());
					if (ct == null) 
						throw new SchemaException(Pass.PLANNER, "Unknown constraint kind: " + kt.getConstraint());
					exists.setConstraint(ct);
				}
				if (!already)
					tt.addKey(variablesContext, exists, false);
			}
			
			try {
				RedistributionExecutionStep pes =
						RedistributionExecutionStep.build(variablesContext, ondb, srcGroup, srcDV,
								ps, findGroup(ttt.getGroup()), tt, 
								/*redistToScopedTable=*/null, 
								/*dv=*/dkt, //DistributionKeyTemplate
								/*missingAutoInc=*/null, // PEColumn
								/*offsetToExistingAutoInc=*/null, // Integer
								/*onDupKey=*/null, // List<ExpressionNode>
								/*rc=*/null, // Boolean
								/*dk=*/null, // DistributionKey
								/*mustEnforceScalarValue=*/false,
								/*insertIgnore=*/false,
								/*tempTableGenerator=*/null,
								rawExplain);
				plan.getSequence().append(pes);
			} catch (PEException pe) {
				throw new SchemaException(Pass.PLANNER, "Unable to build redist step",pe);
			}
		}
	}
	
	private void buildDeleteStep(DMLStepType in) {
		UpdateStepType ust = (UpdateStepType) in;
		DeleteStatement ds = (DeleteStatement) parse(in.getSrcsql(),true).getSecond();
		ds = forwardVariables(ds);

		DistributionKey dk = buildDistKey(in.getDistkey());
		PEStorageGroup srcGroup = findGroup(in.getSrcgrp());
		PETable tab = null;
		if (ust.getTable().isTemp()) {
			throw new SchemaException(Pass.PLANNER, "No support yet for updating a temp table");
		} 
		tab = variablesContext.findTable(PEAbstractTable.getTableKey(ondb, new UnqualifiedName(ust.getTable().getName()))).asTable();
		TableKey tk = TableKey.make(variablesContext,tab,0); 
		try {
			DeleteExecutionStep des = 
					DeleteExecutionStep.build(variablesContext, ondb, srcGroup, tk, 
							dk, 
							ds, 
							/*requiresReferenceTimestamp=*/false, 
							rawExplain);
			plan.getSequence().append(des);
		} catch (PEException pe) {
			throw new SchemaException(Pass.PLANNER, "Unable to construct delete step",pe);
		}
	}
	
	private void buildUpdateStep(DMLStepType in) {
		UpdateStepType ust = (UpdateStepType) in;
		UpdateStatement us = (UpdateStatement) parse(in.getSrcsql(),true).getSecond();
		us = forwardVariables(us);

		DistributionKey dk = buildDistKey(in.getDistkey());
		PEStorageGroup srcGroup = findGroup(in.getSrcgrp());
		PETable tab = null;
		if (ust.getTable().isTemp()) {
			throw new SchemaException(Pass.PLANNER, "No support yet for updating a temp table");
		} 
		tab = variablesContext.findTable(PEAbstractTable.getTableKey(ondb, new UnqualifiedName(ust.getTable().getName()))).asTable();
		try {
			UpdateExecutionStep des =
					UpdateExecutionStep.build(variablesContext, ondb, srcGroup, tab, 
							dk, 
							us, 
							false,
							rawExplain);
			plan.getSequence().append(des);
		} catch (PEException pe) {
			throw new SchemaException(Pass.PLANNER, "Unable to construct delete step",pe);
		}
		
	}
	
	private void buildTransactionStep(StepType st) {
		TransactionStepType tst = (TransactionStepType) st;
		ExecutionStep es = null;
		try {
			if (tst.getKind() == TransactionActionType.BEGIN) {
				es = TransactionExecutionStep.buildStart(variablesContext, ondb);
			} else if (tst.getKind() == TransactionActionType.COMMIT) {
				es = TransactionExecutionStep.buildCommit(variablesContext, ondb);
			} else if (tst.getKind() == TransactionActionType.ROLLBACK) {
				es = TransactionExecutionStep.buildRollback(variablesContext, ondb);
			} else
				throw new SchemaException(Pass.PLANNER, "Unknown transaction kind: " + tst.getKind());
			plan.getSequence().append(es);
		} catch (PEException pe) {
			throw new SchemaException(Pass.PLANNER, "Unable to schedule transaction execution step",pe);
		}
	}
	
	private PEStorageGroup findGroup(String n) {
		PEStorageGroup c = declaredDynGroups.get(n);
		if (c != null) return c;
		c = usedPersGroups.get(n);
		if (c == null) {
			c = variablesContext.findStorageGroup(new UnqualifiedName(n));
			if (c == null)
				throw new SchemaException(Pass.PLANNER, "Unable to find persistent group " + n);
			usedPersGroups.put(n, c);
		}
		return c;
	}
	
	private <T extends DMLStatement> T forwardVariables(T in) {
		new VariableConversionTraversal(literalsForParameters).traverse(in);
		return in;
	}
	
	private DistributionKey buildDistKey(DistKeyValue dkv) {
		if (dkv == null) return null;
		PEAbstractTable<?> pet = variablesContext.findTable(PEAbstractTable.getTableKey(ondb, new UnqualifiedName(dkv.getTable())));
		if (pet == null)
			throw new SchemaException(Pass.PLANNER, "No such table: " + dkv.getTable());
		TableKey ptk = TableKey.make(variablesContext, pet, 0); 
		DistributionVector dv = pet.getDistributionVector(variablesContext);
		List<PEColumn> distCols = dv.getColumns(variablesContext);
		if (distCols.size() != dkv.getValue().size())
			throw new SchemaException(Pass.PLANNER, "Invalid dist key value on table " + dkv.getTable() + ", require " + distCols.size() + " values but have " + dkv.getValue().size());
		ListOfPairs<PEColumn,ConstantExpression> vect = new ListOfPairs<PEColumn,ConstantExpression>();
		TreeMap<Integer,DelegatingLiteralExpression> values = new TreeMap<Integer,DelegatingLiteralExpression>();
		for(DistKeyColumnValue dkcv : dkv.getValue()) {
			DelegatingLiteralExpression dle = literalsForParameters.get(dkcv.getParam());
			values.put(dkcv.getPosition(), (DelegatingLiteralExpression)dle.copy(null));
		}
		List<DelegatingLiteralExpression> litVals = Functional.toList(values.values());
		for(int i = 0; i < distCols.size(); i++) 
			vect.add(distCols.get(i), litVals.get(i));
		DistributionKey dk = pet.getDistributionVector(variablesContext).buildDistKey(variablesContext, ptk, vect);
		return dk;
	}

	
	private static class VariableConversionTraversal extends Traversal {

		private final Map<String,DelegatingLiteralExpression> forwarding;
		
		public VariableConversionTraversal(Map<String,DelegatingLiteralExpression> mapper) {
			super(Order.POSTORDER, ExecStyle.ONCE);
			forwarding = mapper;
		}
		
		@Override
		public LanguageNode action(LanguageNode in) {
			if (EngineConstant.VARIABLE.has(in)) {
				VariableInstance vi = (VariableInstance) in;
				String name = vi.getVariableName().getUnquotedName().get();
				DelegatingLiteralExpression repl = forwarding.get(name);
				if (repl == null)
					return in;
				DelegatingLiteralExpression ndle = (DelegatingLiteralExpression) repl.copy(null);
				return ndle;
			}
			return in;
		}
		
	}
	
}
