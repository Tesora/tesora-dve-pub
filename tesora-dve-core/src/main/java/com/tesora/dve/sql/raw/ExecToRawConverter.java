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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.tesora.dve.db.GenericSQLCommand;
import com.tesora.dve.resultset.IntermediateResultSet;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.parser.CandidateParser;
import com.tesora.dve.sql.parser.ExtractedLiteral;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.raw.jaxb.DMLStepType;
import com.tesora.dve.sql.raw.jaxb.DMLType;
import com.tesora.dve.sql.raw.jaxb.DistVectColumn;
import com.tesora.dve.sql.raw.jaxb.DistributionType;
import com.tesora.dve.sql.raw.jaxb.DynamicGroupType;
import com.tesora.dve.sql.raw.jaxb.ParameterType;
import com.tesora.dve.sql.raw.jaxb.ProjectingStepType;
import com.tesora.dve.sql.raw.jaxb.Rawplan;
import com.tesora.dve.sql.raw.jaxb.StepType;
import com.tesora.dve.sql.raw.jaxb.TargetTableType;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEDynamicGroup;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.IDelegatingLiteralExpression;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.transform.execution.AbstractProjectingExecutionStep;
import com.tesora.dve.sql.transform.execution.DeleteExecutionStep;
import com.tesora.dve.sql.transform.execution.DirectExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionPlan;
import com.tesora.dve.sql.transform.execution.ExecutionStep;
import com.tesora.dve.sql.transform.execution.HasPlanning;
import com.tesora.dve.sql.transform.execution.RedistributionExecutionStep;
import com.tesora.dve.sql.transform.execution.TransactionExecutionStep;
import com.tesora.dve.sql.transform.execution.UpdateExecutionStep;
import com.tesora.dve.sql.util.UnaryProcedure;

public final class ExecToRawConverter {

	public static IntermediateResultSet convertForRawExplain(SchemaContext sc, ExecutionPlan ep, Statement orig, String origSQL) {
		return RawUtils.buildRawExplainResults(convert(sc, ep,orig,origSQL));
	}

	public static Rawplan convert(SchemaContext sc, ExecutionPlan ep, Statement orig, String origSQL) {
		if (origSQL == null)
			throw new SchemaException(Pass.PLANNER,"Raw plan conversion requires original sql");
		ExecToRawConverter c = new ExecToRawConverter(sc, ep,orig,origSQL);
		return c.getRaw();
	}
	
	private final ExecutionPlan thePlan;
	private final String originalSQL;
	private final SchemaContext sc;

	private final Rawplan raw;
	
	private HashMap<IDelegatingLiteralExpression, ParameterType> parameters;
	private LinkedHashMap<PEStorageGroup,DynamicGroupType> dynGroups;
	
	public ExecToRawConverter(SchemaContext pc, ExecutionPlan ep, Statement orig, String origSQL) {
		raw = new Rawplan();
		thePlan = ep;
		sc = pc;
		String sql = origSQL;
		SourceLocation sloc = orig.getSourceLocation();
		int offset = sloc.getPositionInLine();
		if (offset > -1)
			sql = origSQL.substring(offset);
		originalSQL = sql;
		parameters = new HashMap<IDelegatingLiteralExpression, ParameterType>();
		dynGroups = new LinkedHashMap<PEStorageGroup,DynamicGroupType>();
		convert();
	}

	public Rawplan getRaw() {
		return raw;
	}
	
	private void convert() {
		defineMatchAndVars();
		defineGroupsAndSteps();
	}
	
	private void defineMatchAndVars() {
		// take a stmt of the form select a.id from A a where a.pid = 15 limit 2
		// into select a.id from A a where a.pid = @p1 limit @p2
		// and record types for variables.
		CandidateParser cp = new CandidateParser(originalSQL);
		if (!cp.shrink())
			throw new SchemaException(Pass.PLANNER, "Unable to shrink supposed cacheable input for raw explain");
		List<ExtractedLiteral> shrunkLiterals = cp.getLiterals();
	
		HashMap<ExtractedLiteral,ParameterType> paramForLit = new HashMap<ExtractedLiteral,ParameterType>();
		
		List<IDelegatingLiteralExpression> literals = thePlan.getValueManager().getRawLiterals();
		for(IDelegatingLiteralExpression idle : literals) {
			if (idle.getPosition() == 0) continue;
			int pos = idle.getPosition() - 1;
			ExtractedLiteral ex = shrunkLiterals.get(pos);
			ParameterType pt = new ParameterType();
			pt.setName("p" + pos);
			pt.setType(EnumConverter.convert(ex.getType()));
			parameters.put(idle,pt);
			paramForLit.put(ex, pt);
		}
		
		StringBuilder buf = new StringBuilder();
		String sbuf = cp.getShrunk();
		ExtractedLiteral prev = null;
		for(ExtractedLiteral el : shrunkLiterals) {
			int prevIndex = (prev == null ? 0 : prev.getFinalOffset() + 1);
			int curIndex = el.getFinalOffset();
			buf.append(sbuf.substring(prevIndex, curIndex));
			buf.append("@").append(paramForLit.get(el).getName());
			prev = el;
		}
		if (prev != null)
			buf.append(sbuf.substring(prev.getFinalOffset()+1));
		else
			buf.append(originalSQL);
		String match = buf.toString();
		raw.setInsql(match);
		for(ExtractedLiteral el : shrunkLiterals) {
			ParameterType pt = paramForLit.get(el);
			raw.getParameter().add(pt);
		}
	}
	
	private void defineGroupsAndSteps() {
		final List<ExecutionStep> stepsInOrder = new ArrayList<ExecutionStep>();
		thePlan.visitInExecutionOrder(new UnaryProcedure<HasPlanning>() {

			@Override
			public void execute(HasPlanning object) {
				stepsInOrder.add((ExecutionStep) object);
			}
			
		});
		for(ExecutionStep es : stepsInOrder) {
			PEStorageGroup src = es.getPEStorageGroup();
			maybeAccDynGroup(src);
			if (es instanceof RedistributionExecutionStep) {
				RedistributionExecutionStep pes = (RedistributionExecutionStep) es;
				maybeAccDynGroup(pes.getTargetGroup(sc));
			}
		}
		for(DynamicGroupType dgt : dynGroups.values()) {
			raw.getDyngroup().add(dgt);
		}
		for(ExecutionStep es : stepsInOrder) {
			StepType out = null;
			if (es instanceof AbstractProjectingExecutionStep) {
				out = buildProjectingStep((AbstractProjectingExecutionStep)es);
			} else if (es instanceof UpdateExecutionStep || es instanceof DeleteExecutionStep) {
				out = buildUpdateStep((DirectExecutionStep)es);
			} else if (es instanceof TransactionExecutionStep) {
				out = buildTransactionStep((TransactionExecutionStep)es);
			} else {
				throw new SchemaException(Pass.PLANNER, "Unable to convert to raw plan step for " + es.getClass().getSimpleName());
			}
			if (out == null)
				throw new SchemaException(Pass.PLANNER, "Unable to build raw plan step for " + es.getClass().getSimpleName());
			raw.getStep().add(out);
		}
	}
	
	private void maybeAccDynGroup(PEStorageGroup g) {
		if (g == null) return;
		if (!g.isTempGroup()) return;
		PEDynamicGroup dynGroup = (PEDynamicGroup) g.getPEStorageGroup(sc);
		DynamicGroupType e = dynGroups.get(dynGroup);
		if (e == null) {
			e = new DynamicGroupType();
			e.setName("dg" + dynGroups.size());
			e.setSize(EnumConverter.convert(dynGroup.getScale()));
			dynGroups.put(dynGroup, e);
		}
	}
	
	private StepType buildProjectingStep(AbstractProjectingExecutionStep pes) {
		ProjectingStepType out = new ProjectingStepType();
		if (pes instanceof RedistributionExecutionStep) {
			RedistributionExecutionStep redist = (RedistributionExecutionStep) pes;
			out.setTarget(buildTargetTable(redist.getTargetTable()));
		}
		fillDML(out, pes, DMLType.PROJECTING);
		return out;
	}
	
	private StepType buildUpdateStep(DirectExecutionStep des) {
		return null;
	}
	
	private StepType buildTransactionStep(TransactionExecutionStep tes) {
		return null;
	}
	
	private void fillDML(DMLStepType dmlt, DirectExecutionStep des, DMLType action) {
		dmlt.setAction(action);
		PEStorageGroup src = des.getPEStorageGroup();
		PEStorageGroup actual = src.getPEStorageGroup(sc);
		dmlt.setSrcgrp(getGroupName(actual));
		DistributionVector dv = des.getDistributionVector();
		if (dv != null)
			dmlt.setSrcmod(EnumConverter.convert(dv.getModel()));
		GenericSQLCommand gsql = des.getRawSQL();
		dmlt.setSrcsql(parameterize(gsql));
	}
	
	private TargetTableType buildTargetTable(PETable tab) {
		TargetTableType out = new TargetTableType();
		out.setName(tab.getName(sc).getUnquotedName().get());
		out.setTemp(tab.isTempTable());
		PEStorageGroup ofGroup = tab.getStorageGroup(sc);
		PEStorageGroup actual = ofGroup.getPEStorageGroup(sc);
		out.setGroup(getGroupName(actual));
		if (tab.isTempTable())
			out.setDistvect(buildDistributionType(tab.getDistributionVector(sc)));
		return out;
	}
	
	private String getGroupName(PEStorageGroup actual) {
		if (actual.isTempGroup()) {
			DynamicGroupType dgt = dynGroups.get(actual);
			if (dgt == null)
				throw new SchemaException(Pass.PLANNER, "Internal error: unknown dynamic group");
			return dgt.getName();
		} else {
			return actual.getPersistent(sc).getName();
		}
	}
	
	DistributionType buildDistributionType(DistributionVector dvect) {
		DistributionType out = new DistributionType();
		out.setModel(EnumConverter.convert(dvect.getModel()));
		if (dvect.isRange())
			out.setRange(dvect.getRangeDistribution().getName().getUnquotedName().get());
		if (dvect.getModel().getUsesColumns()) {
			List<PEColumn> cols = dvect.getColumns(sc);
			for(int i = 0; i < cols.size(); i++) {
				DistVectColumn dvc = new DistVectColumn();
				dvc.setName(cols.get(i).getName().getUnquotedName().get());
				dvc.setPosition(i);
				out.getColumn().add(dvc);
			}
		}
		return out;
	}
	
	private String parameterize(GenericSQLCommand in) {
		Map<Integer, String> mapping = new HashMap<Integer, String>();
		for(Map.Entry<IDelegatingLiteralExpression, ParameterType> me : parameters.entrySet()) {
			mapping.put(me.getKey().getPosition(), "@" + me.getValue().getName());
		}
		GenericSQLCommand p = in.resolve(mapping, sc);
		return p.getUnresolved();
	}
}
