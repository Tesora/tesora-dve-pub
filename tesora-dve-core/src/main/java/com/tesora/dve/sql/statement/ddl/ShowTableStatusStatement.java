package com.tesora.dve.sql.statement.ddl;

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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.tesora.dve.db.Emitter;
import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.infoschema.InformationSchemaTable;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.expression.NameAlias;
import com.tesora.dve.sql.node.structural.SortingSpecification;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.DistributionKeyTemplate;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PEPersistentGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.TempTable;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.CopyVisitor;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.ProjectingExecutionStep;
import com.tesora.dve.sql.transform.execution.RedistributionExecutionStep;
import com.tesora.dve.sql.transform.strategy.TempGroupManager;
import com.tesora.dve.sql.util.BinaryProcedure;
import com.tesora.dve.sql.util.Functional;

public class ShowTableStatusStatement extends DelegatingDDLStatement {

	// ugh
	private List<TableKey> tables;
	@SuppressWarnings("unused")
	private ExpressionNode originalWhereClause;
	private ExpressionNode likeClause;
	
	public ShowTableStatusStatement(List<TableKey> tabs, ExpressionNode whereClause, 
			PEPersistentGroup execGroup, InformationSchemaTable showTable) {
		super(execGroup, showTable);
		tables = tabs;
		originalWhereClause = whereClause;
		likeClause = null;
	}

	
	public ShowTableStatusStatement(ExpressionNode likeClause,
			InformationSchemaTable showTable) {
		super(null,showTable);
		this.likeClause = likeClause;
	}
	
	@Override
	public String getSQL(SchemaContext sc, Emitter emitter, EmitOptions opts, boolean unused) {
		if (likeClause != null) {
			StringBuilder buf = new StringBuilder();
			buf.append("SHOW TABLE STATUS LIKE ");
            Singletons.require(HostService.class).getDBNative().getEmitter().emitExpression(sc, sc.getValues(),likeClause, buf, -1);
			return buf.toString();
		} else 
			return "show table status ...";
	}
	
	@Override
	public void plan(SchemaContext pc, ExecutionSequence es, BehaviorConfiguration config) throws PEException {
		Database<?> ondb = null;
		for(TableKey tk : tables) {
			if (ondb == null) ondb = tk.getTable().getDatabase(pc);
			else if (!ondb.equals(tk.getTable().getDatabase(pc)))
				throw new SchemaException(Pass.PLANNER, "Unable to show table status across databases");
		}
		String origCommand = buildInitialShowStatusCommand();
		if (onGroup.isSingleSiteGroup()) {
			es.append(ProjectingExecutionStep.build(pc, ondb, onGroup, origCommand));
		} else {
			//		System.out.println(origCommand);
			TempGroupManager tgm = new TempGroupManager(); 
			TempTable t1 = buildFirstTempTable(pc,tgm.getGroup(true));
			if (!pc.hasCurrentDatabase() && !t1.hasDatabase(pc)) {
				// if no current database is set and the temp table has no database
				// we need to set one
				t1.setDatabase(pc, (PEDatabase)ondb, false);
			}
			SelectStatement firstSelect = buildFirstSelect(pc,t1);
			DistributionKeyTemplate dkt = buildKeyTemplate();
			for(int i = 0; i < firstSelect.getProjectionEdge().size(); i++) 
				dkt.addColumn(firstSelect.getProjectionEdge().get(i), i);
			DistributionVector sourceVect = buildVector(pc);
			tgm.plan(pc);
			es.append(RedistributionExecutionStep.build(pc, ondb, onGroup, origCommand, sourceVect, t1, t1.getStorageGroup(pc), dkt, null));
			SelectStatement aggCommand = buildAggCommand(firstSelect);
			//		System.out.println(aggCommand.getSQL());
			es.append(ProjectingExecutionStep.build(pc,ondb,t1.getStorageGroup(pc),null,null,aggCommand, null));
		}
	}

	private String buildInitialShowStatusCommand() {
		StringBuilder buf = new StringBuilder();
		buf.append("SHOW TABLE STATUS WHERE Name IN (");
		Functional.join(tables, buf, ",", new BinaryProcedure<TableKey,StringBuilder>() {

			@Override
			public void execute(TableKey aobj, StringBuilder bobj) {
				bobj.append("'").append(aobj.getAbstractTable().getName().getUnquotedName().get()).append("'");				
			}
			
		});
		buf.append(")");
		return buf.toString();
	}
	
	private static final Set<String> maxedColumns = 
		new HashSet<String>(Arrays.asList(new String[] {
			"Engine", "Version", "Row_format", "Max_data_length", 
			"Auto_increment", "Update_time", "Check_time", 
			"Collation", "Create_options", "Comment"	
		}));
	
	private static final Set<String> summedColumns =
		new HashSet<String>(Arrays.asList(new String[] {
			"Rows", "Data_length", "Index_length", "Data_free",
		}));
		
	protected SelectStatement buildAggCommand(SelectStatement in) {
		SelectStatement out = CopyVisitor.copy(in);
		// we're going to build 
		// select Name, max(Engine), max(Version), max(Row_format), 
		// sum(Rows), avg(Avg_row_length), sum(Data_length), 
        // max(Max_data_length), sum(Index_length), 
        // sum(Data_free), max(Auto_increment), min(Create_time),
        // max(Update_time), max(Check_time), 
        // max(Collation), null /*Checksum*/, max(Create_options), max(Comment) from temp1 group by Name
		
		// eventually we will build another temp table of rewrite info (visible name, auto_inc values) and join
		// but for now this is good enough

		List<ExpressionNode> proj = new ArrayList<ExpressionNode>();
		ExpressionAlias nameColumn = null;
		for(ExpressionNode en : out.getProjection()) {
			ExpressionAlias ea = (ExpressionAlias) en;
			ColumnInstance ci = (ColumnInstance) ea.getTarget();
			String cn = ci.getColumn().getName().getUnquotedName().get();
			FunctionCall fc = null;
			if (maxedColumns.contains(cn)) {
				fc = new FunctionCall(FunctionName.makeMax(),ci);
			} else if (summedColumns.contains(cn)) {
				fc = new FunctionCall(FunctionName.makeSum(),ci);
			} else if ("Name".equals(cn)) {
				fc = null;
				nameColumn = ea;
			} else if ("Avg_row_length".equals(cn)) {
				fc = new FunctionCall(FunctionName.makeRound(), new FunctionCall(FunctionName.makeAvg(),ci));
			} else if ("Create_time".equals(cn)) {
				fc = new FunctionCall(FunctionName.makeMin(),ci);
			} else if ("Checksum".equals(cn)) {
				fc = null;
				ea = new ExpressionAlias(LiteralExpression.makeNullLiteral(),new NameAlias(ci.getColumn().getName().getUnqualified()),false);
			} else {
				throw new SchemaException(Pass.PLANNER, "Unknown show status column: " + cn);
			}
			if (fc != null) {
				ea = new ExpressionAlias(fc,new NameAlias(ci.getColumn().getName().getUnqualified()),false);				
			}
			proj.add(ea);
		}
		out.setProjection(proj);
		SortingSpecification ngb = new SortingSpecification(nameColumn.buildAliasInstance(),true);
		ngb.setOrdering(Boolean.FALSE);
		out.getGroupBysEdge().add(ngb);

		return out;
	}
}
