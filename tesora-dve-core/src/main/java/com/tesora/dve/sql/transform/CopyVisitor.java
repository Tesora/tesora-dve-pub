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


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.tesora.dve.sql.expression.Visitor;
import com.tesora.dve.sql.expression.VisitorContext;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.TableJoin;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.node.structural.JoinedTable;
import com.tesora.dve.sql.node.structural.LimitSpecification;
import com.tesora.dve.sql.node.structural.SortingSpecification;
import com.tesora.dve.sql.statement.dml.AliasInformation;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.DeleteStatement;
import com.tesora.dve.sql.statement.dml.InsertIntoSelectStatement;
import com.tesora.dve.sql.statement.dml.InsertIntoValuesStatement;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.statement.dml.ReplaceIntoSelectStatement;
import com.tesora.dve.sql.statement.dml.ReplaceIntoValuesStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.statement.dml.TruncateStatement;
import com.tesora.dve.sql.statement.dml.UnionStatement;
import com.tesora.dve.sql.statement.dml.UpdateStatement;

public final class CopyVisitor extends Visitor {

	private final CopyContext forwarding;
	
	private CopyVisitor(CopyContext cc) {
		forwarding = cc;
	}

	@Override
	public VisitorContext newContext() {
		return forwarding;
	}
	
	@SuppressWarnings("unchecked")
	public static <S extends DMLStatement> S copy(S in) {
		CopyContext cc = new CopyContext("CopyVisitor.copy(DMLStatement)");
		CopyVisitor cv = new CopyVisitor(cc);
		S out = (S) cv.visit(in);
		SchemaMapper sm = new SchemaMapper(Collections.singleton((DMLStatement)in), out, cc);
		out.setMapper(sm);
		return out;
	}

	@SuppressWarnings("unchecked")
	public static <T extends LanguageNode> T copy(T in, CopyContext cc) {
		CopyVisitor cv = new CopyVisitor(cc);
		return (T) cv.visit(in);
	}

	@SuppressWarnings("unchecked")
	public static <T extends ProjectingStatement> T copy(T in, CopyContext cc) {
		CopyVisitor cv = new CopyVisitor(cc);
		T out = (T) cv.visit(in);
		boolean was = cc.isFixed();
		SchemaMapper sm = new SchemaMapper(Collections.singleton((DMLStatement)in), out, cc);
		cc.setFixed(was);
		out.setMapper(sm);
		return out;
	}
	
	@Override
	public <I extends LanguageNode> List<I> visit(List<I> l, VisitorContext vc) {
		if (l == null) return l;
		ArrayList<I> out = new ArrayList<I>();
		for(I t : l)
			out.add(visit(t, vc));
		return out;
	}

	@Override
	public ExpressionNode visitExpression(ExpressionNode in, VisitorContext vc) {
		CopyContext cvc = (CopyContext) vc;
		return (ExpressionNode)in.copy(cvc);
	}
	
	@Override
	public SelectStatement visitSelectStatement(SelectStatement ss, VisitorContext vc) {
		CopyContext cc = (CopyContext) vc;
		cc.pushScope();
		SelectStatement nss =  
			new SelectStatement(
					visit(ss.getTables(),vc),
					visit(ss.getProjection(),vc),
					visit(ss.getWhereClause(),vc),
					visit(ss.getOrderBys(),vc),
					visit(ss.getLimit(),vc),
					ss.getSetQuantifier(),
					ss.getSelectOptions(),
					visit(ss.getGroupBys(),vc),
					visit(ss.getHavingExpression(),vc),
					ss.isLocking(),
					new AliasInformation(ss.getAliases()),
					ss.getSourceLocation());
		nss.setGrouped(ss.isGrouped());
		nss.getDerivedInfo().copyTake(ss.getDerivedInfo());
		nss.getDerivedInfo().takeCopy(cc);
		cc.popScope();
		cc.registerProjectingStatement(nss);
		return nss;
	}
	
	@Override
	public UpdateStatement visitUpdateStatement(UpdateStatement in, VisitorContext vc) {
		CopyContext cc = (CopyContext) vc;
		cc.pushScope();
		UpdateStatement us = 
			new UpdateStatement(visit(in.getTables(),vc),
					visit(in.getUpdateExpressions(),vc),
					visit(in.getWhereClause(),vc),
					visit(in.getOrderBys(),vc),
					visit(in.getLimit(),vc),
					new AliasInformation(in.getAliases()),
					in.getSourceLocation());
		us.getDerivedInfo().copyTake(in.getDerivedInfo());
		us.getDerivedInfo().takeCopy(cc);
		cc.popScope();
		return us;
	}
	
	@Override
	public DeleteStatement visitDeleteStatement(DeleteStatement in, VisitorContext vc) {
		CopyContext cc = (CopyContext) vc;
		cc.pushScope();
		DeleteStatement ds =
			new DeleteStatement(visit(in.getTargetDeletes(),vc),
					visit(in.getTables(),vc),
					visit(in.getWhereClause(),vc),
					visit(in.getOrderBys(),vc),
					visit(in.getLimit(),vc),
					in.isTruncate(),
					new AliasInformation(),
					in.getSourceLocation());
		ds.getDerivedInfo().copyTake(in.getDerivedInfo());
		ds.getDerivedInfo().takeCopy(cc);
		cc.popScope();
		return ds;
	}
	
	@Override
	public InsertIntoValuesStatement visitInsertIntoValuesStatement(InsertIntoValuesStatement is, VisitorContext vc) {
		List<List<ExpressionNode>> values = new ArrayList<List<ExpressionNode>>();
		for(List<ExpressionNode> row : is.getValues()) {
			List<ExpressionNode> rowCopy = visit(row, vc);
			values.add(rowCopy);
		}
		InsertIntoValuesStatement nis = new InsertIntoValuesStatement(visit(is.getTableInstance(),vc),
				visit(is.getColumnSpecification(),vc),
				values,
				visit(is.getOnDuplicateKey(),vc),
				new AliasInformation(),
				is.getSourceLocation());			
		nis.getDerivedInfo().copyTake(is.getDerivedInfo());
		return nis;
	}

	@Override
	public ReplaceIntoValuesStatement visitReplaceIntoValuesStatement(ReplaceIntoValuesStatement is, VisitorContext vc) {
		List<List<ExpressionNode>> values = new ArrayList<List<ExpressionNode>>();
		for(List<ExpressionNode> row : is.getValues()) {
			List<ExpressionNode> rowCopy = visit(row, vc);
			values.add(rowCopy);
		}
		ReplaceIntoValuesStatement nis = new ReplaceIntoValuesStatement(visit(is.getTableInstance(),vc),
				visit(is.getColumnSpecification(),vc),
				values,
				new AliasInformation(),
				is.getSourceLocation());			
		nis.getDerivedInfo().copyTake(is.getDerivedInfo());
		return nis;		
	}
	
	@Override
	public InsertIntoSelectStatement visitInsertIntoSelectStatement(InsertIntoSelectStatement iiss, VisitorContext vc) {
		CopyContext cc = (CopyContext) vc;
		cc.pushScope();
		InsertIntoSelectStatement ns = 
			new InsertIntoSelectStatement(visit(iiss.getTableInstance(),vc),
					visit(iiss.getColumnSpecification(),vc),
					visit(iiss.getSource(),vc),
					iiss.isNestedGrouped(),
					visit(iiss.getOnDuplicateKey(),vc),
					new AliasInformation(),
					iiss.getSourceLocation());
		ns.getDerivedInfo().copyTake(iiss.getDerivedInfo());
		ns.getDerivedInfo().takeCopy(cc);
		cc.popScope();
		return ns;
	}
	
	@Override
	public ReplaceIntoSelectStatement visitReplaceIntoSelectStatement(ReplaceIntoSelectStatement iiss, VisitorContext vc) {
		CopyContext cc = (CopyContext) vc;
		cc.pushScope();
		ReplaceIntoSelectStatement ns = 
			new ReplaceIntoSelectStatement(visit(iiss.getTableInstance(),vc),
					visit(iiss.getColumnSpecification(),vc),
					visit(iiss.getSource(),vc),
					iiss.isNestedGrouped(),
					new AliasInformation(),
					iiss.getSourceLocation());
		ns.getDerivedInfo().copyTake(iiss.getDerivedInfo());
		ns.getDerivedInfo().takeCopy(cc);
		cc.popScope();
		return ns;		
	}
	
	public TruncateStatement visitTruncateStatement(TruncateStatement ts, VisitorContext vc) {
		return new TruncateStatement(visit(ts.getTruncatedTable(), vc), ts.getSourceLocation());
	}
		
	@Override
	public UnionStatement visitUnionStatement(UnionStatement us, VisitorContext vc) {
		CopyContext cc = (CopyContext) vc;
		cc.pushScope();
		UnionStatement nus = new UnionStatement(visit(us.getFromEdge().get(), vc),
				visit(us.getToEdge().get(), vc),
				us.isUnionAll(),us.getSourceLocation());
		nus.setOrderBy(visit(us.getOrderBys(), vc));
		nus.setLimit(visit(us.getLimit(),vc));
		nus.setGrouped(us.isGrouped());
		nus.getDerivedInfo().copyTake(us.getDerivedInfo());
		nus.getDerivedInfo().takeCopy(cc);
		cc.popScope();
		cc.registerProjectingStatement(nus);		
		return nus;
	}
	
	@Override
	public FromTableReference visitFromTableReference(FromTableReference in, VisitorContext vc) {
		return new FromTableReference(visit(in.getTarget(),vc));
	}
	
	@Override
	public TableJoin visitTableJoin(TableJoin tj, VisitorContext vc) {
		return new TableJoin(visit(tj.getFactor(),vc),
				visit(tj.getJoins(),vc));
	}
	
	@Override
	public JoinedTable visitJoinedTable(JoinedTable in, VisitorContext vc) {
		return 
			new JoinedTable(visit(in.getJoinedTo(),vc),
					visit(in.getJoinOn(),vc),
					in.getJoinType());
	}
	
	@Override
	public LimitSpecification visitLimitSpecification(LimitSpecification in, VisitorContext vc) {
		return 
			new LimitSpecification(visit(in.getRowcount(),vc),visit(in.getOffset(),vc));
	}
	
	@Override
	public SortingSpecification visitSortingSpecification(SortingSpecification in, VisitorContext vc) {
		return
			new SortingSpecification(visit(in.getTarget(),vc),in.isAscending());
	}
	
}
