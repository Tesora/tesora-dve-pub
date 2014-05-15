// OS_STATUS: public
package com.tesora.dve.sql.expression;


import java.util.List;

import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.StructuralNode;
import com.tesora.dve.sql.node.expression.AliasInstance;
import com.tesora.dve.sql.node.expression.CaseExpression;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.Default;
import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.ExpressionSet;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.expression.NameInstance;
import com.tesora.dve.sql.node.expression.Parameter;
import com.tesora.dve.sql.node.expression.Subquery;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.expression.TableJoin;
import com.tesora.dve.sql.node.expression.VariableInstance;
import com.tesora.dve.sql.node.expression.WhenClause;
import com.tesora.dve.sql.node.expression.Wildcard;
import com.tesora.dve.sql.node.expression.WildcardTable;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.node.structural.JoinedTable;
import com.tesora.dve.sql.node.structural.LimitSpecification;
import com.tesora.dve.sql.node.structural.SortingSpecification;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.ddl.DDLStatement;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.DeleteStatement;
import com.tesora.dve.sql.statement.dml.InsertIntoSelectStatement;
import com.tesora.dve.sql.statement.dml.InsertIntoValuesStatement;
import com.tesora.dve.sql.statement.dml.InsertStatement;
import com.tesora.dve.sql.statement.dml.ReplaceIntoSelectStatement;
import com.tesora.dve.sql.statement.dml.ReplaceIntoValuesStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.statement.dml.TruncateStatement;
import com.tesora.dve.sql.statement.dml.UnionStatement;
import com.tesora.dve.sql.statement.dml.UpdateStatement;
import com.tesora.dve.sql.statement.session.SessionStatement;

public class Visitor {

	public VisitorContext newContext() {
		return new VisitorContext();
	}
	
	public LanguageNode visit(LanguageNode t) {
		return visit(t, newContext());
	}

	public <T extends LanguageNode> T visit(T in, VisitorContext context) {
		if (in == null)
			return null;
		context.push(in);
		T out = visitNode(in, context);
		context.pop();
		return out;
	}
	
	@SuppressWarnings("unchecked")
	public <T extends LanguageNode> T visitNode(T in, VisitorContext context) {
		LanguageNode out = null;
		if (in instanceof ExpressionNode) {
			out = visitExpression((ExpressionNode)in, context);
		} else if (in instanceof DMLStatement) {
			out = visitStatement((DMLStatement)in, context);
		} else if (in instanceof StructuralNode) {
			out = visitStructural((StructuralNode)in, context);
		} else if (in instanceof SessionStatement) {
			out = visitSessionStatement((SessionStatement)in,context);
		} else if (in instanceof DDLStatement) {
			out = visitDDLStatement((DDLStatement)in,context);
		} else {
			throw new IllegalArgumentException("Cannot visit: " + in.getClass().getName());
		}
		return (T)out;
	}
	
	public ExpressionNode visitExpression(ExpressionNode in, VisitorContext context) {
		ExpressionNode out = null;
		if (in instanceof ColumnInstance) {
			out = visitColumnInstance((ColumnInstance)in, context);
		} else if (in instanceof FunctionCall) {
			out = visitFunctionCall((FunctionCall)in, context);
		} else if (in instanceof LiteralExpression) {
			out = visitLiteralExpression((LiteralExpression)in, context);
		} else if (in instanceof ExpressionAlias) {
			out = visitDerivedColumn((ExpressionAlias)in, context);
		} else if (in instanceof TableInstance) {
			out = visitTableInstance((TableInstance)in, context);
		} else if (in instanceof TableJoin) {
			out = visitTableJoin((TableJoin)in, context);
		} else if (in instanceof Wildcard) {
			out = visitWildcard((Wildcard)in, context);
		} else if (in instanceof WildcardTable) {
			out = visitWildcardTable((WildcardTable)in, context);
		} else if (in instanceof Default) {
			out = visitDefault((Default)in, context);
		} else if (in instanceof VariableInstance) {
			out = visitVariable((VariableInstance)in, context);
		} else if (in instanceof Subquery) {
			out = visitSubquery((Subquery)in, context);
		} else if (in instanceof AliasInstance) {
			out = visitAliasInstance((AliasInstance)in, context);
		} else if (in instanceof Parameter) {
			out = visitParameter((Parameter)in, context);
		} else if (in instanceof ExpressionSet) {
			out = visitExpressionSet((ExpressionSet) in, context);
		} else if (in instanceof CaseExpression) {
			out = visitCaseExpression((CaseExpression)in, context);
		} else if (in instanceof WhenClause) {
			out = visitWhenClause((WhenClause)in, context);
		} else if (in instanceof NameInstance) {
			out = visitNameInstance((NameInstance)in, context);
		} else {
			throw new IllegalArgumentException("Unknown expression kind for visitor: " + in.getClass().getName());
		}
		return out;
	}
		
	public ExpressionNode visitColumnInstance(ColumnInstance cr, VisitorContext vc) {
		return cr;
	}
	
	public ExpressionNode visitNameInstance(NameInstance ni, VisitorContext vc) {
		return ni;
	}
	
	public ExpressionNode visitAliasInstance(AliasInstance ai, VisitorContext vc) {
		visit(ai.getTarget(),vc);
		return ai;
	}
	
	public ExpressionNode visitParameter(Parameter in, VisitorContext vc) {
		return in;
	}
	
	public ExpressionNode visitExpressionSet(ExpressionSet set, VisitorContext vc) {
		for (final ExpressionNode value : set.getSubExpressions()) {
			visit(value, vc);
		}
		return set;
	}

	public ExpressionNode visitCaseExpression(CaseExpression ce, VisitorContext vc) {
		visit(ce.getTestExpression(),vc);
		visit(ce.getElseExpression(),vc);
		visit(ce.getWhenClauses(),vc);
		return ce;
	}
	
	public ExpressionNode visitWhenClause(WhenClause in, VisitorContext context) {
		visit(in.getTestExpression(),context);
		visit(in.getResultExpression(),context);
		return in;
	}

	public ExpressionNode visitFunctionCall(FunctionCall fc, VisitorContext vc) {
		visit(fc.getParameters(), vc);
		return fc;
	}
	
	public ExpressionNode visitLiteralExpression(LiteralExpression le, VisitorContext vc) {
		return le;
	}
	
	public ExpressionNode visitDerivedColumn(ExpressionAlias dc, VisitorContext vc) {
		visit(dc.getTarget(), vc);
		return dc;
	}
	
	public ExpressionNode visitTableInstance(TableInstance tr, VisitorContext vc) {
		return tr;
	}
	
	public ExpressionNode visitTableJoin(TableJoin tj, VisitorContext vc) {
		visit(tj.getFactor(),vc);
		visit(tj.getJoins(),vc);
		return tj;
	}
	
	public ExpressionNode visitWildcard(Wildcard wc, VisitorContext vc) {
		return wc;
	}
	
	public ExpressionNode visitWildcardTable(WildcardTable wct, VisitorContext vc) {
		return wct;
	}
	
	public ExpressionNode visitDefault(Default def, VisitorContext vc) {
		return def;
	}
	
	public ExpressionNode visitVariable(VariableInstance v, VisitorContext vc) {
		return v;
	}
	
	public ExpressionNode visitSubquery(Subquery sq, VisitorContext vc) {
		visit(sq.getStatement(),vc);
		return sq;
	}
	
	public SessionStatement visitSessionStatement(SessionStatement in, VisitorContext context) {
		return in;
	}
	
	public DDLStatement visitDDLStatement(DDLStatement in, VisitorContext context) {
		return in;
	}
	
	public Statement visitStatement(Statement in, VisitorContext context) {
		Statement out = null;
		if (in instanceof SelectStatement) {
			out = visitSelectStatement((SelectStatement)in, context);
		} else if (in instanceof UpdateStatement) {
			out = visitUpdateStatement((UpdateStatement)in, context);
		} else if (in instanceof InsertStatement) {
			out = visitInsertStatement((InsertStatement)in, context);
		} else if (in instanceof DeleteStatement) {
			out = visitDeleteStatement((DeleteStatement)in, context);
		} else if (in instanceof TruncateStatement) {
			out = visitTruncateStatement((TruncateStatement)in, context);
		} else if (in instanceof UnionStatement) {
			out = visitUnionStatement((UnionStatement)in, context);
		} else if (in instanceof DMLStatement) {
			throw new IllegalArgumentException("Unknown DML statement: " + in.getClass().getName());
		} else {
			throw new IllegalArgumentException("Visitor does not support nondml statements: " + in.getClass().getName());
		}
		return out;
	}
	
	public SelectStatement visitSelectStatement(SelectStatement ss, VisitorContext vc) {
		visit(ss.getTables(), vc);
		visit(ss.getProjection(), vc);
		visit(ss.getWhereClause(), vc);
		visit(ss.getGroupBys(), vc);
		visit(ss.getOrderBys(), vc);
		return ss;
	}
	
	public UpdateStatement visitUpdateStatement(UpdateStatement in, VisitorContext vc) {
		visit(in.getTables(), vc);
		visit(in.getUpdateExpressions(), vc);
		visit(in.getWhereClause(), vc);
		visit(in.getOrderBys(), vc);
		return in;
	}
	
	public DeleteStatement visitDeleteStatement(DeleteStatement in, VisitorContext vc) {
		visit(in.getTables(), vc);
		visit(in.getWhereClause(), vc);
		return in;
	}
	
	public InsertStatement visitInsertStatement(InsertStatement is, VisitorContext vc) {
		if (is.isReplace()) {
			if (is instanceof ReplaceIntoValuesStatement)
				return visitReplaceIntoValuesStatement((ReplaceIntoValuesStatement)is, vc);
			else
				return visitReplaceIntoSelectStatement((ReplaceIntoSelectStatement)is, vc);
		} else {
			if (is instanceof InsertIntoValuesStatement) {
				return visitInsertIntoValuesStatement((InsertIntoValuesStatement)is, vc);
			} else {
				return visitInsertIntoSelectStatement((InsertIntoSelectStatement)is, vc);
			}
		}
	}
	
	public InsertIntoValuesStatement visitInsertIntoValuesStatement(InsertIntoValuesStatement is, VisitorContext vc) {
		visit(is.getTableInstance(), vc);
		visit(is.getColumnSpecification(), vc);
		for(List<ExpressionNode> v: is.getValues())
			visit(v, vc);
		return is;
	}
	
	public InsertIntoSelectStatement visitInsertIntoSelectStatement(InsertIntoSelectStatement iiss, VisitorContext vc) {
		visit(iiss.getTableInstance(), vc);
		visit(iiss.getColumnSpecification(), vc);
		return iiss;
	}
	
	public ReplaceIntoValuesStatement visitReplaceIntoValuesStatement(ReplaceIntoValuesStatement is, VisitorContext vc) {
		visit(is.getTableInstance(), vc);
		visit(is.getColumnSpecification(), vc);
		for(List<ExpressionNode> v: is.getValues())
			visit(v, vc);
		return is;		
	}
	
	public ReplaceIntoSelectStatement visitReplaceIntoSelectStatement(ReplaceIntoSelectStatement iiss, VisitorContext vc) {
		visit(iiss.getTableInstance(), vc);
		visit(iiss.getColumnSpecification(), vc);
		return iiss;		
	}
	
	public UnionStatement visitUnionStatement(UnionStatement us, VisitorContext vc) {
		visit(us.getFromEdge().get(), vc);
		visit(us.getToEdge().get(), vc);
		return us;
	}
	
	public TruncateStatement visitTruncateStatement(TruncateStatement ts, VisitorContext vc) {
		visit(ts.getTruncatedTable(), vc);
		return ts;
	}
	
	public FromTableReference visitFromTableReference(FromTableReference ftr, VisitorContext vc) {
		if (ftr.getBaseTable() != null)
			visitTableInstance(ftr.getBaseTable(), vc);
		else
			visit(ftr.getBaseSubquery(), vc);
		visit(ftr.getTableJoins(), vc);
		return ftr;
	}
	
	public JoinedTable visitJoinedTable(JoinedTable jt, VisitorContext vc) {
		visit(jt.getJoinedTo(), vc);
		visit(jt.getJoinOn(), vc);
		return jt;
	}
	
	public LimitSpecification visitLimitSpecification(LimitSpecification ls, VisitorContext vc) {
		visit(ls.getOffset(), vc);
		visit(ls.getRowcount(), vc);
		return ls;
	}
	
	public SortingSpecification visitSortingSpecification(SortingSpecification ss, VisitorContext vc) {
		visit(ss.getTarget(), vc);
		return ss;
	}
	
	public <I extends LanguageNode> List<I> visit(List<I> l, VisitorContext vc) {
		if (l == null)
			return null;
		for(LanguageNode t : l)
			visit(t, vc);
		return l;
	}	
	
	public StructuralNode visitStructural(StructuralNode s, VisitorContext vc) {
		if (s instanceof FromTableReference) {
			return visitFromTableReference((FromTableReference)s, vc);
		} else if (s instanceof JoinedTable) {
			return visitJoinedTable((JoinedTable)s, vc);
		} else if (s instanceof LimitSpecification) {
			return visitLimitSpecification((LimitSpecification)s, vc);
		} else if (s instanceof SortingSpecification) {
			return visitSortingSpecification((SortingSpecification)s, vc);
		} else {
			throw new IllegalArgumentException("Unknown structural kind: " + s.getClass().getName());
		}
	}
}
