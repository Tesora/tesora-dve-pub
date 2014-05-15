// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.joinsimplification;


import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.Traversal;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.UnaryFunction;

public class NRBuilder extends Traversal {

	private HashMap<ExpressionNode,NRNode> lookup;
	private List<ExpressionNode> roots;
	private SchemaContext sc;
	private boolean unsupported = false;
	private boolean doSimpTest;
	
	public NRBuilder(SchemaContext sc, boolean simpTest, ExpressionNode root) {
		super(Order.POSTORDER, ExecStyle.ONCE);
		lookup = new HashMap<ExpressionNode,NRNode>();
		doSimpTest = simpTest;
		this.sc = sc;
		ExpressionNode newRoot = (ExpressionNode) traverse(root);
		roots = ExpressionUtils.decomposeAndClause(newRoot);
	}
	
	public NRBuilder(SchemaContext sc, boolean simpTest, List<ExpressionNode> roots) {
		super(Order.POSTORDER, ExecStyle.ONCE);
		lookup = new HashMap<ExpressionNode,NRNode>();
		doSimpTest = simpTest;
		this.roots = roots;
		this.sc = sc;
		for(ExpressionNode en : roots) {
			if (unsupported) return;
			traverse(en);
		}
	}
	
	public boolean isUnsupported() {
		return unsupported;
	}
	
	public boolean allow(Edge<?,?> e) {
		return !unsupported;
	}
	
	public boolean allow(LanguageNode ln) {
		return !unsupported;
	}
	
	private List<NRNode> findChildren(FunctionCall fc) {
		return Functional.apply(fc.getParameters(), new UnaryFunction<NRNode,ExpressionNode>() {

			@Override
			public NRNode evaluate(ExpressionNode object) {
				NRNode n = lookup.get(object);
				if (n == null)
					throw new SchemaException(Pass.PLANNER, "missing null reject node");
				return n;
			}
			
		});
	}
	
	
	
	@Override
	public LanguageNode action(LanguageNode in) {
		if (unsupported) return in;
		if (in instanceof ColumnInstance) {
			ColumnInstance ci = (ColumnInstance) in;
			SimpleNode sn = new SimpleNode(ci);
			lookup.put(ci, sn);
		} else if (in instanceof LiteralExpression) {
			LiteralExpression litex = (LiteralExpression) in;
			SimpleNode sn = new SimpleNode(litex);
			lookup.put(litex, sn);
		} else if (in instanceof FunctionCall) {
			FunctionCall fc = (FunctionCall) in;
			FunctionNode fn = new FunctionNode(sc,fc,findChildren(fc));
			if (doSimpTest) {
				ExpressionNode repl = fn.getSimplifiedValue();
				if (repl != null) {
					if (repl instanceof FunctionCall) {
						fc = (FunctionCall) repl;
						// fc is already wrapped
						return fc;
					} else {
						SimpleNode sn = new SimpleNode(repl);
						lookup.put(repl, sn);
					}
					return repl;
				}
			}
			lookup.put(fc,fn);
		} else {
			unsupported = true;
		}
		return in;
	}

	public List<TableKey> required(Collection<TableKey> ojTabs) {
		ListSet<TableKey> buf = new ListSet<TableKey>(ojTabs);
		ArrayList<TableKey> out = new ArrayList<TableKey>();
		for(Iterator<TableKey> iter = buf.iterator(); iter.hasNext();) {
			TableKey tk = iter.next();
			for(ExpressionNode en : roots) {
				NRNode nrn = lookup.get(en);
				if (nrn.required(tk)) {
					out.add(tk);
					iter.remove();
					break;
				}
			}
		}
		return out;
		
	}
	
}
