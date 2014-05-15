// OS_STATUS: public
package com.tesora.dve.sql.node.expression;

import java.util.Collections;
import java.util.List;

import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.EdgeName;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.SingleEdge;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.CopyContext;

public class AliasInstance extends ExpressionNode {

	protected Name specified;
	protected SingleEdge<AliasInstance,ExpressionAlias> target = 
		new SingleEdge<AliasInstance,ExpressionAlias>(AliasInstance.class, this, EdgeName.ALIAS_REFERENCE,false);
	
	public AliasInstance(ExpressionAlias targ, SourceLocation orig) {
		super(orig);
		if (targ == null)
			throw new SchemaException(Pass.PLANNER, "Invalid alias instance: null target");
		target.set(targ);
	}
	
	public AliasInstance(ExpressionAlias targ) {
		super((SourceLocation)null);
		if (targ == null)
			throw new SchemaException(Pass.PLANNER, "Invalid alias instance: null target");
		target.set(targ);
	}
	
	public ExpressionAlias getTarget() { return target.get(); }
	
	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		if (cc == null) {
			return new AliasInstance(target.get(), getSourceLocation());
		} else {
			ExpressionAlias orig = target.get();
			ExpressionAlias repl = cc.getExpressionAlias(orig);
			if (repl == null)
				repl = (ExpressionAlias)orig.copy(cc);
			return new AliasInstance(repl, getSourceLocation());
		}
	}

	@Override
	public NameAlias buildAlias(SchemaContext sc) {
		return target.has() ? target.get().getAlias().asNameAlias() : null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<? extends Edge<?,?>> getEdges() {
		return Collections.singletonList(target);
	}

	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		AliasInstance oai = (AliasInstance) other;
		return (specified == null && oai == null) ||
				(specified != null && oai != null && specified.equals(oai));
	}

	@Override
	protected int selfHashCode() {
		return (specified == null ? 0 : specified.hashCode());
	}
}
