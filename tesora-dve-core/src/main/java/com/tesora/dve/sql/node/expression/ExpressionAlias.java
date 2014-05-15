// OS_STATUS: public
package com.tesora.dve.sql.node.expression;


import java.util.Collections;
import java.util.List;

import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.EdgeName;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.MigrationException;
import com.tesora.dve.sql.node.SingleEdge;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.TempColumn;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.types.TempColumnType;
import com.tesora.dve.sql.transform.CopyContext;
import com.tesora.dve.sql.util.IsInstance;

public class ExpressionAlias extends ExpressionNode {

	public static final IsInstance<ExpressionNode> instanceTest = new IsInstance<ExpressionNode>(ExpressionAlias.class);
	
	protected SingleEdge<ExpressionAlias,ExpressionNode> target = 
		new SingleEdge<ExpressionAlias, ExpressionNode>(ExpressionAlias.class, this, EdgeName.EXPRESSION_ALIAS_TARGET);
	
	private Alias alias;
	private boolean synthetic;
	
	public ExpressionAlias(ExpressionNode t, Alias n, boolean synth) {
		this(t,n,null);
		synthetic = synth;
	}
	
	public ExpressionAlias(ExpressionNode t, Alias n, SourceLocation orig) {
		super(orig);
		target.set(t);
		this.alias = n;
		synthetic = (orig == null);
	}
	
	public AliasInstance buildAliasInstance() {
		return new AliasInstance(this);
	}
	
	public ExpressionNode getTarget() { return target.get(); }
	public Alias getAlias() { return alias; }
	
	public boolean isSynthetic() {
		return !(getSourceLocation() != null || !synthetic);
	}
	
	// sometimes we need to reset the alias
	// i.e. for redist back into user tables
	public void setAlias(UnqualifiedName unq) {
		alias = new NameAlias(unq);
	}

	// used in rewrites
    public PEColumn buildTempColumn(SchemaContext pc) {
        return buildTempColumn(pc,TempColumnType.TEMP_TYPE);
    }

	// used in rewrites
	public PEColumn buildTempColumn(SchemaContext pc, TempColumnType typeIfUndefined) {
		TempColumnType tt = null;
		if (target.get() instanceof ColumnInstance) {
			ColumnInstance ci = (ColumnInstance) target.get();
			tt = new TempColumnType(ci.getColumn().getType());
		} else {
			tt = typeIfUndefined;
		}
		if (!getAlias().isName())
			throw new SchemaException(Pass.PLANNER, "Invalid source name for temp column: found string alias but require name alias");
		return new TempColumn(pc,getAlias().getNameAlias(),tt);
	}

	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		ExpressionAlias orig = this;
		ExpressionAlias repl = null;
		if (!synthetic && getSourceLocation() == null)
			repl = new ExpressionAlias((ExpressionNode)target.get().copy(cc), alias, false);
		else
			repl = new ExpressionAlias((ExpressionNode)target.get().copy(cc), alias, getSourceLocation());
		if (cc == null) throw new MigrationException("where's my context?");
		return cc.putExpressionAlias(orig, repl);
	}

	@Override
	public NameAlias buildAlias(SchemaContext sc) {
		return alias.asNameAlias();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends Edge<?,?>> List<T> getEdges() {
		return (List<T>) Collections.singletonList(target);
	}

	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		return true;
	}

	@Override
	protected int selfHashCode() {
		return 0;
	}

}
