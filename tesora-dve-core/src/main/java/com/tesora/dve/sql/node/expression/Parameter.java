// OS_STATUS: public
package com.tesora.dve.sql.node.expression;


import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.cache.IConstantExpression;
import com.tesora.dve.sql.schema.cache.IParameter;
import com.tesora.dve.sql.schema.types.Type;
import com.tesora.dve.sql.transform.CopyContext;

public class Parameter extends ConstantExpression implements IParameter {

	protected int position;
	
	public Parameter(SourceLocation sloc) {
		super(sloc);
	}

	protected Parameter(Parameter p) {
		super(p);
		this.position = p.position;
	}
	
	@Override
	public Object getValue(SchemaContext sc) {
		return sc.getValueManager().getValue(sc, this);
	}
	
	@Override
	public Object convert(SchemaContext sc, Type type) {
        return Singletons.require(HostService.class).getDBNative().getValueConverter().convert(getValue(sc), type);
	}
	
	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		return new Parameter(this);
	}
	
	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		Parameter op = (Parameter) other;
		return position == op.position;
	}

	@Override
	protected int selfHashCode() {
		return position;
	}
	

	
	@Override
	public int getPosition() {
		return position;
	}
	
	public void setPosition(int p) {
		position = p;
	}

	@Override
	public NameAlias buildAlias(SchemaContext sc) {
		return new NameAlias(new UnqualifiedName("param"));
	}

	@Override
	public boolean isParameter() {
		return true;
	}

	@Override
	public IConstantExpression getCacheExpression() {
		return new CachedParameterExpression(position);
	}

}
