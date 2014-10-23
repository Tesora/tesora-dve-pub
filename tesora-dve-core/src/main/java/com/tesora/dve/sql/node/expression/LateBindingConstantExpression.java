package com.tesora.dve.sql.node.expression;

import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.ConstantType;
import com.tesora.dve.sql.schema.cache.IConstantExpression;
import com.tesora.dve.sql.schema.types.Type;
import com.tesora.dve.sql.transform.CopyContext;

public class LateBindingConstantExpression extends ConstantExpression {

	private final int position;
	
	public LateBindingConstantExpression(int position) {
		super((SourceLocation)null);
		this.position = position;
	}
	
	public LateBindingConstantExpression(LateBindingConstantExpression o) {
		super(o);
		this.position = o.position;
	}
	
	@Override
	public int getPosition() {
		return position;
	}

	@Override
	public ConstantType getConstantType() {
		return ConstantType.RUNTIME;
	}

	@Override
	public IConstantExpression getCacheExpression() {
		// we can be our own cache expression since we are really just an offset
		return this;
	}

	@Override
	public Object getValue(SchemaContext sc) {
		return sc._getValues().getRuntimeConstant(position);
	}

	@Override
	public Object convert(SchemaContext sc, Type type) {
		Object val = getValue(sc);
		if (val == null) return null;
        return Singletons.require(HostService.class).getDBNative().getValueConverter().convert(val, type);
	}

	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		return new LateBindingConstantExpression(this);
	}

	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		if (other instanceof LateBindingConstantExpression) {
			LateBindingConstantExpression o = (LateBindingConstantExpression) other;
			return position == o.position;
		}
		return false;
	}

	@Override
	protected int selfHashCode() {
		return position;
	}

}
