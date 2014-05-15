// OS_STATUS: public
package com.tesora.dve.sql.node.expression;

import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.cache.IDelegatingLiteralExpression;
import com.tesora.dve.sql.schema.cache.ILiteralExpression;
import com.tesora.dve.sql.transform.CopyContext;

public class DelegatingLiteralExpression extends LiteralExpression implements IDelegatingLiteralExpression {

	protected int position;
	protected ValueSource source;
	protected Boolean hasValue = null;
	
	public  DelegatingLiteralExpression(int tt, SourceLocation sloc, ValueSource vs, int position, UnqualifiedName charsetHint) {
		super(tt,sloc,charsetHint);
		this.position = position;
		source = vs;
		if (source == null) throw new IllegalStateException("should be created with source");
		if (sloc != null && sloc.getPositionInLine() == -1) throw new IllegalStateException("Invalid source position");
	}

	protected DelegatingLiteralExpression(DelegatingLiteralExpression dle) {
		super(dle);
		position = dle.position;
		source = dle.source;
	}
	
	@Override
	public Object getValue(SchemaContext sc) {
		if (source != null) return source.getLiteral(sc, this);
		Object v = sc.getValueManager().getLiteral(sc, this);
		if (hasValue == null)
			hasValue = v != null;
		else if (v == null && hasValue.booleanValue())
			throw new IllegalStateException("mismatch");
		return v;
	}
	
	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		return new DelegatingLiteralExpression(this);
	}
	
	@Override
	public int getPosition() {
		return position;
	}
	
	public void setPosition(int p, boolean clearSource) {
		position = p;
		if (clearSource)
			source = null;
	}

	@Override
	public boolean isParameter() {
		return false;
	}
	
	@Override
	public ILiteralExpression getCacheExpression() {
		if (source == null)
			return new CachedDelegatingLiteralExpression(getValueType(), position, getCharsetHint());
		return this;
	}
	
	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		if (!super.schemaSelfEqual(other))
			return false;
		DelegatingLiteralExpression odle = (DelegatingLiteralExpression) other;
		return position == odle.position;
	}

	@Override
	protected int selfHashCode() {
		return addSchemaHash(super.selfHashCode(),position);
	}

}
