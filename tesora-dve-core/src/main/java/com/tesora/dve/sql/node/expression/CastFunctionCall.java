// OS_STATUS: public
package com.tesora.dve.sql.node.expression;


import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.parser.TokenTypes;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.transform.CopyContext;

public class CastFunctionCall extends FunctionCall {

	private Name typeName;
	private String asText;
	
	public CastFunctionCall(ExpressionNode firstArg, Name typeName) {
		this(firstArg,"CAST",typeName,"AS");
	}
	
	public CastFunctionCall(ExpressionNode firstArg, String castText, Name typeName, String asText) {
		super(new FunctionName(castText, TokenTypes.CAST, false), firstArg);
		this.typeName = typeName;
		this.asText = asText;
	}

	public Name getTypeName() {
		return typeName;
	}
	
	public String getAsText() {
		return asText;
	}
	
	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		ExpressionNode argcopy = (ExpressionNode) getParameters().get(0).copy(cc); 
		CastFunctionCall nfc = new CastFunctionCall(argcopy, getFunctionName().get(), typeName, asText);
		nfc.setSetQuantifier(getSetQuantifier());
		return nfc;
	}

	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		if (!super.schemaSelfEqual(other))
			return false;
		CastFunctionCall oth = (CastFunctionCall) other;
		return typeName.equals(oth.typeName);
	}

	@Override
	protected int selfHashCode() {
		return addSchemaHash(super.selfHashCode(),typeName.hashCode());
	}
	
}
