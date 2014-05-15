// OS_STATUS: public
package com.tesora.dve.sql.node.expression;

import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.transform.CopyContext;

public class ConvertFunctionCall extends FunctionCall {

	private final Name name;
	private final boolean transcoding;
	
	public ConvertFunctionCall(ExpressionNode expr, Name rhs, boolean transcoding) {
		super(FunctionName.makeConvert(),expr);
		this.name = rhs;
		this.transcoding = transcoding;
	}
	
	public boolean isTranscoding() {
		return transcoding;
	}
	
	public Name getTypeName() {
		return name;
	}
	
	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		ExpressionNode argcopy = (ExpressionNode) getParameters().get(0).copy(cc); 
		ConvertFunctionCall nfc = new ConvertFunctionCall(argcopy, name, transcoding);
		nfc.setSetQuantifier(getSetQuantifier());
		return nfc;
	}

	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		if (!super.schemaSelfEqual(other))
			return false;
		ConvertFunctionCall ocfc = (ConvertFunctionCall) other;
		return this.name.equals(ocfc.name) && this.transcoding == ocfc.transcoding;
	}

	@Override
	protected int selfHashCode() {
		return addSchemaHash(addSchemaHash(super.selfHashCode(),name.hashCode()),transcoding);
	}	
}
