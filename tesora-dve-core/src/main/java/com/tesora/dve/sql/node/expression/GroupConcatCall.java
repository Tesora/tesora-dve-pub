// OS_STATUS: public
package com.tesora.dve.sql.node.expression;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.ObjectUtils;

import com.tesora.dve.sql.expression.SetQuantifier;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.transform.CopyContext;

public class GroupConcatCall extends FunctionCall {

	private String separator;
	
	public GroupConcatCall(FunctionName funName, List<ExpressionNode> params,
			SetQuantifier quant, String separator) {
		super(funName, params, quant, null);
		this.separator = separator;
	}

	public String getSeparator() {
		return separator;
	}
	
	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		ArrayList<ExpressionNode> np = new ArrayList<ExpressionNode>();
		for(ExpressionNode p : getParameters()) {
			np.add((ExpressionNode)p.copy(cc));
		}
		GroupConcatCall gcc = new GroupConcatCall(getFunctionName(),np,getSetQuantifier(),separator);
		return gcc;
	}

	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		if (!super.schemaSelfEqual(other))
			return false;
		GroupConcatCall gcc = (GroupConcatCall) other;
		return ObjectUtils.equals(separator, gcc.separator);
	}

	@Override
	protected int selfHashCode() {
		return addSchemaHash(super.selfHashCode(),(separator == null ? 0 : separator.hashCode()));
	}



}
