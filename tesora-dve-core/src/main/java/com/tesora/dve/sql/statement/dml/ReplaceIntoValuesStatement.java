// OS_STATUS: public
package com.tesora.dve.sql.statement.dml;

import java.util.List;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.node.MultiMultiEdge;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.Table;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.strategy.InformationSchemaRewriteTransformFactory;
import com.tesora.dve.sql.transform.strategy.ReplaceIntoTransformFactory;
import com.tesora.dve.sql.transform.strategy.SessionRewriteTransformFactory;
import com.tesora.dve.sql.transform.strategy.TransformFactory;

public class ReplaceIntoValuesStatement extends InsertIntoValuesStatement {

	public ReplaceIntoValuesStatement(TableInstance table, List<ExpressionNode> columns,
			List<List<ExpressionNode>> values, AliasInformation aliasInfo, SourceLocation loc) {
		super(table, columns, values, null, aliasInfo, loc);
	}

	@Override
	public boolean isReplace() {
		return true;
	}
	
	@Override
	public void plan(SchemaContext pc, ExecutionSequence ges) throws PEException {
		Table<?> table = getPrimaryTable().getTable();
		if (table.isInfoSchema()) 
			throw new PEException("Cannot insert into info schema table " + intoTable.get().getTable().getName());
		PEAbstractTable<?> peat = getPrimaryTable().getAbstractTable();
		if (peat.isView())
			throw new PEException("No support for updatable views");
		PETable pet = peat.asTable();
		if (pet.getStorageGroup(pc).isSingleSiteGroup())
			planInternal(pc,ges);
		else if (pet.getUniqueKeys(pc).isEmpty())
			planInternal(pc,ges);
		else
			DMLStatement.planViaTransforms(pc,this, ges);
	}

	@Override
	public TransformFactory[] getTransformers() {
		return new TransformFactory[] {
			new InformationSchemaRewriteTransformFactory(),
				new SessionRewriteTransformFactory(),
			new ReplaceIntoTransformFactory()
		};
	}

	// we need access of this for efficiency purposes
	// don't go messing around with this, however
	public MultiMultiEdge<InsertIntoValuesStatement, ExpressionNode> getValuesEdge() {
		return values;
	}
	
}
