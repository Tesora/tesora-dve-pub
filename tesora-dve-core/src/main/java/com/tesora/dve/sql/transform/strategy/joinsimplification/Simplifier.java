// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.joinsimplification;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.DMLStatement;

abstract class Simplifier {

	public abstract boolean applies(SchemaContext sc, DMLStatement dmls) throws PEException;
	
	public abstract DMLStatement simplify(SchemaContext sc, DMLStatement in, JoinSimplificationTransformFactory parent) throws PEException;
	
}
