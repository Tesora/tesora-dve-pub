// OS_STATUS: public
package com.tesora.dve.sql.schema.cache;

import java.util.List;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.parser.ExtractedLiteral;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.execution.ExecutionPlan;
import com.tesora.dve.sql.transform.execution.RebuiltPlan;

public interface RegularCachedPlan extends CachedPlan {

	public RebuiltPlan rebuildPlan(SchemaContext sc, List<ExtractedLiteral> literals) throws PEException;
	
	public boolean isValid(SchemaContext sc, List<ExtractedLiteral> literals) throws PEException;	
	
	// bypass the usual literals checking
	public ExecutionPlan showPlan(SchemaContext sc, List<ExtractedLiteral> literals) throws PEException;
	// get the literal types - used in show plan
	public List<ExtractedLiteral.Type> getLiteralTypes();
}
