// OS_STATUS: public
package com.tesora.dve.sql.statement.ddl;

import java.util.List;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PESiteInstance;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.util.Pair;

public class PEAlterSiteInstanceStatement extends
		PEAlterStatement<PESiteInstance> {

	List<Pair<Name,LiteralExpression>> newOptions = null;

	public PEAlterSiteInstanceStatement(PESiteInstance target,
			List<Pair<Name,LiteralExpression>> newOptions) {
		super(target, true);
		
		this.newOptions = newOptions;
	}
	

	@Override
	protected PESiteInstance modify(SchemaContext pc, PESiteInstance backing) throws PEException {
		PECreateSiteInstanceStatement.parseOptions(pc, backing, newOptions, false);
		return backing;
	}


	@Override
	public CacheInvalidationRecord getInvalidationRecord(SchemaContext pc) {
		return CacheInvalidationRecord.GLOBAL;
	}

}
