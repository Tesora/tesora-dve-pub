// OS_STATUS: public
package com.tesora.dve.sql.statement.ddl;

import java.util.HashMap;
import java.util.List;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.Container;
import com.tesora.dve.distribution.DistributionRange;
import com.tesora.dve.distribution.RangeTableRelationship;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.RangeDistribution;
import com.tesora.dve.sql.schema.SchemaContext;

public class PEDropRangeStatement extends
		PEDropStatement<RangeDistribution, DistributionRange> {

	public PEDropRangeStatement(
			Persistable<RangeDistribution, DistributionRange> targ) {
		super(RangeDistribution.class, null, true, targ, "RANGE");
	}

	public void ensureUnreferenced(SchemaContext sc) {
		RangeDistribution rd = getTarget().get();
		try {
			sc.beginSaveContext();
			DistributionRange dr = null;
			try {
				rd.persistTree(sc,true);
				dr = rd.getPersistent(sc);
			} finally {
				sc.endSaveContext();
			}
			HashMap<String,Object> params = new HashMap<String,Object>();
			params.put("dr", dr);
			List<CatalogEntity> any = sc.getCatalog().query("from RangeTableRelationship where distributionRange = :dr", params);
			if (!any.isEmpty()) {
				RangeTableRelationship rtr = (RangeTableRelationship)any.get(0);
				throw new SchemaException(Pass.PLANNER, "Unable to drop range " + rd.getName().get() + " because used by table " + rtr.getTable().getName());
			}
			any = sc.getCatalog().query("from Container ct where ct.range = :dr",params);
			if (!any.isEmpty()) {
				Container c = (Container)any.get(0);
				throw new SchemaException(Pass.PLANNER, "Unable to drop range " + rd.getName().get() + " because used by container " + c.getName());
			}
		} catch (PEException pe) {
			throw new SchemaException(Pass.PLANNER, "Unable to compute reference set for range " + rd.getName().get());
		}
	}
}
