// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.join;

import java.io.PrintStream;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.util.ListSet;

public class RewriteBuffers {

	OriginalProjectionBuffer originalProjection;
	P1ProjectionBuffer p1proj;
	P2ProjectionBuffer p2proj;
	OriginalWhereBuffer originalFilter;
	ExplicitJoinBuffer explicitJoins;
	P3ProjectionBuffer finalProjection;
	
	Buffer[] buffers;
	
	PartitionLookup partitions;
	SchemaContext sc;
	
	public RewriteBuffers(PartitionLookup pl, SchemaContext sc) {
		this.sc = sc;
		originalProjection = new OriginalProjectionBuffer();
		p1proj = new P1ProjectionBuffer(originalProjection);
		p2proj = new P2ProjectionBuffer(p1proj, pl);
		originalFilter = new OriginalWhereBuffer(p2proj, pl);
		explicitJoins = new ExplicitJoinBuffer(originalFilter,pl);
		finalProjection = new P3ProjectionBuffer(explicitJoins,pl);
		partitions = pl;
		buffers = new Buffer[] { originalProjection, p1proj, p2proj, originalFilter, explicitJoins, finalProjection };
	}
	
	public void adapt(SelectStatement allParts) throws PEException {
		for(Buffer b : buffers)
			b.adapt(sc, allParts);
				
		partitions.adapt(finalProjection, originalFilter, explicitJoins);
	}
	
	public OriginalWhereBuffer getWhereClauseBuffer() {
		return originalFilter;
	}
	
	public P3ProjectionBuffer getProjectionBuffer() {
		return finalProjection;
	}
	
	public ExplicitJoinBuffer getJoinsBuffer() {
		return explicitJoins;
	}
	
	public P2ProjectionBuffer getP2() {
		return p2proj;
	}
	
	public P1ProjectionBuffer getP1() {
		return p1proj;
	}
	
	public OriginalProjectionBuffer getOriginalProjection() {
		return originalProjection;
	}
	
	public void describeState(PrintStream ps) {
		ps.println("scoredJoins:");
		for(BufferEntry be : explicitJoins.getScoredBridging())
			ps.println("   " + be);
		ps.println("scoredProjections:");
		for(BufferEntry be : finalProjection.getScoredBridging())
			ps.println("   " + be);
		ps.println("scoredFilters:");
		for(BufferEntry be : originalFilter.getScoredBridging())
			ps.println("   " + be);
	}
	
	public boolean apply(ListSet<JoinedPartitionEntry> heads, boolean last) {
		boolean any = false;
		for(JoinedPartitionEntry ipe : heads)
			if (ipe.take(getP2()))
				any = true;
		if (any) return true;
		for(JoinedPartitionEntry ipe : heads)
			if (ipe.take(getJoinsBuffer()))
				any = true;
		if (any) return true;
		for(JoinedPartitionEntry ipe : heads)
			if (ipe.take(getWhereClauseBuffer(), last))
				any = true;
		return any;
	}	
}
