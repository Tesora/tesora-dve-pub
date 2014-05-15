// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.join;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.jg.DPart;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.UnaryFunction;

abstract class FinalBuffer extends Buffer {

	protected BufferPartitionInfo partitionInfo;
	protected ListSet<BufferEntry> bridging;
	protected ListSet<BufferEntry> scoredBridges;
	
	public FinalBuffer(BufferKind bk, Buffer bef, PartitionLookup pl) {
		super(bk, bef);
		partitionInfo = new BufferPartitionInfo(pl);
		bridging = null;
	}
	
	public ListSet<BufferEntry> getEntriesFor(DPart p) {
		return partitionInfo.filter(getEntries(), p);
	}
	
	public List<DPart> getPartitionsFor(BufferEntry be) {
		return partitionInfo.getPartitionsForEntry(be);
	}
	
	public void setBridging(ListSet<BufferEntry> set) throws PEException {
		bridging = set;
		scoredBridges = buildScoredList(bridging, getScoreFunction());
	}
	
	public ListSet<BufferEntry> getBridging() {
		return bridging;
	}
	
	public ListSet<BufferEntry> getScoredBridging() {
		return scoredBridges;
	}
		
	@SuppressWarnings("rawtypes")
	public static <S, R extends Comparable> ListSet<S> buildScoredList(Collection<S> over, final UnaryFunction<R, S> fun) throws PEException {
		Comparator<S> ordering = new Comparator<S>() {
			@SuppressWarnings("unchecked")
			@Override
			public int compare(S left, S right) {
				R ls = fun.evaluate(left);
				R rs = fun.evaluate(right);
				return ls.compareTo(rs);
			}
		};
		List<S> out = Functional.toList(over);
		Collections.sort(out, ordering);
		if (over.size() != out.size())
			throw new PEException("Scoring lost entries");
		ListSet<S> outlist = new ListSet<S>();
		outlist.addAll(out);
		return outlist;
	}

	
	public UnaryFunction<Integer, BufferEntry> getScoreFunction() {
		return new UnaryFunction<Integer, BufferEntry>() {

			@Override
			public Integer evaluate(BufferEntry object) {
				List<DPart> parts = getPartitionsFor(object);
				if (parts == null) return 0;
				return parts.size();
			}
			
		};
	}
	
}