// OS_STATUS: public
package com.tesora.dve.sql.node;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MultiMultiEdge<O extends LanguageNode, T extends LanguageNode> extends Edge<O,T> implements Iterable<MultiEdge<O,T>> {

	protected List<MultiEdge<O, T>> target = new ArrayList<MultiEdge<O, T>>();
	
	private MultiEdge<O, T> makeSub() {
		return new MultiEdge<O, T>(getParentClass(),getParent(),EdgeName.INSERT_MULTIVALUE);
	}
	
	public MultiMultiEdge(Class<O> oc, O owner, IEdgeName name) {
		super(oc, owner, name);
	}

	@Override
	public boolean has() {
		return !target.isEmpty();
	}

	@Override
	public boolean isMultiMulti() {
		return true;
	}
	
	public List<MultiEdge<O, T>> getMultiEdges() {
		return target;
	}
	
	public List<List<T>> getMultiMulti() {
		ArrayList<List<T>> results = new ArrayList<List<T>>();
		for(MultiEdge<O, T> me : target)
			results.add(me.getMulti());
		return results;
	}
	
	public void setMultiMulti(List<List<T>> in) {
		for(List<T> l : in) {
			MultiEdge<O, T> container = makeSub();
			container.set(l);
			target.add(container);
		}
	}

	@Override
	public Iterator<MultiEdge<O, T>> iterator() {
		return target.iterator();
	}

	
	@Override
	public void clear() {
		target.clear();
	}

	@Override
	public LanguageNode apply(LanguageNode in) {
		throw new MigrationException("Illegal call to MultiMultiEdge.apply");
	}

	@Override
	public boolean isMulti() {
		return true;
	}

	@Override
	public T get() {
		throw new MigrationException("Illegal call to MultiMultiEdge.get");
	}

	@Override
	public List<T> getMulti() {
		throw new MigrationException("Illegal call to MultiMultiEdge.getMulti");
	}

	@Override
	public void set(T in) {
		throw new MigrationException("Illegal call to MultiMultiEdge.set");
	}

	@Override
	public void set(List<T> in) {
		throw new MigrationException("Illegal call to MultiMultiEdge.setMulti");		
	}

	@Override
	public boolean schemaEqual(Edge<O, T> other) {
		throw new MigrationException("Illegal call to MultiMultiEdge.schemaEqual");
	}

	@Override
	public void add(T in) {
		throw new MigrationException("Illegal call to MultiMultiEdge.add");
	}

	@Override
	public void add(List<T> in) {
		throw new MigrationException("Illegal call to MultiMultiEdge.add");
	}

	@Override
	protected int schemaTargetHash() {
		throw new MigrationException("Illegal call to MultiMultiEdge.schemaTargetHash");
	}


}
