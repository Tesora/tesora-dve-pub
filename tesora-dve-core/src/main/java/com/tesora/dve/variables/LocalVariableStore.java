package com.tesora.dve.variables;

import java.util.concurrent.ConcurrentHashMap;

public class LocalVariableStore extends VariableStore {

	private final ConcurrentHashMap<String, ValueReference<?>> values;
	
	public LocalVariableStore() {
		super();
		values = new ConcurrentHashMap<String, ValueReference<?>>();
	}

	@SuppressWarnings("unchecked")
	@Override
	protected <Type> ValueReference<Type> getReference(VariableHandler<Type> vh) {
		return (ValueReference<Type>) values.get(vh.getName());
	}

	@SuppressWarnings("unchecked")
	@Override
	public <Type> void setValue(VariableHandler<Type> vh, Type t) {
		// no locking
		ValueReference<Type> vr = (ValueReference<Type>) values.get(vh.getName());
		if (vr == null) {
			vr = new ValueReference<Type>(vh);
			values.put(vh.getName(),vr);
		}
		vr.set(t);
	}

	protected void setInternal(VariableHandler<?> vh, Object o) {
		ValueReference<?> vr = new ValueReference(vh);
		values.put(vh.getName(),vr);
		vr.setInternal(o);		
	}
	
}
