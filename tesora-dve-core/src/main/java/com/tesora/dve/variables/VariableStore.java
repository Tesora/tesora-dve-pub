package com.tesora.dve.variables;

import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;

public abstract class VariableStore {

	public VariableStore() {
	}

	protected abstract <Type> ValueReference<Type> getReference(VariableHandler<Type> vh);
	
	public abstract <Type> void setValue(VariableHandler<Type> vh, Type t);
	
	public <Type> Type getValue(VariableHandler<Type> vh) {
		ValueReference<Type> vr = (ValueReference<Type>) getReference(vh);
		if (vr == null)
			throw new PECodingException("Variable \'" + vh.getName() + "\' not found");
		return vr.get();
	}
	
	protected static class ValueReference<Type> {
		
		private final VariableHandler<Type> variable;
		private Type value;
		
		public ValueReference(VariableHandler<Type> vh) {
			variable = vh;
		}
		
		public ValueReference<Type> copy() {
			ValueReference<Type> out = new ValueReference<Type>(variable);
			out.set(get());
			return out;
		}
		
		public void set(Type t) {
			value = t;
		}
		
		public void set(String v) throws PEException {
			set(variable.getMetadata().convertToInternal(v));
		}
		
		protected void setInternal(Object t) {
			value = (Type) t;
		}
		
		public Type get() {
			return value;
		}
		
		public VariableHandler<Type> getVariable() {
			return variable;
		}
	}
}
