// OS_STATUS: public
package com.tesora.dve.sql.transform.execution;

import com.tesora.dve.sql.util.BinaryProcedure;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.Pair;

public class PassThroughCommand<T,K,V> {

	public enum Command {
		CREATE, DROP, ALTER, SHOW
	}

	protected T target;
	protected Command action;
	protected ListOfPairs<K, V> options;
	
	public PassThroughCommand(Command a, T target, ListOfPairs<K,V> opts) {
		this.action = a;
		this.target = target;
		this.options = opts;
	}
	
	public Command getAction() {
		return action;
	}
	
	public T getTarget() {
		return target;
	}
	
	public ListOfPairs<K,V> getOptions() {
		return options;
	}
	
	@Override
	public String toString() {
		StringBuilder buf = new StringBuilder();
		buf.append(this.getClass().getName()).append(":'").append(getTarget()).append("':")
			.append(action).append("{");
		Functional.join(options, buf, ", ", new BinaryProcedure<Pair<K, V>, StringBuilder>() {

			@Override
			public void execute(Pair<K, V> aobj, StringBuilder bobj) {
				bobj.append(aobj.getFirst()).append("=").append(aobj.getSecond());
			}
			
		});
		buf.append("}");
		return buf.toString();
	}

}
