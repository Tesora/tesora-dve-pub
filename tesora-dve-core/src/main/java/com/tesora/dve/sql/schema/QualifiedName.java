// OS_STATUS: public
package com.tesora.dve.sql.schema;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.UnaryFunction;
import com.tesora.dve.sql.util.UnaryPredicate;

public class QualifiedName extends Name {

	private static final long serialVersionUID = 1L;
	private List<UnqualifiedName> names;
	
	public QualifiedName(List<UnqualifiedName> parts) {
		names = parts;
	}

	public QualifiedName(UnqualifiedName ...parts) {
		names = Arrays.asList(parts);
	}
	
	@Override
	public String get() {
		return join(new UnaryFunction<String, UnqualifiedName>() {

			@Override
			public String evaluate(UnqualifiedName object) {
				return object.get();
			}
			
		});
	}

	@Override
	public String getQuoted() {
		return join(new UnaryFunction<String, UnqualifiedName>() {

			@Override
			public String evaluate(UnqualifiedName object) {
				return object.getQuoted();
			}
			
		});
	}

	@Override
	public boolean isQuoted() {
		return Functional.all(names, new UnaryPredicate<UnqualifiedName>() {

			@Override
			public boolean test(UnqualifiedName object) {
				return object.isQuoted();
			}
			
		});
	}

	@Override
	public boolean isQualified() {
		return true;
	}

	@Override
	public String getSQL() {
		return join(new UnaryFunction<String, UnqualifiedName>() {

			@Override
			public String evaluate(UnqualifiedName object) {
				return object.getSQL();
			}
			
		});
	}
	
	private String join(UnaryFunction<String, UnqualifiedName> proc) {
		StringBuilder buf = new StringBuilder();
		List<String> strs = Functional.apply(names, proc);
		Functional.join(strs, buf, ".");
		return buf.toString();
	}

	@Override
	public Name getCapitalized() {
		if (isQuoted())
			return null;
		return new QualifiedName(Functional.apply(names, new UnaryFunction<UnqualifiedName, UnqualifiedName>() {

			@Override
			public UnqualifiedName evaluate(UnqualifiedName object) {
				return (UnqualifiedName)object.getCapitalized();
			}
			
		}));
	}

	@Override
	public Name getQuotedName() {
		if (isQuoted())
			return this;
		return new QualifiedName(Functional.apply(names, new UnaryFunction<UnqualifiedName, UnqualifiedName>() {

			@Override
			public UnqualifiedName evaluate(UnqualifiedName object) {
				return (UnqualifiedName)object.getQuotedName();
			}
			
		}));
	}
	
	@Override
	public Name getUnquotedName() {
		return new QualifiedName(Functional.apply(names, new UnaryFunction<UnqualifiedName, UnqualifiedName>() {

			@Override
			public UnqualifiedName evaluate(UnqualifiedName object) {
				return object.getUnquotedName().getUnqualified();
			}
			
		}));
	}
	
	@Override
	public UnqualifiedName getUnqualified() {
		return names.get(names.size() - 1);
	}
	
	public int getQualifiedDepth() { return names.size(); }
	public UnqualifiedName getNamespace() {
		if (names.size() > 1)
			return names.get(names.size() - 2);
		return null;
	}
	
	@Override
	public List<UnqualifiedName> getParts() { return Collections.unmodifiableList(names); }
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((names == null) ? 0 : names.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		QualifiedName other = (QualifiedName) obj;
		if (names == null) {
			if (other.names != null)
				return false;
		} else if (!names.equals(other.names))
			return false;
		return true;
	}

	@Override
	public Name copy() {
		return new QualifiedName(new ArrayList<UnqualifiedName>(names));
	}
	
	
	
}
