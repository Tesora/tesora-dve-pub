// OS_STATUS: public
package com.tesora.dve.sql.schema;

import java.util.Collections;
import java.util.List;

import com.tesora.dve.sql.parser.SourceLocation;

public class UnqualifiedName extends Name {

	private static final long serialVersionUID = 1L;
	protected String name;
	protected boolean quoted;
		
	public UnqualifiedName(String raw, boolean q) {
		this.name = raw;
		this.quoted = q;
	}
	
	public UnqualifiedName(String raw) {
		this(raw,null);
	}
	
	public UnqualifiedName(String raw, SourceLocation sloc) {
		super(sloc);
		quoted = (raw.length() > 0 && raw.charAt(0) == '`' && raw.endsWith("`"));
		if (quoted)
			name = raw.substring(1,raw.length() - 1);
		else
			name = raw;
	}
	
	@Override
	public String get() { return name; }
	@Override
	public String getQuoted() { return "`" + get() + "`"; }
	
	@Override
	public String getSQL() { 
		if (orig != null)
			return orig.getText();
		return (quoted ? getQuoted() : get()); 
	}
	
	@Override
	public boolean isQuoted() { return quoted; }
	
	@Override
	public boolean isQualified() { return false; }

	@Override
	public Name getCapitalized() {
		return new UnqualifiedName(get().toUpperCase(), isQuoted());
	}
	
	@Override
	public Name getQuotedName() {
		if (isQuoted())
			return this;
		return new UnqualifiedName(get(), true);
	}

	@Override
	public Name getUnquotedName() {
		if (!quoted)
			return this;
		return new UnqualifiedName(get());
	}
	
	@Override
	public UnqualifiedName getUnqualified() { return this; }
	
	@Override
	public List<UnqualifiedName> getParts() { return Collections.singletonList(this); }
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((get() == null) ? 0 : get().hashCode());
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
		UnqualifiedName other = (UnqualifiedName) obj;
		if (get() == null) {
			if (other.get() != null)
				return false;
		} else if (!get().equals(other.get()))
			return false;
		return true;
	}

	@Override
	public Name copy() {
		return new UnqualifiedName(name,quoted);
	}

}
