// OS_STATUS: public
package com.tesora.dve.common;

import java.util.Collection;
import java.util.List;

public final class PECollectionUtils {
	
	private PECollectionUtils() {
	}
	
	@SuppressWarnings("unchecked")
	public static <T> T selectRandom(Collection<T> col) {
		int selected = (int) (Math.random() * col.size());
		return (T) col.toArray()[selected];
	}

	public static <T> T selectRandom(List<T> col) {
		int selected = (int) (Math.random() * col.size());
		return col.get(selected);
	}

}
