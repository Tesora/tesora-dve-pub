// OS_STATUS: public
package com.tesora.dve.tools.aitemplatebuilder;

import java.util.Set;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.types.Type;
import com.tesora.dve.tools.aitemplatebuilder.CorpusStats.TableStats;
import com.tesora.dve.tools.aitemplatebuilder.CorpusStats.TableStats.TableColumn;

public interface TemplateRangeItem extends TemplateItem {

	public abstract boolean contains(final TableStats table);

	public abstract Set<TableColumn> getRangeColumnsFor(final TableStats table);

	public Set<Type> getUniqueColumnTypes() throws PEException;

	/**
	 * Columns are compared based on their unqualified name and type only.
	 */
	public boolean hasCommonColumn(final Set<TableColumn> columns);
}
