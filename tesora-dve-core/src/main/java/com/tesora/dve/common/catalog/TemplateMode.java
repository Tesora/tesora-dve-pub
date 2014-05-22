package com.tesora.dve.common.catalog;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import com.tesora.dve.sql.schema.ConnectionContext;
import com.tesora.dve.sql.schema.SchemaVariables;

public enum TemplateMode {
	OPTIONAL(false, false),
	REQUIRED(true, false),
	STRICT(true, true);

	public static TemplateMode getCurrentDefault(final ConnectionContext cc) {
		return SchemaVariables.getTemplateMode(cc);
	}

	public static TemplateMode getCurrentDefault() {
		return TemplateMode.getCurrentDefault(null);
	}

	public static TemplateMode getModeFromName(final ConnectionContext cc, final String name) {
		if (name != null) {
			return TemplateMode.valueOf(name);
		}

		return TemplateMode.getCurrentDefault(cc);
	}

	public static TemplateMode getModeFromName(final String name) {
		return TemplateMode.getModeFromName(null, name);
	}

	public static boolean hasModeForName(final String name) {
		try {
			TemplateMode.valueOf(name);
		} catch (final IllegalArgumentException e) {
			return false;
		}

		return true;
	}

	private boolean requiresTemplate;
	private boolean isStrict;

	private TemplateMode(final boolean requiresTemplate, final boolean isStrict) {
		this.requiresTemplate = requiresTemplate;
		this.isStrict = isStrict;
	}

	public boolean isDefault(final ConnectionContext cc) {
		return this.equals(TemplateMode.getCurrentDefault(cc));
	}

	public boolean isDefault() {
		return this.isDefault(null);
	}

	public boolean requiresTemplate() {
		return this.requiresTemplate;
	}

	public boolean isStrict() {
		return (this.requiresTemplate && this.isStrict);
	}
}