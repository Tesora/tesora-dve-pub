// OS_STATUS: public
package com.tesora.dve.common.catalog;

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