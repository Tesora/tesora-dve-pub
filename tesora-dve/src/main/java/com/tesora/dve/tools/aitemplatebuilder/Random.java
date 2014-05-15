// OS_STATUS: public
package com.tesora.dve.tools.aitemplatebuilder;

public final class Random implements TemplateItem {

	private static final String DISTRIBUTION_TEMPLATE_BLOCK_NAME = "Random";

	public static final TemplateItem SINGLETON_TEMPLATE_ITEM = new Random();

	private Random() {
	}

	@Override
	public String getTemplateItemName() {
		return DISTRIBUTION_TEMPLATE_BLOCK_NAME;
	}

	@Override
	public String toString() {
		return getTemplateItemName();
	}
}