package com.tesora.dve.sql;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.tesora.dve.server.bootstrap.BootstrapHost;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ProxyConnectionResource;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.standalone.PETest;

public class MagentoSchemaTest extends SchemaTest {
	
	private static final StorageGroupDDL sg = 
			new StorageGroupDDL("sys",3,"sysg");

	private static final ProjectDDL testDDL =
		new PEDDL("checkdb",sg,"schema");

	private static ProxyConnectionResource conn;

	@BeforeClass
	public static void setup() throws Throwable {
		PETest.projectSetup(testDDL);
		PETest.bootHost = BootstrapHost.startServices(PETest.class);

		conn = new ProxyConnectionResource();
		testDDL.create(conn);
		
		final String[] requirements = {
				"CREATE RANGE IF NOT EXISTS magento_xl_catalog_compare_item_range (smallint) PERSISTENT GROUP sysg",
				"CREATE RANGE IF NOT EXISTS magento_xl_catalog_product_index_price_range (int) PERSISTENT GROUP sysg"
		};

		for (final String stmt : requirements) {
			conn.execute(stmt);
		}

		final String[] schema = {
				"CREATE TABLE `catalog_product_entity` (`entity_id` int(10) unsigned NOT NULL,`entity_type_id` smallint(5) unsigned NOT NULL,`attribute_set_id` smallint(5) unsigned NOT NULL,`type_id` varchar(32) NOT NULL,`sku` varchar(64) DEFAULT NULL,`has_options` smallint(5) NOT NULL,`required_options` smallint(5) unsigned NOT NULL,`created_at` timestamp NULL DEFAULT NULL,`updated_at` timestamp NULL DEFAULT NULL,PRIMARY KEY (`entity_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 BROADCAST DISTRIBUTE",
				"CREATE TABLE `catalog_compare_item` (`catalog_compare_item_id` int(10) unsigned NOT NULL,`visitor_id` int(10) unsigned NOT NULL,`customer_id` int(10) unsigned DEFAULT NULL,`product_id` int(10) unsigned NOT NULL,`store_id` smallint(5) unsigned DEFAULT NULL,PRIMARY KEY (`catalog_compare_item_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 RANGE DISTRIBUTE ON (`store_id`) USING `magento_xl_catalog_compare_item_range`",
				"CREATE TABLE `catalog_product_index_price` (`entity_id` int(10) unsigned NOT NULL,`customer_group_id` smallint(5) unsigned NOT NULL,`website_id` smallint(5) unsigned NOT NULL,`tax_class_id` smallint(5) unsigned DEFAULT NULL,`price` decimal(12,0) DEFAULT NULL,`final_price` decimal(12,0) DEFAULT NULL,`min_price` decimal(12,0) DEFAULT NULL,`max_price` decimal(12,0) DEFAULT NULL,`tier_price` decimal(12,0) DEFAULT NULL,`group_price` decimal(12,0) DEFAULT NULL,PRIMARY KEY (`customer_group_id`,`entity_id`,`website_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 RANGE DISTRIBUTE ON (`entity_id`) USING `magento_xl_catalog_product_index_price_range`",
				"CREATE TABLE `catalog_category_product_index` (`category_id` int(10) unsigned NOT NULL,`product_id` int(10) unsigned NOT NULL,`position` int(10) DEFAULT NULL,`is_parent` smallint(5) unsigned NOT NULL,`store_id` smallint(5) unsigned NOT NULL,`visibility` smallint(5) unsigned NOT NULL,PRIMARY KEY (`category_id`,`product_id`,`store_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 RANGE DISTRIBUTE ON (`store_id`) USING `magento_xl_catalog_compare_item_range`",
				"CREATE TABLE `core_store` (`store_id` smallint(5) unsigned NOT NULL,`code` varchar(32) DEFAULT NULL,`website_id` smallint(5) unsigned NOT NULL,`group_id` smallint(5) unsigned NOT NULL,`name` varchar(255) NOT NULL,`sort_order` smallint(5) unsigned NOT NULL,`is_active` smallint(5) unsigned NOT NULL,PRIMARY KEY (`store_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 BROADCAST DISTRIBUTE",
				"CREATE TABLE `core_store_group` (`group_id` smallint(5) unsigned NOT NULL,`website_id` smallint(5) unsigned NOT NULL,`name` varchar(255) NOT NULL,`root_category_id` int(10) unsigned NOT NULL,`default_store_id` smallint(5) unsigned NOT NULL,PRIMARY KEY (`group_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 BROADCAST DISTRIBUTE",
				"CREATE TABLE `report_compared_product_index` (`index_id` bigint(20) unsigned NOT NULL,`visitor_id` int(10) unsigned DEFAULT NULL,`customer_id` int(10) unsigned DEFAULT NULL,`product_id` int(10) unsigned NOT NULL,`store_id` smallint(5) unsigned DEFAULT NULL,`added_at` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',PRIMARY KEY (`index_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 RANGE DISTRIBUTE ON (`store_id`) USING `magento_xl_catalog_compare_item_range`"
		};
		
		for (final String stmt : schema) {
			conn.execute(stmt);
		}
	}

	@AfterClass
	public static void after() throws Throwable {
		testDDL.destroy(conn);
		conn.disconnect();
	}
	
	@Ignore
	@Test
	public void testJoinSchedule() throws Throwable {
		final String case1 = "SELECT COUNT(DISTINCT e.entity_id) FROM `catalog_product_entity` AS `e`"
				+ " INNER JOIN `catalog_compare_item` AS `t_compare` ON (t_compare.product_id=e.entity_id) AND (t_compare.customer_id = '4984')"
				+ " INNER JOIN `catalog_product_index_price` AS `price_index` ON price_index.entity_id = e.entity_id AND price_index.website_id = '5' AND price_index.customer_group_id = '1'"
				+ " INNER JOIN `catalog_category_product_index` AS `cat_index` ON cat_index.product_id=e.entity_id AND cat_index.store_id=5 AND cat_index.category_id = '6'"
				+ " LEFT JOIN `core_store` AS `store_index` ON store_index.store_id = t_compare.store_id"
				+ " LEFT JOIN `core_store_group` AS `store_group_index` ON store_index.group_id = store_group_index.group_id"
				+ " LEFT JOIN `catalog_category_product_index` AS `store_cat_index` ON store_cat_index.product_id = e.entity_id AND store_cat_index.store_id = t_compare.store_id AND store_cat_index.category_id=store_group_index.root_category_id WHERE (cat_index.visibility IN(3, 2, 4) OR store_cat_index.visibility IN(3, 2, 4))";

		final String case2 = "SELECT `e`.*, `price_index`.`price`, `price_index`.`tax_class_id`, `price_index`.`final_price`, IF(price_index.tier_price IS NOT NULL, LEAST(price_index.min_price, price_index.tier_price), price_index.min_price) AS `minimal_price`, `price_index`.`min_price`, `price_index`.`max_price`, `price_index`.`tier_price`, `idx_table`.`product_id`, `idx_table`.`store_id` AS `item_store_id`, `idx_table`.`added_at`, `cat_index`.`position` AS `cat_index_position`, `cat_index`.`visibility`, `store_cat_index`.`visibility` AS `store_visibility` FROM `catalog_product_entity` AS `e`"
				+ " INNER JOIN `catalog_product_index_price` AS `price_index` ON price_index.entity_id = e.entity_id AND price_index.website_id = '5' AND price_index.customer_group_id = 0"
				+ " INNER JOIN `report_compared_product_index` AS `idx_table` ON (idx_table.product_id=e.entity_id) AND (idx_table.visitor_id = '4')"
				+ " INNER JOIN `catalog_category_product_index` AS `cat_index` ON cat_index.product_id=e.entity_id AND cat_index.store_id=5 AND cat_index.category_id = '6'"
				+ " LEFT JOIN `core_store` AS `store_index` ON store_index.store_id = idx_table.store_id"
				+ " LEFT JOIN `core_store_group` AS `store_group_index` ON store_index.group_id = store_group_index.group_id"
				+ " LEFT JOIN `catalog_category_product_index` AS `store_cat_index` ON store_cat_index.product_id = e.entity_id AND store_cat_index.store_id = idx_table.store_id AND store_cat_index.category_id=store_group_index.root_category_id WHERE (cat_index.visibility IN(3, 2, 4) OR store_cat_index.visibility IN(3, 2, 4)) ORDER BY `added_at` DESC LIMIT 5";

		conn.execute(case1);
		conn.execute(case2);
	}
	
}
