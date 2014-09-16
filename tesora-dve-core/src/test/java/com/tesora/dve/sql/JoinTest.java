package com.tesora.dve.sql;

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

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.tesora.dve.sql.util.DatabaseDDL;
import com.tesora.dve.sql.util.MirrorProc;
import com.tesora.dve.sql.util.MirrorTest;
import com.tesora.dve.sql.util.NativeDDL;
import com.tesora.dve.sql.util.NativeDatabaseDDL;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.PEDatabaseDDL;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.sql.util.TestResource;

public class JoinTest extends SchemaMirrorTest {
	private static final int SITES = 5;
	protected static boolean reloadDataAfterTest = false;

	private static PEDDL buildPEDDL() {
		PEDDL out = new PEDDL();
		StorageGroupDDL sgddl = new StorageGroupDDL("sys",SITES,"sysg");
		out.withStorageGroup(sgddl)
			.withDatabase(new PEDatabaseDDL("sysdb").withStorageGroup(sgddl))
			.withDatabase(new PEDatabaseDDL("tsysdb").withStorageGroup(sgddl));
		return out;
	}
	
	private static NativeDDL buildNativeDDL() {
		NativeDDL out = new NativeDDL();
		out.withDatabase(new NativeDatabaseDDL("sysdb"))
			.withDatabase(new NativeDatabaseDDL("tsysdb"));
		return out;
	}
	
	private static final ProjectDDL sysDDL = buildPEDDL();
	static final NativeDDL nativeDDL = buildNativeDDL();
	
	@Override
	protected ProjectDDL getMultiDDL() {
		return sysDDL;
	}
	
	@Override
	protected ProjectDDL getNativeDDL() {
		return nativeDDL;
	}

	@BeforeClass
	public static void setup() throws Throwable {
		setup(sysDDL,null,nativeDDL,getSchema());
	}

	@After
	public void teardownAfter () throws Throwable {
		if (reloadDataAfterTest) {
			ArrayList<MirrorTest> loadDataCmds = new ArrayList<MirrorTest>();
			loadDataCmds.add(buildPopulate());
			runTest(loadDataCmds);
			reloadDataAfterTest = false;
		}
	}
	
	// having a very hard time building this test
	// here's the basic shapes of the tables:
	// L (`id` int, `fid` int, `sid` int, `test` varchar(32), primary key (`id`), unique key (`fid`), index (`sid`) )
	// population is 20 rows.  id is monotonically increasing, fid is factors of 2, sid is factors of three
	// distribution is either range on id, or something else

	static String[] tabNames = new String[] {
			"LA", "RA",
			"LRa", "RRa",
			"LRb", "RRb",
			"LB", "RB"
	};

	private static MirrorProc buildPopulate() {
		return new MirrorProc() {

			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				if (mr == null) return null;
				DatabaseDDL db = mr.getDDL().getDatabases().get(0);
				// declare the tables
				ResourceResponse rr = null;

				mr.getConnection().execute("use " + db.getDatabaseName());
				
				String colspec = " (`fid`, `test`, `nufid`, `nusid`) values ";
				char[] letters = " abcdefghijklmnopqrstuv".toCharArray();
				StringBuffer insertBody = new StringBuffer();
				for(int i = 1; i < 21; i++) {
					if (i > 1)
						insertBody.append(", ");
					insertBody.append("(")
						.append(2*i).append(", '")
						.append(letters[i]).append("', ")
						.append( i > 10 ? 2 : 1).append(", ")
						.append( i / 2 )
						.append(")");
				}
				for(int i = 0; i < tabNames.length; i++) {
					rr = mr.getConnection().execute("truncate " + tabNames[i]);
					rr = mr.getConnection().execute("insert into " + tabNames[i] + colspec + insertBody.toString());
				}

				db = mr.getDDL().getDatabases().get(1);
				mr.getConnection().execute("use " + db.getDatabaseName());
				mr.getConnection().execute("truncate otab");
				rr = mr.getConnection().execute("insert into otab " + colspec + insertBody.toString());
					
				return rr;
			}
		};
	}
	
	
	private static List<MirrorTest> getSchema() {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		out.add(new MirrorProc() {

			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				if (mr == null) return null;
				boolean ext = !nativeDDL.equals(mr.getDDL());
				DatabaseDDL db = mr.getDDL().getDatabases().get(0);
				// declare the tables
				ResourceResponse rr = null;
				if (ext) { 
					// declare the range
					mr.getConnection().execute("/*#dve create range arange_" + db.getDatabaseName() + " (int) persistent group " + mr.getDDL().getPersistentGroup().getName() + " */");
					mr.getConnection().execute("/*#dve create range brange_" + db.getDatabaseName() + " (int) persistent group " + mr.getDDL().getPersistentGroup().getName() + " */");
				}
				String body = "(`id` int auto_increment, `fid` int, `nufid` int, `nusid` int, `test` varchar(32), "
						+ " primary key (`id`), unique key (`fid`), index (`nufid`), index (`nusid`) )";
				String[] dists = new String[] {
						"random distribute",
						"range distribute on (`id`) using arange_" + db.getDatabaseName(),
						"range distribute on (`id`) using brange_" + db.getDatabaseName(),
						"broadcast distribute"
				};
				for(int i = 0; i < dists.length; i++) {
					for(int j = 0; j < 2; j++) {
						String tna = tabNames[2*i + j];
						StringBuffer buf = new StringBuffer();
						buf.append("create table `").append(tna).append("` ").append(body);
						buf.append(" /*#dve ").append(dists[i]).append(" */");
						rr = mr.getConnection().execute(buf.toString());
					}
				}
				// create one table in the second database
				db = mr.getDDL().getDatabases().get(1);
				mr.getConnection().execute("use " + db.getDatabaseName());
				StringBuffer buf = new StringBuffer();
				buf.append("create table `otab` ").append(body);
				buf.append(" /*#dve ").append(dists[1]).append(" */");
				mr.getConnection().execute(buf.toString());
				
				return rr;
			}
		});
		out.add(buildPopulate());
		return out;
	}	

	private static final String[] lefts = new String[] { "LA", "LRa", "LRb", "LB" };
	private static final String[] rights = new String[] { "RA", "RRa", "RRb", "RB" };
	
	private void runAll(String[] lts, String rts[], String sql, boolean ignoreorder) throws Throwable {
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		for(int l = 0; l < lts.length; l++) {
			for(int r = 0; r < rts.length; r++) {
				String actual = sql.replaceFirst("#L", lts[l]).replaceFirst("#R", rts[r]);
				tests.add(new StatementMirrorFun(ignoreorder,actual));
			}
		}
		runTest(tests);
	}

	@Override
	protected void onConnect(TestResource tr) throws Throwable {
		DatabaseDDL db = tr.getDDL().getDatabases().get(0);
		tr.getConnection().execute("use " + db.getDatabaseName());
	}

	
	@Test
	public void testInnerJoin() throws Throwable {
		runAll(lefts,rights,"select l.*, r.* from #L l inner join #R r on l.id=r.fid",true);
	}
	
	@Test
	public void testLOJ() throws Throwable {
		runAll(lefts,rights,"select l.*, r.* from #L l left outer join #R r on l.id=r.fid",true);
	}
	
	@Test
	public void testROJ() throws Throwable {
		runAll(lefts,rights,"select l.*, r.* from #L l right outer join #R r on l.id=r.fid",true);
	}
	
	@Test
	public void testLOJA() throws Throwable {
		runAll(lefts,rights,"select l.*, r.* from #L l left outer join #R r on l.id=r.fid where l.nusid = 2",true);
	}
	
	@Test
	public void testLOJB() throws Throwable {
		runAll(lefts,rights,"select l.*, r.* from #L l left outer join #R r on l.id=r.nufid where l.test in ('a','b')",true);
	}

	@Test
	public void testNaturalJoin() throws Throwable {
		runAll(lefts, rights, "select l.*, r.* from #L l natural join #R r", true);
	}

	@Test
	public void testNaturalLeftJoin() throws Throwable {
		runAll(lefts, rights, "select l.*, r.* from #L l natural left outer join #R r", true);
	}

	@Test
	public void testPE227() throws Throwable {
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		tests.add(new StatementMirrorFun(true,"select l.*, r.* from LRa l, tsysdb.otab r where l.id = r.id"));
		runTest(tests);
	}

	@Test
	public void testPE227B() throws Throwable {
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		tests.add(new StatementMirrorProc("use tsysdb"));
		tests.add(new StatementMirrorFun(true,"select l.*, r.*, s.* from sysdb.LRa l, otab r, sysdb.LB s where l.id = r.id and r.id = s.id"));
		runTest(tests);
	}
	
	@Test
	public void testPE672() throws Throwable {
		reloadDataAfterTest = true;
		// the select we want is
		// select a.id, b.id, c.id from LA a left outer join LB b on a.fid = b.nusid left outer join RB c on b.id = c.nusid where a.id < 10;
		// the result set for this is fully populated on a.id for 17 rows, on b.id for 13 rows, on c.id for 8 rows
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		// someday we should test more possibilities, but not today
		tests.add(new StatementMirrorProc("delete a, b, c from LRa a left outer join LB b on a.fid = b.nusid left outer join LRb c on b.id = c.nusid where a.id < 10"));
		runTest(tests);
	}

	@Test
	public void testPE692() throws Throwable {
		String[] decls = new String[] { 
				"/*#dve create range if not exists order_range (int) persistent group " + sysDDL.getPersistentGroup().getName() + "*/",
				"/*#dve create range if not exists node_range (int) persistent group " + sysDDL.getPersistentGroup().getName() + "*/",
				"CREATE TABLE if not exists `uc_orders` (`order_id` int(10) unsigned NOT NULL  auto_increment ,`uid` int(10) unsigned NOT NULL  DEFAULT '0', "
				+"`order_status` varchar(32) NOT NULL  DEFAULT '' ,`order_total` decimal(15,3) NOT NULL  DEFAULT '0.000' ,"
				+"`primary_email` varchar(96) NOT NULL  DEFAULT '' ,`delivery_first_name` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`delivery_last_name` varchar(255) NOT NULL  DEFAULT '' ,`delivery_phone` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`delivery_company` varchar(255) NOT NULL  DEFAULT '' ,`delivery_street1` varchar(255) NOT NULL  DEFAULT '' ," 
				+"`delivery_street2` varchar(255) NOT NULL  DEFAULT '' ,`delivery_city` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`delivery_zone` mediumint(8) unsigned NOT NULL  DEFAULT '0' ,`delivery_postal_code` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`delivery_country` mediumint(8) unsigned NOT NULL  DEFAULT '0' ,`billing_first_name` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`billing_last_name` varchar(255) NOT NULL  DEFAULT '' ,`billing_phone` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`billing_company` varchar(255) NOT NULL  DEFAULT '' ,`billing_street1` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`billing_street2` varchar(255) NOT NULL  DEFAULT '' ,`billing_city` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`billing_zone` mediumint(8) unsigned NOT NULL  DEFAULT '0' ,`billing_postal_code` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`billing_country` mediumint(8) unsigned NOT NULL  DEFAULT '0' ,`payment_method` varchar(32) NOT NULL  DEFAULT '' ,"
				+"`data` text,`created` int(11) NOT NULL  DEFAULT '0' ,`modified` int(11) NOT NULL  DEFAULT '0' ,"
				+"`host` varchar(255) NOT NULL  DEFAULT '' ,`payment_currency` char(3) DEFAULT NULL  COMMENT 'reebonz order local currency',"
				+"`payment_currency_amount` decimal(15,3) DEFAULT NULL , PRIMARY KEY (`order_id`), KEY `uid` (`uid`), "
				+"KEY `order_status` (`order_status`), KEY `billing_country` (`billing_country`))"
				+" engine=myisam /*#dve range distribute on (`order_id`) using order_range */",
				"CREATE TABLE if not exists `uc_order_products` (`order_product_id` int(10) unsigned NOT NULL AUTO_INCREMENT,"
				+"`order_id` int(10) unsigned NOT NULL DEFAULT '0',`nid` int(10) unsigned NOT NULL DEFAULT '0',"
				+"`title` varchar(128) NOT NULL DEFAULT '',`manufacturer` varchar(32) NOT NULL DEFAULT '',"
				+"`model` varchar(255) NOT NULL DEFAULT '',`qty` smallint(5) unsigned NOT NULL DEFAULT '0',"
				+"`cost` decimal(15,3) NOT NULL DEFAULT '0.000',`price` decimal(15,3) NOT NULL DEFAULT '0.000',"
				+"`weight` float NOT NULL DEFAULT '0',`data` text,`payment_currency` char(3) DEFAULT NULL,"
				+"`payment_currency_amount` decimal(15,3) DEFAULT NULL,"
				+"PRIMARY KEY (`order_product_id`), KEY `order_id` (`order_id`), KEY `nid` (`nid`), KEY `model` (`model`)"
				+") ENGINE=MyISAM DEFAULT CHARSET=utf8 AUTO_INCREMENT=676637 /*#dve  RANGE DISTRIBUTE ON (`order_id`) USING `order_range` */",
				"CREATE TABLE if not exists `content_type_product` (`vid` int(10) unsigned NOT NULL DEFAULT '0',`nid` int(10) unsigned NOT NULL DEFAULT '0',"
				+"`field_product_type_value` varchar(255) DEFAULT NULL,`hw_itemno` varchar(125) DEFAULT NULL,`field_event_nid` int(10) unsigned DEFAULT NULL,"
				+"`field_item_list_fid` int(11) DEFAULT NULL,`field_item_list_list` tinyint(4) DEFAULT NULL,`field_item_list_data` text,`field_color_value` longtext,"
				+"`field_was_price_value` decimal(10,2) DEFAULT NULL,`field_weight_value` int(11) DEFAULT NULL,`field_shipping_package_nid` int(10) unsigned DEFAULT NULL,"
				+"`field_dp_sdate_value` int(11) DEFAULT NULL,`field_dp_edate_value` int(11) DEFAULT NULL,`field_tag_no_value` varchar(25) DEFAULT NULL,"
				+"`field_supplier_sku_value` varchar(50) DEFAULT NULL,`field_wholesale_price_value` float DEFAULT NULL,`field_rrp_value` float DEFAULT NULL,"
				+"`field_rrp_currency_value` char(3) DEFAULT NULL,`field_supplier_cost_value` float DEFAULT NULL,`field_supplier_cost_currency_value` char(3) DEFAULT NULL,"
				+"`field_cogs_value` float DEFAULT NULL,`field_was_discounted_price_value` float DEFAULT NULL,`field_facebook_fans_choice_value` longtext,"
				+"`field_generic_name_value` longtext,`field_multi_wholesale_australia_value` decimal(10,2) DEFAULT NULL,"
				+"`field_multi_wholesale_hongkong_value` decimal(10,2) DEFAULT NULL,`field_multi_wholesale_indonesia_value` decimal(20,2) DEFAULT NULL,"
				+"`field_multi_wholesale_malaysia_value` decimal(10,2) DEFAULT NULL,`field_multi_wholesale_korea_value` decimal(20,2) DEFAULT NULL,"
				+"`field_multi_wholesale_taiwan_value` decimal(10,2) DEFAULT NULL,`field_multi_wholesale_brunei_value` decimal(10,2) DEFAULT NULL,"
				+"`field_multi_was_australia2_value` decimal(10,2) DEFAULT NULL,`field_multi_was_hongkong2_value` decimal(10,2) DEFAULT NULL,"
				+"`field_multi_was_indonesia2_value` decimal(20,2) DEFAULT NULL,`field_multi_was_malaysia2_value` decimal(10,2) DEFAULT NULL,"
				+"`field_multi_was_korea2_value` decimal(20,2) DEFAULT NULL,`field_multi_was_taiwan2_value` decimal(10,2) DEFAULT NULL,"
				+"`field_multi_was_brunei2_value` decimal(10,2) DEFAULT NULL,`field_multi_was_disc_australia_value` decimal(10,2) DEFAULT NULL,"
				+"`field_multi_was_disc_hongkong_value` decimal(10,2) DEFAULT NULL,`field_multi_was_disc_indonesia_value` decimal(20,2) DEFAULT NULL,"
				+"`field_multi_was_disc_malaysia_value` decimal(10,2) DEFAULT NULL,`field_multi_was_disc_korea_value` decimal(20,2) DEFAULT NULL,"
				+"`field_multi_was_disc_taiwan_value` decimal(10,2) DEFAULT NULL,`field_multi_was_disc_brunei_value` decimal(10,2) DEFAULT NULL,"
				+"`field_multi_selling_australia_value` decimal(10,2) DEFAULT NULL,`field_multi_selling_hongkong_value` decimal(10,2) DEFAULT NULL,"
				+"`field_multi_selling_indonesia_value` decimal(20,2) DEFAULT NULL,`field_multi_selling_malaysia_value` decimal(10,2) DEFAULT NULL,"
				+"`field_multi_selling_korea_value` decimal(20,2) DEFAULT NULL,`field_multi_selling_taiwan_value` decimal(10,2) DEFAULT NULL,"
				+"`field_multi_selling_brunei_value` decimal(10,2) DEFAULT NULL,`field_name_australia_value` longtext,`field_name_hongkong_value` longtext,"
				+"`field_name_indonesia_value` longtext,`field_name_malaysia_value` longtext,`field_name_korea_value` longtext,`field_name_taiwan_value` longtext,"
				+"`field_name_brunei_value` longtext,`field_desc_australia_value` longtext,`field_desc_hongkong_value` longtext,`field_desc_indonesia_value` longtext,"
				+"`field_desc_malaysia_value` longtext,`field_desc_korea_value` longtext,`field_desc_taiwan_value` longtext,`field_desc_brunei_value` longtext,"
				+"`field_name_thailand_value` longtext,`field_name_philippin_value` longtext,`field_desc_thailand_value` longtext,`field_desc_philippin_value` longtext,"
				+"`field_multi_wholesale_thailand_value` decimal(10,2) DEFAULT NULL,`field_multi_wholesale_philippin_value` decimal(10,2) DEFAULT NULL,"
				+"`field_multi_was_thailand2_value` decimal(10,2) DEFAULT NULL,`field_multi_was_philippin2_value` decimal(10,2) DEFAULT NULL,"
				+"`field_multi_was_disc_thailand_value` decimal(10,2) DEFAULT NULL,`field_multi_was_disc_philippin_value` decimal(10,2) DEFAULT NULL,"
				+"`field_multi_selling_thailand_value` decimal(10,2) DEFAULT NULL,`field_multi_selling_philippin_value` decimal(10,2) DEFAULT NULL,`field_stylist_pick_value` longtext,"
				+"`field_bagaholic_boys_choice_value` longtext,`field_grade_value` longtext,`field_rarity_guide_value` longtext,`field_max_qty_city_product_value` longtext,"
				+"`field_package_city_value` longtext,`field_name_newzealand_value` longtext,`field_desc_newzealand_value` longtext,"
				+"`field_multi_wholesale_newzealand_value` decimal(10,2) DEFAULT NULL,`field_multi_was_newzealand2_value` decimal(10,2) DEFAULT NULL,"
				+"`field_multi_was_disc_newzealand_value` decimal(10,2) DEFAULT NULL,`field_multi_selling_newzealand_value` decimal(10,2) DEFAULT NULL,"
				+"`field_multi_wholesale_canada_value` decimal(10,2) DEFAULT NULL,`field_multi_was_canada2_value` decimal(10,2) DEFAULT NULL,"
				+"`field_multi_was_disc_canada_value` decimal(10,2) DEFAULT NULL,`field_multi_selling_canada_value` decimal(10,2) DEFAULT NULL,`field_name_canada_value` longtext,"
				+"`field_desc_canada_value` longtext,`field_desc_usa_value` longtext CHARACTER SET utf8 COLLATE utf8_unicode_ci,"
				+"`field_name_usa_value` longtext CHARACTER SET utf8 COLLATE utf8_unicode_ci,`field_multi_wholesale_usa_value` decimal(10,2) DEFAULT NULL,"
				+"`field_multi_selling_usa_value` decimal(10,2) DEFAULT NULL,`field_multi_was_usa2_value` decimal(10,2) DEFAULT NULL,`field_multi_was_disc_usa_value` decimal(10,2) DEFAULT NULL,"
				+"`field_name_china_value` longtext,`field_desc_china_value` longtext,`field_multi_wholesale_china_value` decimal(10,2) DEFAULT NULL,`field_multi_was_china2_value` decimal(10,2) DEFAULT NULL,"
				+"`field_multi_was_disc_china_value` decimal(10,2) DEFAULT NULL,`field_multi_selling_china_value` decimal(10,2) DEFAULT NULL,"
				+"`field_desc_uae_value` longtext CHARACTER SET utf8 COLLATE utf8_unicode_ci,`field_name_uae_value` longtext CHARACTER SET utf8 COLLATE utf8_unicode_ci,"
				+"`field_multi_wholesale_uae_value` decimal(10,2) DEFAULT NULL,`field_multi_selling_uae_value` decimal(10,2) DEFAULT NULL,`field_multi_was_uae2_value` decimal(10,2) DEFAULT NULL,"
				+"`field_multi_was_disc_uae_value` decimal(10,2) DEFAULT NULL,`field_desc_arabia_value` longtext CHARACTER SET utf8 COLLATE utf8_unicode_ci,"
				+"`field_name_arabia_value` longtext CHARACTER SET utf8 COLLATE utf8_unicode_ci,`field_multi_wholesale_arabia_value` decimal(10,2) DEFAULT NULL,`field_multi_selling_arabia_value` decimal(10,2) DEFAULT NULL,`field_multi_was_arabia2_value` decimal(10,2) DEFAULT NULL,`field_multi_was_disc_arabia_value` decimal(10,2) DEFAULT NULL,`field_show_in_malaysia_value` int(11) DEFAULT '1',`field_show_in_indonesia_value` int(11) DEFAULT '1',`field_show_in_taiwan_value` int(11) DEFAULT '1',`field_show_in_korea_value` int(11) DEFAULT '1',`field_show_in_thailand_value` int(11) DEFAULT '1',`field_show_in_brunei_value` int(11) DEFAULT '1',`field_show_in_philippines_value` int(11) DEFAULT '1',`field_show_in_newzealand_value` int(11) DEFAULT '1',`field_show_in_arabia_value` int(11) DEFAULT '1',`field_show_in_canada_value` int(11) DEFAULT '1',`field_show_in_usa_value` int(11) DEFAULT '1',`field_show_in_uae_value` int(11) DEFAULT '1',`field_show_in_china_value` int(11) DEFAULT '1',`field_show_in_australia_value` int(11) DEFAULT '1',`field_show_in_hongkong_value` int(11) DEFAULT '1',`field_name_macau_value` longtext,`field_desc_macau_value` longtext,`field_multi_wholesale_macau_value` decimal(10,2) DEFAULT NULL,`field_multi_was_macau2_value` decimal(10,2) DEFAULT NULL,`field_multi_was_disc_macau_value` decimal(10,2) DEFAULT NULL,`field_multi_selling_macau_value` decimal(10,2) DEFAULT NULL,`field_show_in_macau_value` int(11) DEFAULT NULL,`field_name_india_value` longtext,`field_desc_india_value` longtext,`field_multi_wholesale_india_value` decimal(10,2) DEFAULT NULL,`field_multi_was_india2_value` decimal(10,2) DEFAULT NULL,`field_multi_was_disc_india_value` decimal(10,2) DEFAULT NULL,`field_multi_selling_india_value` decimal(10,2) DEFAULT NULL,`field_show_in_india_value` int(11) DEFAULT NULL,`field_multi_selling_japan_value` decimal(20,2) DEFAULT NULL,`field_multi_was_disc_japan_value` decimal(20,2) DEFAULT NULL,`field_multi_was_japan2_value` decimal(20,2) DEFAULT NULL,`field_multi_wholesale_japan_value` decimal(20,2) DEFAULT NULL,"
				+"`field_desc_japan_value` longtext,`field_name_japan_value` longtext,`field_show_in_japan_value` int(11) DEFAULT NULL,`field_name_uk_value` longtext,`field_desc_uk_value` longtext,`field_multi_wholesale_uk_value` decimal(10,2) DEFAULT NULL,`field_multi_was_uk2_value` decimal(10,2) DEFAULT NULL,`field_multi_was_disc_uk_value` decimal(10,2) DEFAULT NULL,`field_multi_selling_uk_value` decimal(10,2) DEFAULT NULL,`field_show_in_uk_value` int(11) DEFAULT NULL,`field_name_vietnam_value` longtext,`field_desc_vietnam_value` longtext,`field_multi_wholesale_vietnam_value` decimal(20,2) DEFAULT NULL,`field_multi_was_vietnam2_value` decimal(20,2) DEFAULT NULL,`field_multi_was_disc_vietnam_value` decimal(20,2) DEFAULT NULL,`field_multi_selling_vietnam_value` decimal(20,2) DEFAULT NULL,`field_show_in_vietnam_value` int(11) DEFAULT NULL,`field_show_in_singapore_value` int(11) DEFAULT NULL,`field_shipping_package_code_value` longtext,`field_lux_watch_gender_value` longtext,`field_lux_watch_category_value` longtext,`field_show_in_kuwait_value` int(11) DEFAULT NULL,`field_multi_selling_kuwait_value` decimal(10,2) DEFAULT NULL,`field_name_kuwait_value` longtext,`field_desc_kuwait_value` longtext,`field_multi_wholesale_kuwait_value` decimal(10,2) DEFAULT NULL,`field_multi_was_disc_kuwait_value` decimal(10,2) DEFAULT NULL,`field_multi_was_kuwait2_value` decimal(10,2) DEFAULT NULL,`field_virtual_location_value` longtext,PRIMARY KEY (`vid`),KEY `nid` (`nid`),KEY `field_event_nid` (`field_event_nid`),KEY `field_shipping_package_nid` (`field_shipping_package_nid`),KEY `field_supplier_sku_value` (`field_supplier_sku_value`),KEY `field_product_type_value` (`field_product_type_value`),KEY `field_item_list_fid` (`field_item_list_fid`),KEY `field_item_list_list` (`field_item_list_list`),KEY `field_tag_no_value` (`field_tag_no_value`)) ENGINE=MyISAM DEFAULT CHARSET=utf8 /*#dve RANGE DISTRIBUTE ON (`nid`) USING `node_range` */",
				"CREATE TABLE if not exists `uc_products` (`vid` int(10) unsigned NOT NULL DEFAULT '0',`nid` int(10) unsigned NOT NULL DEFAULT '0',`model` varchar(255) NOT NULL DEFAULT '',`list_price` decimal(15,3) NOT NULL DEFAULT '0.000',`cost` decimal(15,3) NOT NULL DEFAULT '0.000',`sell_price` decimal(15,3) NOT NULL DEFAULT '0.000',`weight` float NOT NULL DEFAULT '0',`weight_units` varchar(255) NOT NULL DEFAULT 'lb',`length` float NOT NULL DEFAULT '0',`width` float NOT NULL DEFAULT '0',`height` float NOT NULL DEFAULT '0',`length_units` varchar(255) NOT NULL DEFAULT 'in',`pkg_qty` smallint(5) unsigned NOT NULL DEFAULT '1',`default_qty` smallint(5) unsigned NOT NULL DEFAULT '1',`unique_hash` varchar(32) NOT NULL DEFAULT 'd41d8cd98f00b204e9800998ecf8427e',`ordering` tinyint(4) NOT NULL DEFAULT '0',`shippable` tinyint(3) unsigned NOT NULL DEFAULT '1',PRIMARY KEY (`vid`),KEY `nid` (`nid`),KEY `model` (`model`)) ENGINE=MyISAM DEFAULT CHARSET=utf8 /*#dve RANGE DISTRIBUTE ON (`nid`) USING `node_range` */"
			};
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		for(String decl : decls) {
			tests.add(new StatementMirrorProc(decl));
		}
		tests.add(new StatementMirrorProc("SELECT sum( p.field_cogs_value * op.qty )  AS cogs FROM `uc_orders` AS o, `uc_order_products` AS op " +
				"INNER JOIN `content_type_product` AS p ON p.nid = op.nid " +
				"INNER JOIN `uc_products` AS ucp ON ucp.nid = op.nid " +
				"WHERE op.order_id = o.order_id AND op.nid > 1 AND p.field_event_nid = 2015328 AND o.delivery_country = 841 AND o.order_status IN ( 'payment_received','completed','goods_returned','on_delivery','processing','packaging','unfulfilled' )"));
		runTest(tests);
	}

	@Test
	public void testPE796() throws Throwable {
		String[] decls = new String[] { 
				"/*#dve create range if not exists order_range (int) persistent group " + sysDDL.getPersistentGroup().getName() + "*/",
				"/*#dve create range if not exists user_range (int) persistent group " + sysDDL.getPersistentGroup().getName() + "*/",
				"CREATE TABLE if not exists `uc_order_line_items` (  `line_item_id` int(10) unsigned NOT NULL AUTO_INCREMENT,  `order_id` int(10) unsigned NOT NULL DEFAULT '0',  `type` varchar(32) NOT NULL DEFAULT '',  `title` varchar(128) NOT NULL DEFAULT '',  `amount` decimal(15,3) NOT NULL DEFAULT '0.000',  `weight` smallint(6) NOT NULL DEFAULT '0',  `data` text,  `payment_currency` char(3) DEFAULT NULL,  `payment_currency_amount` decimal(15,3) DEFAULT NULL,  `credit_deduction_id` int(11) DEFAULT NULL,  `credit_category` varchar(20) DEFAULT NULL,  PRIMARY KEY (`line_item_id`),  KEY `order_id` (`order_id`),  FULLTEXT KEY `type` (`type`),  FULLTEXT KEY `title` (`title`)) ENGINE=MyISAM AUTO_INCREMENT=722189 DEFAULT CHARSET=utf8 /*#dve  RANGE DISTRIBUTE ON (`order_id`) USING `order_range` */",
				"CREATE TABLE if not exists `users` (`uid` int(10) unsigned NOT NULL,`name` varchar(60) NOT NULL DEFAULT '',`pass` varchar(32) NOT NULL DEFAULT '',`mail` varchar(64) DEFAULT '',`mode` tinyint(4) NOT NULL DEFAULT '0',`sort` tinyint(4) DEFAULT '0',`threshold` tinyint(4) DEFAULT '0',`theme` varchar(255) NOT NULL DEFAULT '',`signature` varchar(255) NOT NULL DEFAULT '',`created` int(11) NOT NULL DEFAULT '0',`access` int(11) NOT NULL DEFAULT '0',`login` int(11) NOT NULL DEFAULT '0',`status` tinyint(4) NOT NULL DEFAULT '0',`timezone` varchar(8) DEFAULT NULL,`language` varchar(12) NOT NULL DEFAULT '',`picture` varchar(255) NOT NULL DEFAULT '',`init` varchar(64) DEFAULT '',`data` longtext,`timezone_name` varchar(50) NOT NULL DEFAULT '',PRIMARY KEY (`uid`),CONSTRAINT `name` UNIQUE KEY `name` (`name`),KEY `access` (`access`),KEY `created` (`created`),KEY `mail` (`mail`),KEY `status` (`status`),KEY `pass` (`pass`(5)),KEY `mode` (`mode`),KEY `sort` (`sort`),KEY `threshold` (`threshold`),KEY `language` (`language`)) ENGINE=MyISAM DEFAULT CHARSET=utf8  /*#dve RANGE DISTRIBUTE ON (`uid`) USING `user_range` */",
				"CREATE TABLE if not exists `bs_user` (`uid` int(10) NOT NULL,`first_name` varchar(255) NOT NULL,`last_name` varchar(255) NOT NULL,`dob` varchar(10) NOT NULL,`occupation` varchar(50) NOT NULL,`occupation_new` int(11) NOT NULL DEFAULT '0',`country` varchar(255) NOT NULL,`city` varchar(255) NOT NULL,`gender` enum('m','f','') NOT NULL,`income_range` varchar(50) DEFAULT NULL,`education` varchar(50) DEFAULT NULL,`shopping_range` varchar(50) DEFAULT NULL,`address` text,`interest_view` longtext,`activation_status` int(1) NOT NULL DEFAULT '0',`activation_date` timestamp NULL DEFAULT NULL,`reg_type` enum('invitation_code','friendinvite_online','friendinvite_offline','waitinglist','invitation_code_link','friendinvite_link','promo_code','promo_code_link','personal_email','personal_email_special') NOT NULL,`invite_code` varchar(25) DEFAULT NULL,`referrer_member_uid` int(10) DEFAULT NULL,`credit` double NOT NULL DEFAULT '0',`credit_deduct` double NOT NULL DEFAULT '0',`vip` tinyint(4) NOT NULL DEFAULT '0',`personal_link_invite` varchar(200) DEFAULT NULL,`email_temp_message` text,`email_notification_event` char(1) NOT NULL DEFAULT 'y',`email_notification_promo` char(1) NOT NULL DEFAULT 'y',`email_notification_city_event` char(1) NOT NULL DEFAULT 'n',`email_notification_city_promo` char(1) NOT NULL DEFAULT 'n',`email_notification_travel_event` char(1) NOT NULL DEFAULT 'n',`email_notification_travel_promo` char(1) NOT NULL DEFAULT 'n',`email_notification_kwerkee_event` char(1) NOT NULL DEFAULT 'n',`email_notification_kwerkee_promo` char(1) NOT NULL DEFAULT 'n',`email_notification_vintage_event` char(1) NOT NULL,`email_notification_vintage_promo` char(1) NOT NULL,`title` enum('mr','mrs','ms','') DEFAULT NULL,`state` varchar(50) NOT NULL,`postal_code` varchar(10) NOT NULL,`town` varchar(50) NOT NULL,`mobile_phone_no` varchar(20) NOT NULL,`home_phone_no` varchar(20) NOT NULL,`other_phone_no` varchar(20) NOT NULL,`shoe_size` varchar(2) NOT NULL,`clothes_size` varchar(4) NOT NULL,`hear_from` text NOT NULL,`interested_in` text NOT NULL,`fave_brand` varchar(50) NOT NULL,`other_store` varchar(50) NOT NULL,`reminder_date` datetime DEFAULT NULL,`vertical_id` int(10) unsigned NOT NULL,`is_active_fashion` tinyint(1) NOT NULL,`is_active_city` tinyint(1) NOT NULL,`is_active_travel` tinyint(1) NOT NULL,`mxpp` int(1) NOT NULL DEFAULT '0',`fave_magazines` varchar(100) DEFAULT NULL,`fave_shopping_websites` varchar(100) DEFAULT NULL,`monthly_expend` varchar(30) DEFAULT NULL,`factor_to_visit` varchar(100) DEFAULT NULL,`suggest_improvement` varchar(100) DEFAULT NULL,`modified` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' ON UPDATE CURRENT_TIMESTAMP,`source_page` varchar(25) DEFAULT NULL,`source_media` varchar(25) DEFAULT NULL,`is_active_kwerky` tinyint(1) NOT NULL DEFAULT '0',`signup_method` varchar(25) DEFAULT NULL,`kwerkee_fb_share_purchase` char(1) DEFAULT 'n',`kwerkee_fb_share_fave` char(1) DEFAULT 'n',`kwerkee_fb_social` char(1) DEFAULT 'n',`kwerkee_twitter_social` char(1) DEFAULT NULL,`kwerkee_twitter_share_purchase` char(1) DEFAULT NULL,`kwerkee_twitter_share_fave` char(1) DEFAULT NULL,`source` varchar(25) DEFAULT NULL COMMENT 'store actual source string',PRIMARY KEY (`uid`),KEY `reg_type` (`reg_type`),KEY `activation_status` (`activation_status`),KEY `invite_code` (`invite_code`),KEY `referrer_member_uid` (`referrer_member_uid`),KEY `country` (`country`),KEY `occupation_new` (`occupation_new`),KEY `gender` (`gender`),KEY `income_range` (`income_range`),KEY `title` (`title`),KEY `activation_date` (`activation_date`),KEY `personal_link_invite` (`personal_link_invite`),KEY `vertical_id` (`vertical_id`),KEY `is_active_fashion` (`is_active_fashion`),KEY `is_active_city` (`is_active_city`),KEY `is_active_travel` (`is_active_travel`),KEY `email_notification_kwerkee_event` (`email_notification_kwerkee_event`, `email_notification_kwerkee_promo`),KEY `is_active_kwerky` (`is_active_kwerky`),KEY `kwerkee_twitter_social` (`kwerkee_twitter_social`),KEY `kwerkee_twitter_share_purchase` (`kwerkee_twitter_share_purchase`),KEY `kwerkee_twitter_share_fave` (`kwerkee_twitter_share_fave`),KEY `source` (`source`)) ENGINE=MyISAM DEFAULT CHARSET=utf8 /*#dve BROADCAST DISTRIBUTE */",
				"CREATE TABLE if not exists `uc_orders` (`order_id` int(10) unsigned NOT NULL  auto_increment ,`uid` int(10) unsigned NOT NULL  DEFAULT '0', "
				+"`order_status` varchar(32) NOT NULL  DEFAULT '' ,`order_total` decimal(15,3) NOT NULL  DEFAULT '0.000' ,"
				+"`primary_email` varchar(96) NOT NULL  DEFAULT '' ,`delivery_first_name` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`delivery_last_name` varchar(255) NOT NULL  DEFAULT '' ,`delivery_phone` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`delivery_company` varchar(255) NOT NULL  DEFAULT '' ,`delivery_street1` varchar(255) NOT NULL  DEFAULT '' ," 
				+"`delivery_street2` varchar(255) NOT NULL  DEFAULT '' ,`delivery_city` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`delivery_zone` mediumint(8) unsigned NOT NULL  DEFAULT '0' ,`delivery_postal_code` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`delivery_country` mediumint(8) unsigned NOT NULL  DEFAULT '0' ,`billing_first_name` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`billing_last_name` varchar(255) NOT NULL  DEFAULT '' ,`billing_phone` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`billing_company` varchar(255) NOT NULL  DEFAULT '' ,`billing_street1` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`billing_street2` varchar(255) NOT NULL  DEFAULT '' ,`billing_city` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`billing_zone` mediumint(8) unsigned NOT NULL  DEFAULT '0' ,`billing_postal_code` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`billing_country` mediumint(8) unsigned NOT NULL  DEFAULT '0' ,`payment_method` varchar(32) NOT NULL  DEFAULT '' ,"
				+"`data` text,`created` int(11) NOT NULL  DEFAULT '0' ,`modified` int(11) NOT NULL  DEFAULT '0' ,"
				+"`host` varchar(255) NOT NULL  DEFAULT '' ,`payment_currency` char(3) DEFAULT NULL  COMMENT 'reebonz order local currency',"
				+"`payment_currency_amount` decimal(15,3) DEFAULT NULL , PRIMARY KEY (`order_id`), KEY `uid` (`uid`), "
				+"KEY `order_status` (`order_status`), KEY `billing_country` (`billing_country`))"
				+" engine=myisam /*#dve range distribute on (`order_id`) using order_range */",
				"CREATE TABLE if not exists `uc_payment_imerchant_data` (  `pid` int(10) unsigned NOT NULL AUTO_INCREMENT,  `order_id` int(10) unsigned NOT NULL DEFAULT '0',  `oid_prefix` varchar(10) DEFAULT NULL,  `oid_suffix` varchar(5) DEFAULT NULL,  `environment` varchar(20) NOT NULL,  `cc_num_first6` varchar(6) NOT NULL,  `cc_num_last4` varchar(4) NOT NULL,  `cc_num` varchar(19) NOT NULL,  `cc_name` varchar(255) NOT NULL,  `vpc_amount` int(11) DEFAULT NULL,  `vpc_authorisedamount` int(11) DEFAULT '0',  `vpc_capturedamount` int(11) DEFAULT '0',  `vpc_refundedamount` int(11) DEFAULT '0',  `vpc_currency` char(3) DEFAULT NULL,  `vpc_message` varchar(255) DEFAULT NULL,  `vpc_txnresponsecode` char(1) DEFAULT NULL,  `vpc_receiptno` varchar(12) DEFAULT NULL,  `vpc_shoptransactionno` varchar(19) DEFAULT NULL,  `vpc_transactionno` varchar(19) DEFAULT NULL,  `vpc_authorizeid` varchar(6) DEFAULT NULL,  `vpc_card` varchar(16) DEFAULT NULL,  `vpc_cscresultcode` varchar(11) DEFAULT NULL,  `vpc_epp_available` varchar(1) DEFAULT NULL,  `vpc_epp_numbermonths` varchar(5) DEFAULT NULL,  `vpc_epp_monthlypayment` varchar(12) DEFAULT NULL,  `vpc_epp_interestrate` varchar(8) DEFAULT NULL,  `vpc_epp_duration` varchar(2) DEFAULT NULL,  `cron` tinyint(3) unsigned NOT NULL DEFAULT '0',  `cancel` tinyint(3) unsigned NOT NULL DEFAULT '0',  `created` int(10) unsigned NOT NULL,  `modified` int(10) unsigned NOT NULL,  PRIMARY KEY (`pid`),  KEY `cc_num_first6` (`cc_num_first6`),  KEY `cc_num_last4` (`cc_num_last4`),  KEY `order_id` (`order_id`)) ENGINE=MyISAM AUTO_INCREMENT=203529 DEFAULT CHARSET=utf8 /*#dve  RANGE DISTRIBUTE ON (`order_id`) USING `order_range` */",
				"CREATE TABLE if not exists `membership_user` (  `uid` int(10) NOT NULL,  `mid` int(10) NOT NULL,  `start` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,  `end` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',  `is_active` varchar(1) NOT NULL DEFAULT 'N',  `last_reminder` timestamp NULL DEFAULT NULL,  PRIMARY KEY (`uid`),  KEY `mid` (`mid`)) ENGINE=MyISAM DEFAULT CHARSET=utf8 /*#dve  BROADCAST DISTRIBUTE */",
				"CREATE TABLE if not exists `membership_type` (  `mid` int(11) NOT NULL AUTO_INCREMENT,  `name` varchar(100) DEFAULT NULL,  `description` varchar(100) DEFAULT NULL,  `rebate` int(11) DEFAULT NULL,  `early_prev` int(11) DEFAULT NULL,  `price` int(11) DEFAULT NULL,  `status` varchar(100) DEFAULT NULL,  `timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,  PRIMARY KEY (`mid`)) ENGINE=MyISAM AUTO_INCREMENT=3 DEFAULT CHARSET=utf8 /*#dve  BROADCAST DISTRIBUTE */",
				"CREATE TABLE if not exists `email_ver` (  `eid` int(11) NOT NULL AUTO_INCREMENT,  `uid` int(11) NOT NULL,  `email_content` varchar(32) NOT NULL,  `email_status` tinyint(4) NOT NULL,  `generate` varchar(64) NOT NULL,  `d_created` int(11) NOT NULL,  `d_activated` int(11) NOT NULL,  `mass_import` tinyint(4) NOT NULL DEFAULT '0',  PRIMARY KEY (`eid`),  KEY `uid` (`uid`),  KEY `generate` (`generate`)) ENGINE=MyISAM AUTO_INCREMENT=1781333 DEFAULT CHARSET=utf8 /*#dve  BROADCAST DISTRIBUTE */",
				"CREATE TABLE if not exists `bin` (  `bin` varchar(6) NOT NULL,  `card_brand` varchar(30) NOT NULL,  `organization` varchar(50) NOT NULL,  `card_type` varchar(30) NOT NULL,  `card_category` varchar(30) NOT NULL,  `country_iso_name` varchar(50) NOT NULL,  `country_iso_a2` varchar(30) NOT NULL,  `country_iso_a3` varchar(30) NOT NULL,  `country_iso_number` int(11) NOT NULL,  `organization_url` varchar(255) DEFAULT NULL,  `organization_phone` varchar(25) DEFAULT NULL,  KEY `bin_idx` (`bin`),  KEY `card_brand_idx` (`card_brand`(10)),  KEY `organization_idx` (`organization`(10)),  KEY `card_typ_idx` (`card_type`(10)),  KEY `country_iso_name_idx` (`country_iso_name`(10)),  KEY `country_iso_a2_idx` (`country_iso_a2`(10)),  KEY `country_iso_a3_idx` (`country_iso_a3`(10)),  KEY `country_iso_number_idx` (`country_iso_number`)) ENGINE=MyISAM DEFAULT CHARSET=utf8 /*#dve  BROADCAST DISTRIBUTE */",
				"CREATE TABLE if not exists `uc_order_vertical` (`order_id` int(11) NOT NULL DEFAULT '0',`vertical_id` int(11) DEFAULT NULL,PRIMARY KEY (`order_id`),KEY `vertical_id` (`vertical_id`)) ENGINE=MyISAM DEFAULT CHARSET=utf8 /*#dve RANGE DISTRIBUTE ON (`order_id`) USING `order_range` */",
				"CREATE TABLE if not exists `service_order_api` (  `so_id` int(10) NOT NULL AUTO_INCREMENT,  `order_id` int(11) NOT NULL,  `uid` int(11) NOT NULL,  `cartcontents` text COLLATE utf8_unicode_ci,  `billing` text COLLATE utf8_unicode_ci,  `transaction` text COLLATE utf8_unicode_ci,  `type` varchar(10) COLLATE utf8_unicode_ci DEFAULT NULL,  `created` int(11) DEFAULT NULL,  `updated` int(11) DEFAULT NULL,  PRIMARY KEY (`so_id`),  KEY `order_id` (`order_id`),  KEY `uid` (`uid`),  KEY `type` (`type`)) ENGINE=MyISAM AUTO_INCREMENT=36372 DEFAULT CHARSET=utf8 /*#dve  BROADCAST DISTRIBUTE */"
			};
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		for(String decl : decls) {
			tests.add(new StatementMirrorProc(decl));
		}
		tests.add(new StatementMirrorProc("SELECT t1.mail as EMAIL, t1.uid as USERID, t2.first_name as FIRSTNAME, t2.last_name as LASTNAME, from_unixtime(t1.created) as datetime_signup, from_unixtime(t1.access) as datetime_access, if(t8.email_status = 1, 'Y', 'N') as email_verified, ifnull(t2.title, '') as title, t2.gender, t2.country, t2.reg_type, ifnull(t2.invite_code, '') as invite_code, ifnull(t3.mail, '') as referrer_email, (select count(*) from bs_user bu where bu.referrer_member_uid = t1.uid  ) as friends_referred, ifnull(t7.name, '') as membership_type, t1.language as language_choice, (SELECT CASE t2.vertical_id WHEN 434 THEN 'Reebonz' WHEN 435 THEN 'Reebonz City' WHEN 2283 THEN 'Kwerkee' WHEN 2418 THEN 'eBay' ELSE '' END) as vertical_joined, if(t2.is_active_fashion = 1, 'y', 'n') as active_reebonz, if(t2.is_active_city = 1, 'y', 'n') as active_reebonzcity, if(t2.is_active_kwerky = 1, 'y', 'n') as active_kwerkee, t2.email_notification_event as newsletter_eventreminder, t2.email_notification_promo as newsletter_promotion, t2.email_notification_city_event as newsletter_city_eventreminder, t2.email_notification_city_promo as newsletter_city_promotion, t2.email_notification_kwerkee_event as newsletter_kwerkee_eventreminder, t2.email_notification_kwerkee_promo as newsletter_kwerkee_promotion, t2.dob as birthdate, t2.city as address_city, t2.`state` as address_state, t2.town as address_town, t2.postal_code as address_postalcode, ifnull(t2.address, '') as address, t2.mobile_phone_no as mobile_phone, t2.home_phone_no as home_phone, t2.other_phone_no as other_phone, ifnull(t4.order_id, '') as order_id, ifnull(from_unixtime(t4.created), '') as order_datetime, (SELECT CASE t11.vertical_id WHEN 434 THEN 'Reebonz' WHEN 435 THEN 'Reebonz City' WHEN 2283 THEN 'Kwerkee' WHEN 2418 THEN 'eBay' ELSE '' END) as order_vertical, ifnull(t12.type, 'web') as order_channel, ifnull(t4.order_total, '') as order_amount_sgd, ifnull(t4.payment_currency, '') as order_local_currency, ifnull(t4.payment_currency_amount, '') as order_amount_local,	ifnull(if(t4.order_id is not null, (select sum(abs(amount)) from uc_order_line_items where order_id = t4.order_id and type = 'ptdiscount'), ''), '') as order_credit_used_local, ifnull(if(t4.order_id is not null, (select sum(abs(amount)) from uc_order_line_items where order_id = t4.order_id and type = 'uc_discounts'), ''), '') as order_discount_used_local, ifnull(if(t4.order_id is not null, (select sum(abs(amount)) from uc_order_line_items where order_id = t4.order_id and type = 'ccdiscount'), ''), '') as order_creditcard_discount_used_local, ifnull(t4.payment_method, '') as payment_method, ifnull(t4.order_status, '') as order_status, ifnull(t4.delivery_first_name, '') as order_delivery_first_name, ifnull(t4.delivery_last_name, '') as order_delivery_last_name, ifnull(t4.delivery_phone, '') as order_delivery_phone, ifnull(t4.delivery_company, '') as order_delivery_company, ifnull(t4.delivery_street1, '') as order_delivery_street1, ifnull(t4.delivery_street2, '') as order_delivery_street2, ifnull(t4.delivery_city, '') as order_delivery_city, ifnull(t4.delivery_postal_code, '') as order_delivery_postal_code, t5.cc_num_first6 as bin_used, ifnull(t10.card_brand, '') as card_brand, ifnull(t10.organization, '') as card_organization, ifnull(t10.country_iso_name, '') as card_country FROM users t1 INNER JOIN bs_user t2 on t2.uid = t1.uid LEFT JOIN users t3 on t3.uid = t2.referrer_member_uid LEFT JOIN uc_orders t4 on t4.uid = t1.uid AND t4.order_status IN ('payment_received', 'packaging', 'completed', 'goods_returned', 'processing', 'on_delivery') LEFT JOIN uc_payment_imerchant_data t5 ON t5.order_id = t4.order_id LEFT JOIN membership_user t6 on t6.uid = t1.uid LEFT JOIN membership_type t7 on t7.mid = t6.mid LEFT JOIN email_ver t8 on t8.uid = t1.uid LEFT JOIN bin t10 on t10.bin = t5.cc_num_first6 INNER JOIN uc_order_vertical t11 on t11.order_id = t4.order_id LEFT JOIN service_order_api t12 on t12.order_id = t4.order_id WHERE t1.status = 1 AND t2.activation_status = 1 AND t2.country = 'Singapore' AND from_unixtime(t1.created) >= '2013-05-06 00:00:00' AND from_unixtime(t1.created) <= '2013-05-06 23:59:59' AND t2.email_notification_promo = 'y' AND t2.email_notification_city_promo = 'y' "));
		runTest(tests);
	}

	@Test
	public void testPE826() throws Throwable {
		String[] decls = new String[] { 
				"/*#dve create range if not exists order_range (int) persistent group " + sysDDL.getPersistentGroup().getName() + "*/",
				"/*#dve create range if not exists user_range (int) persistent group " + sysDDL.getPersistentGroup().getName() + "*/",
				"CREATE TABLE if not exists `users` (`uid` int(10) unsigned NOT NULL,`name` varchar(60) NOT NULL DEFAULT '',`pass` varchar(32) NOT NULL DEFAULT '',`mail` varchar(64) DEFAULT '',`mode` tinyint(4) NOT NULL DEFAULT '0',`sort` tinyint(4) DEFAULT '0',`threshold` tinyint(4) DEFAULT '0',`theme` varchar(255) NOT NULL DEFAULT '',`signature` varchar(255) NOT NULL DEFAULT '',`created` int(11) NOT NULL DEFAULT '0',`access` int(11) NOT NULL DEFAULT '0',`login` int(11) NOT NULL DEFAULT '0',`status` tinyint(4) NOT NULL DEFAULT '0',`timezone` varchar(8) DEFAULT NULL,`language` varchar(12) NOT NULL DEFAULT '',`picture` varchar(255) NOT NULL DEFAULT '',`init` varchar(64) DEFAULT '',`data` longtext,`timezone_name` varchar(50) NOT NULL DEFAULT '',PRIMARY KEY (`uid`),CONSTRAINT `name` UNIQUE KEY `name` (`name`),KEY `access` (`access`),KEY `created` (`created`),KEY `mail` (`mail`),KEY `status` (`status`),KEY `pass` (`pass`(5)),KEY `mode` (`mode`),KEY `sort` (`sort`),KEY `threshold` (`threshold`),KEY `language` (`language`)) ENGINE=MyISAM DEFAULT CHARSET=utf8  /*#dve RANGE DISTRIBUTE ON (`uid`) USING `user_range` */",
				"CREATE TABLE if not exists `uc_orders` (`order_id` int(10) unsigned NOT NULL  auto_increment ,`uid` int(10) unsigned NOT NULL  DEFAULT '0', "
				+"`order_status` varchar(32) NOT NULL  DEFAULT '' ,`order_total` decimal(15,3) NOT NULL  DEFAULT '0.000' ,"
				+"`primary_email` varchar(96) NOT NULL  DEFAULT '' ,`delivery_first_name` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`delivery_last_name` varchar(255) NOT NULL  DEFAULT '' ,`delivery_phone` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`delivery_company` varchar(255) NOT NULL  DEFAULT '' ,`delivery_street1` varchar(255) NOT NULL  DEFAULT '' ," 
				+"`delivery_street2` varchar(255) NOT NULL  DEFAULT '' ,`delivery_city` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`delivery_zone` mediumint(8) unsigned NOT NULL  DEFAULT '0' ,`delivery_postal_code` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`delivery_country` mediumint(8) unsigned NOT NULL  DEFAULT '0' ,`billing_first_name` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`billing_last_name` varchar(255) NOT NULL  DEFAULT '' ,`billing_phone` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`billing_company` varchar(255) NOT NULL  DEFAULT '' ,`billing_street1` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`billing_street2` varchar(255) NOT NULL  DEFAULT '' ,`billing_city` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`billing_zone` mediumint(8) unsigned NOT NULL  DEFAULT '0' ,`billing_postal_code` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`billing_country` mediumint(8) unsigned NOT NULL  DEFAULT '0' ,`payment_method` varchar(32) NOT NULL  DEFAULT '' ,"
				+"`data` text,`created` int(11) NOT NULL  DEFAULT '0' ,`modified` int(11) NOT NULL  DEFAULT '0' ,"
				+"`host` varchar(255) NOT NULL  DEFAULT '' ,`payment_currency` char(3) DEFAULT NULL  COMMENT 'reebonz order local currency',"
				+"`payment_currency_amount` decimal(15,3) DEFAULT NULL , PRIMARY KEY (`order_id`), KEY `uid` (`uid`), "
				+"KEY `order_status` (`order_status`), KEY `billing_country` (`billing_country`))"
				+" engine=myisam /*#dve range distribute on (`order_id`) using order_range */"
			};
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		for(String decl : decls) {
			tests.add(new StatementMirrorProc(decl));
		}
		tests.add(new StatementMirrorProc("select x.count_p, count(1) as count_u from (SELECT a.uid, case when count(distinct b.order_id) >2 and count(distinct b.order_id)<=5 then 5 when count(distinct b.order_id)>=6 and count(distinct b.order_id) <=10 then 10 when count(distinct b.order_id)>10 then 11 else count(distinct b.order_id) end as count_p FROM users a  INNER JOIN uc_orders b on a.uid=b.uid WHERE from_unixtime(b.created) < '2013-03-01 00:00:00' AND from_unixtime(a.created) between '2013-03-01/00:00:00' and '2013-04-01/00:00:00' AND b.order_status in ('completed','on_delivery','packaging','payment_received','processing') group by a.uid) x group by x.count_p"));
		runTest(tests);
	}

	@Test
	public void testPE830() throws Throwable {
		String[] decls = new String[] { 
				"/*#dve create range if not exists order_range (int) persistent group " + sysDDL.getPersistentGroup().getName() + "*/",
				"/*#dve create range if not exists user_range (int) persistent group " + sysDDL.getPersistentGroup().getName() + "*/",
				"CREATE TABLE if not exists `users` (`uid` int(10) unsigned NOT NULL,`name` varchar(60) NOT NULL DEFAULT '',`pass` varchar(32) NOT NULL DEFAULT '',`mail` varchar(64) DEFAULT '',`mode` tinyint(4) NOT NULL DEFAULT '0',`sort` tinyint(4) DEFAULT '0',`threshold` tinyint(4) DEFAULT '0',`theme` varchar(255) NOT NULL DEFAULT '',`signature` varchar(255) NOT NULL DEFAULT '',`created` int(11) NOT NULL DEFAULT '0',`access` int(11) NOT NULL DEFAULT '0',`login` int(11) NOT NULL DEFAULT '0',`status` tinyint(4) NOT NULL DEFAULT '0',`timezone` varchar(8) DEFAULT NULL,`language` varchar(12) NOT NULL DEFAULT '',`picture` varchar(255) NOT NULL DEFAULT '',`init` varchar(64) DEFAULT '',`data` longtext,`timezone_name` varchar(50) NOT NULL DEFAULT '',PRIMARY KEY (`uid`),CONSTRAINT `name` UNIQUE KEY `name` (`name`),KEY `access` (`access`),KEY `created` (`created`),KEY `mail` (`mail`),KEY `status` (`status`),KEY `pass` (`pass`(5)),KEY `mode` (`mode`),KEY `sort` (`sort`),KEY `threshold` (`threshold`),KEY `language` (`language`)) ENGINE=MyISAM DEFAULT CHARSET=utf8  /*#dve RANGE DISTRIBUTE ON (`uid`) USING `user_range` */",
				"CREATE TABLE if not exists `bs_user` (`uid` int(10) NOT NULL,`first_name` varchar(255) NOT NULL,`last_name` varchar(255) NOT NULL,`dob` varchar(10) NOT NULL,`occupation` varchar(50) NOT NULL,`occupation_new` int(11) NOT NULL DEFAULT '0',`country` varchar(255) NOT NULL,`city` varchar(255) NOT NULL,`gender` enum('m','f','') NOT NULL,`income_range` varchar(50) DEFAULT NULL,`education` varchar(50) DEFAULT NULL,`shopping_range` varchar(50) DEFAULT NULL,`address` text,`interest_view` longtext,`activation_status` int(1) NOT NULL DEFAULT '0',`activation_date` timestamp NULL DEFAULT NULL,`reg_type` enum('invitation_code','friendinvite_online','friendinvite_offline','waitinglist','invitation_code_link','friendinvite_link','promo_code','promo_code_link','personal_email','personal_email_special') NOT NULL,`invite_code` varchar(25) DEFAULT NULL,`referrer_member_uid` int(10) DEFAULT NULL,`credit` double NOT NULL DEFAULT '0',`credit_deduct` double NOT NULL DEFAULT '0',`vip` tinyint(4) NOT NULL DEFAULT '0',`personal_link_invite` varchar(200) DEFAULT NULL,`email_temp_message` text,`email_notification_event` char(1) NOT NULL DEFAULT 'y',`email_notification_promo` char(1) NOT NULL DEFAULT 'y',`email_notification_city_event` char(1) NOT NULL DEFAULT 'n',`email_notification_city_promo` char(1) NOT NULL DEFAULT 'n',`email_notification_travel_event` char(1) NOT NULL DEFAULT 'n',`email_notification_travel_promo` char(1) NOT NULL DEFAULT 'n',`email_notification_kwerkee_event` char(1) NOT NULL DEFAULT 'n',`email_notification_kwerkee_promo` char(1) NOT NULL DEFAULT 'n',`email_notification_vintage_event` char(1) NOT NULL,`email_notification_vintage_promo` char(1) NOT NULL,`title` enum('mr','mrs','ms','') DEFAULT NULL,`state` varchar(50) NOT NULL,`postal_code` varchar(10) NOT NULL,`town` varchar(50) NOT NULL,`mobile_phone_no` varchar(20) NOT NULL,`home_phone_no` varchar(20) NOT NULL,`other_phone_no` varchar(20) NOT NULL,`shoe_size` varchar(2) NOT NULL,`clothes_size` varchar(4) NOT NULL,`hear_from` text NOT NULL,`interested_in` text NOT NULL,`fave_brand` varchar(50) NOT NULL,`other_store` varchar(50) NOT NULL,`reminder_date` datetime DEFAULT NULL,`vertical_id` int(10) unsigned NOT NULL,`is_active_fashion` tinyint(1) NOT NULL,`is_active_city` tinyint(1) NOT NULL,`is_active_travel` tinyint(1) NOT NULL,`mxpp` int(1) NOT NULL DEFAULT '0',`fave_magazines` varchar(100) DEFAULT NULL,`fave_shopping_websites` varchar(100) DEFAULT NULL,`monthly_expend` varchar(30) DEFAULT NULL,`factor_to_visit` varchar(100) DEFAULT NULL,`suggest_improvement` varchar(100) DEFAULT NULL,`modified` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' ON UPDATE CURRENT_TIMESTAMP,`source_page` varchar(25) DEFAULT NULL,`source_media` varchar(25) DEFAULT NULL,`is_active_kwerky` tinyint(1) NOT NULL DEFAULT '0',`signup_method` varchar(25) DEFAULT NULL,`kwerkee_fb_share_purchase` char(1) DEFAULT 'n',`kwerkee_fb_share_fave` char(1) DEFAULT 'n',`kwerkee_fb_social` char(1) DEFAULT 'n',`kwerkee_twitter_social` char(1) DEFAULT NULL,`kwerkee_twitter_share_purchase` char(1) DEFAULT NULL,`kwerkee_twitter_share_fave` char(1) DEFAULT NULL,`source` varchar(25) DEFAULT NULL COMMENT 'store actual source string',PRIMARY KEY (`uid`),KEY `reg_type` (`reg_type`),KEY `activation_status` (`activation_status`),KEY `invite_code` (`invite_code`),KEY `referrer_member_uid` (`referrer_member_uid`),KEY `country` (`country`),KEY `occupation_new` (`occupation_new`),KEY `gender` (`gender`),KEY `income_range` (`income_range`),KEY `title` (`title`),KEY `activation_date` (`activation_date`),KEY `personal_link_invite` (`personal_link_invite`),KEY `vertical_id` (`vertical_id`),KEY `is_active_fashion` (`is_active_fashion`),KEY `is_active_city` (`is_active_city`),KEY `is_active_travel` (`is_active_travel`),KEY `email_notification_kwerkee_event` (`email_notification_kwerkee_event`, `email_notification_kwerkee_promo`),KEY `is_active_kwerky` (`is_active_kwerky`),KEY `kwerkee_twitter_social` (`kwerkee_twitter_social`),KEY `kwerkee_twitter_share_purchase` (`kwerkee_twitter_share_purchase`),KEY `kwerkee_twitter_share_fave` (`kwerkee_twitter_share_fave`),KEY `source` (`source`)) ENGINE=MyISAM DEFAULT CHARSET=utf8 /*#dve BROADCAST DISTRIBUTE */",
				"CREATE TABLE if not exists `uc_orders` (`order_id` int(10) unsigned NOT NULL  auto_increment ,`uid` int(10) unsigned NOT NULL  DEFAULT '0', "
				+"`order_status` varchar(32) NOT NULL  DEFAULT '' ,`order_total` decimal(15,3) NOT NULL  DEFAULT '0.000' ,"
				+"`primary_email` varchar(96) NOT NULL  DEFAULT '' ,`delivery_first_name` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`delivery_last_name` varchar(255) NOT NULL  DEFAULT '' ,`delivery_phone` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`delivery_company` varchar(255) NOT NULL  DEFAULT '' ,`delivery_street1` varchar(255) NOT NULL  DEFAULT '' ," 
				+"`delivery_street2` varchar(255) NOT NULL  DEFAULT '' ,`delivery_city` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`delivery_zone` mediumint(8) unsigned NOT NULL  DEFAULT '0' ,`delivery_postal_code` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`delivery_country` mediumint(8) unsigned NOT NULL  DEFAULT '0' ,`billing_first_name` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`billing_last_name` varchar(255) NOT NULL  DEFAULT '' ,`billing_phone` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`billing_company` varchar(255) NOT NULL  DEFAULT '' ,`billing_street1` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`billing_street2` varchar(255) NOT NULL  DEFAULT '' ,`billing_city` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`billing_zone` mediumint(8) unsigned NOT NULL  DEFAULT '0' ,`billing_postal_code` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`billing_country` mediumint(8) unsigned NOT NULL  DEFAULT '0' ,`payment_method` varchar(32) NOT NULL  DEFAULT '' ,"
				+"`data` text,`created` int(11) NOT NULL  DEFAULT '0' ,`modified` int(11) NOT NULL  DEFAULT '0' ,"
				+"`host` varchar(255) NOT NULL  DEFAULT '' ,`payment_currency` char(3) DEFAULT NULL  COMMENT 'reebonz order local currency',"
				+"`payment_currency_amount` decimal(15,3) DEFAULT NULL , PRIMARY KEY (`order_id`), KEY `uid` (`uid`), "
				+"KEY `order_status` (`order_status`), KEY `billing_country` (`billing_country`))"
				+" engine=myisam /*#dve range distribute on (`order_id`) using order_range */"
			};
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		for(String decl : decls) {
			tests.add(new StatementMirrorProc(decl));
		}
		tests.add(new StatementMirrorProc("select count(1) as cnt, x.last_purchase_day "
				+"from (SELECT a.uid, "
				+"  CASE WHEN (DATEDIFF('2013-01-01 00:00:00',from_unixtime(d.last_purchase))  < 90) then 'a' "
				+"       WHEN (DATEDIFF('2013-01-01 00:00:00',from_unixtime(d.last_purchase)) >=90 "
				+"       and DATEDIFF('2013-01-01 00:00:00',from_unixtime(d.last_purchase))  <180 ) then 'b' else 'c' end as last_purchase_day"
				+"  FROM users a INNER JOIN uc_orders b on a.uid=b.uid INNER JOIN (SELECT max(d.created) as last_purchase,d.uid from uc_orders d "
				+"  where from_unixtime(d.created) < '2013-01-01 00:00:00' "
				+"  AND d.order_status in ('completed','on_delivery','packaging','payment_received','processing') group by d.uid) d on a.uid = d.uid "
				+"INNER JOIN bs_user x on a.uid=x.uid WHERE from_unixtime(a.created) between '2013-03-01/00:00:00' and '2013-04-01/00:00:00' "
				+"AND from_unixtime(b.created) between '2013-01-01 00:00:00' and '2013-02-01 00:00:00' "
				+"AND b.order_status in ('completed','on_delivery','packaging','payment_received','processing')) x group by x.last_purchase_day;"));
		runTest(tests);
	}

	@Test
	public void testPE876() throws Throwable {
		String[] decls = new String[] { 
				"/*#dve create range if not exists order_range (int) persistent group " + sysDDL.getPersistentGroup().getName() + "*/",
				"/*#dve create range if not exists user_range (int) persistent group " + sysDDL.getPersistentGroup().getName() + "*/",
				"CREATE TABLE if not exists `bs_user` (`uid` int(10) NOT NULL,`first_name` varchar(255) NOT NULL,`last_name` varchar(255) NOT NULL,`dob` varchar(10) NOT NULL,`occupation` varchar(50) NOT NULL,`occupation_new` int(11) NOT NULL DEFAULT '0',`country` varchar(255) NOT NULL,`city` varchar(255) NOT NULL,`gender` enum('m','f','') NOT NULL,`income_range` varchar(50) DEFAULT NULL,`education` varchar(50) DEFAULT NULL,`shopping_range` varchar(50) DEFAULT NULL,`address` text,`interest_view` longtext,`activation_status` int(1) NOT NULL DEFAULT '0',`activation_date` timestamp NULL DEFAULT NULL,`reg_type` enum('invitation_code','friendinvite_online','friendinvite_offline','waitinglist','invitation_code_link','friendinvite_link','promo_code','promo_code_link','personal_email','personal_email_special') NOT NULL,`invite_code` varchar(25) DEFAULT NULL,`referrer_member_uid` int(10) DEFAULT NULL,`credit` double NOT NULL DEFAULT '0',`credit_deduct` double NOT NULL DEFAULT '0',`vip` tinyint(4) NOT NULL DEFAULT '0',`personal_link_invite` varchar(200) DEFAULT NULL,`email_temp_message` text,`email_notification_event` char(1) NOT NULL DEFAULT 'y',`email_notification_promo` char(1) NOT NULL DEFAULT 'y',`email_notification_city_event` char(1) NOT NULL DEFAULT 'n',`email_notification_city_promo` char(1) NOT NULL DEFAULT 'n',`email_notification_travel_event` char(1) NOT NULL DEFAULT 'n',`email_notification_travel_promo` char(1) NOT NULL DEFAULT 'n',`email_notification_kwerkee_event` char(1) NOT NULL DEFAULT 'n',`email_notification_kwerkee_promo` char(1) NOT NULL DEFAULT 'n',`email_notification_vintage_event` char(1) NOT NULL,`email_notification_vintage_promo` char(1) NOT NULL,`title` enum('mr','mrs','ms','') DEFAULT NULL,`state` varchar(50) NOT NULL,`postal_code` varchar(10) NOT NULL,`town` varchar(50) NOT NULL,`mobile_phone_no` varchar(20) NOT NULL,`home_phone_no` varchar(20) NOT NULL,`other_phone_no` varchar(20) NOT NULL,`shoe_size` varchar(2) NOT NULL,`clothes_size` varchar(4) NOT NULL,`hear_from` text NOT NULL,`interested_in` text NOT NULL,`fave_brand` varchar(50) NOT NULL,`other_store` varchar(50) NOT NULL,`reminder_date` datetime DEFAULT NULL,`vertical_id` int(10) unsigned NOT NULL,`is_active_fashion` tinyint(1) NOT NULL,`is_active_city` tinyint(1) NOT NULL,`is_active_travel` tinyint(1) NOT NULL,`mxpp` int(1) NOT NULL DEFAULT '0',`fave_magazines` varchar(100) DEFAULT NULL,`fave_shopping_websites` varchar(100) DEFAULT NULL,`monthly_expend` varchar(30) DEFAULT NULL,`factor_to_visit` varchar(100) DEFAULT NULL,`suggest_improvement` varchar(100) DEFAULT NULL,`modified` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' ON UPDATE CURRENT_TIMESTAMP,`source_page` varchar(25) DEFAULT NULL,`source_media` varchar(25) DEFAULT NULL,`is_active_kwerky` tinyint(1) NOT NULL DEFAULT '0',`signup_method` varchar(25) DEFAULT NULL,`kwerkee_fb_share_purchase` char(1) DEFAULT 'n',`kwerkee_fb_share_fave` char(1) DEFAULT 'n',`kwerkee_fb_social` char(1) DEFAULT 'n',`kwerkee_twitter_social` char(1) DEFAULT NULL,`kwerkee_twitter_share_purchase` char(1) DEFAULT NULL,`kwerkee_twitter_share_fave` char(1) DEFAULT NULL,`source` varchar(25) DEFAULT NULL COMMENT 'store actual source string',PRIMARY KEY (`uid`),KEY `reg_type` (`reg_type`),KEY `activation_status` (`activation_status`),KEY `invite_code` (`invite_code`),KEY `referrer_member_uid` (`referrer_member_uid`),KEY `country` (`country`),KEY `occupation_new` (`occupation_new`),KEY `gender` (`gender`),KEY `income_range` (`income_range`),KEY `title` (`title`),KEY `activation_date` (`activation_date`),KEY `personal_link_invite` (`personal_link_invite`),KEY `vertical_id` (`vertical_id`),KEY `is_active_fashion` (`is_active_fashion`),KEY `is_active_city` (`is_active_city`),KEY `is_active_travel` (`is_active_travel`),KEY `email_notification_kwerkee_event` (`email_notification_kwerkee_event`, `email_notification_kwerkee_promo`),KEY `is_active_kwerky` (`is_active_kwerky`),KEY `kwerkee_twitter_social` (`kwerkee_twitter_social`),KEY `kwerkee_twitter_share_purchase` (`kwerkee_twitter_share_purchase`),KEY `kwerkee_twitter_share_fave` (`kwerkee_twitter_share_fave`),KEY `source` (`source`)) ENGINE=MyISAM DEFAULT CHARSET=utf8 /*#dve BROADCAST DISTRIBUTE */",
				"CREATE TABLE if not exists `user_histories` (`id` int(11) NOT NULL,`userid` int(10) DEFAULT NULL,`datetime_added` datetime DEFAULT NULL,`event_id` int(10) DEFAULT NULL,`product_id` int(10) DEFAULT NULL,`product_sku` varchar(50) CHARACTER SET big5 DEFAULT NULL,`channel` varchar(10) DEFAULT NULL,PRIMARY KEY (`id`),KEY `event_id` (`event_id`),KEY `product_id` (`product_id`),KEY `userid` (`userid`),KEY `channel` (`channel`),KEY `index_datetime` (`datetime_added`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 /*#dve RANGE DISTRIBUTE ON (`userid`) USING `user_range` */"
			};
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		for(String decl : decls) {
			tests.add(new StatementMirrorProc(decl));
		}
		tests.add(new StatementMirrorProc("SELECT count(1) from (select distinct h.userid             FROM user_histories h             INNER JOIN bs_user u ON u.uid=h.userid             WHERE h.event_id = 2405721 AND u.country = 'Singapore') a"));
		runTest(tests);
	}

	@Test
	public void testPE895() throws Throwable {
		String[] decls = new String[] { 
				"/*#dve create range if not exists order_range (int) persistent group " + sysDDL.getPersistentGroup().getName() + "*/",
				"/*#dve create range if not exists user_range (int) persistent group " + sysDDL.getPersistentGroup().getName() + "*/",
				"CREATE TABLE if not exists `users` (`uid` int(10) unsigned NOT NULL,`name` varchar(60) NOT NULL DEFAULT '',`pass` varchar(32) NOT NULL DEFAULT '',`mail` varchar(64) DEFAULT '',`mode` tinyint(4) NOT NULL DEFAULT '0',`sort` tinyint(4) DEFAULT '0',`threshold` tinyint(4) DEFAULT '0',`theme` varchar(255) NOT NULL DEFAULT '',`signature` varchar(255) NOT NULL DEFAULT '',`created` int(11) NOT NULL DEFAULT '0',`access` int(11) NOT NULL DEFAULT '0',`login` int(11) NOT NULL DEFAULT '0',`status` tinyint(4) NOT NULL DEFAULT '0',`timezone` varchar(8) DEFAULT NULL,`language` varchar(12) NOT NULL DEFAULT '',`picture` varchar(255) NOT NULL DEFAULT '',`init` varchar(64) DEFAULT '',`data` longtext,`timezone_name` varchar(50) NOT NULL DEFAULT '',PRIMARY KEY (`uid`),CONSTRAINT `name` UNIQUE KEY `name` (`name`),KEY `access` (`access`),KEY `created` (`created`),KEY `mail` (`mail`),KEY `status` (`status`),KEY `pass` (`pass`(5)),KEY `mode` (`mode`),KEY `sort` (`sort`),KEY `threshold` (`threshold`),KEY `language` (`language`)) ENGINE=MyISAM DEFAULT CHARSET=utf8  /*#dve RANGE DISTRIBUTE ON (`uid`) USING `user_range` */",
				"CREATE TABLE if not exists `uc_orders` (`order_id` int(10) unsigned NOT NULL  auto_increment ,`uid` int(10) unsigned NOT NULL  DEFAULT '0', "
				+"`order_status` varchar(32) NOT NULL  DEFAULT '' ,`order_total` decimal(15,3) NOT NULL  DEFAULT '0.000' ,"
				+"`primary_email` varchar(96) NOT NULL  DEFAULT '' ,`delivery_first_name` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`delivery_last_name` varchar(255) NOT NULL  DEFAULT '' ,`delivery_phone` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`delivery_company` varchar(255) NOT NULL  DEFAULT '' ,`delivery_street1` varchar(255) NOT NULL  DEFAULT '' ," 
				+"`delivery_street2` varchar(255) NOT NULL  DEFAULT '' ,`delivery_city` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`delivery_zone` mediumint(8) unsigned NOT NULL  DEFAULT '0' ,`delivery_postal_code` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`delivery_country` mediumint(8) unsigned NOT NULL  DEFAULT '0' ,`billing_first_name` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`billing_last_name` varchar(255) NOT NULL  DEFAULT '' ,`billing_phone` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`billing_company` varchar(255) NOT NULL  DEFAULT '' ,`billing_street1` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`billing_street2` varchar(255) NOT NULL  DEFAULT '' ,`billing_city` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`billing_zone` mediumint(8) unsigned NOT NULL  DEFAULT '0' ,`billing_postal_code` varchar(255) NOT NULL  DEFAULT '' ,"
				+"`billing_country` mediumint(8) unsigned NOT NULL  DEFAULT '0' ,`payment_method` varchar(32) NOT NULL  DEFAULT '' ,"
				+"`data` text,`created` int(11) NOT NULL  DEFAULT '0' ,`modified` int(11) NOT NULL  DEFAULT '0' ,"
				+"`host` varchar(255) NOT NULL  DEFAULT '' ,`payment_currency` char(3) DEFAULT NULL  COMMENT 'reebonz order local currency',"
				+"`payment_currency_amount` decimal(15,3) DEFAULT NULL , PRIMARY KEY (`order_id`), KEY `uid` (`uid`), "
				+"KEY `order_status` (`order_status`), KEY `billing_country` (`billing_country`))"
				+" engine=myisam /*#dve broadcast distribute */",
				"CREATE TABLE if not exists `uc_order_vertical` (`order_id` int(11) NOT NULL DEFAULT '0',`vertical_id` int(11) DEFAULT NULL,PRIMARY KEY (`order_id`),KEY `vertical_id` (`vertical_id`)) ENGINE=MyISAM DEFAULT CHARSET=utf8 /*#dve RANGE DISTRIBUTE ON (`order_id`) USING `order_range` */",
				"CREATE TABLE if not exists `bs_user` (`uid` int(10) NOT NULL,`first_name` varchar(255) NOT NULL,`last_name` varchar(255) NOT NULL,`dob` varchar(10) NOT NULL,`occupation` varchar(50) NOT NULL,`occupation_new` int(11) NOT NULL DEFAULT '0',`country` varchar(255) NOT NULL,`city` varchar(255) NOT NULL,`gender` enum('m','f','') NOT NULL,`income_range` varchar(50) DEFAULT NULL,`education` varchar(50) DEFAULT NULL,`shopping_range` varchar(50) DEFAULT NULL,`address` text,`interest_view` longtext,`activation_status` int(1) NOT NULL DEFAULT '0',`activation_date` timestamp NULL DEFAULT NULL,`reg_type` enum('invitation_code','friendinvite_online','friendinvite_offline','waitinglist','invitation_code_link','friendinvite_link','promo_code','promo_code_link','personal_email','personal_email_special') NOT NULL,`invite_code` varchar(25) DEFAULT NULL,`referrer_member_uid` int(10) DEFAULT NULL,`credit` double NOT NULL DEFAULT '0',`credit_deduct` double NOT NULL DEFAULT '0',`vip` tinyint(4) NOT NULL DEFAULT '0',`personal_link_invite` varchar(200) DEFAULT NULL,`email_temp_message` text,`email_notification_event` char(1) NOT NULL DEFAULT 'y',`email_notification_promo` char(1) NOT NULL DEFAULT 'y',`email_notification_city_event` char(1) NOT NULL DEFAULT 'n',`email_notification_city_promo` char(1) NOT NULL DEFAULT 'n',`email_notification_travel_event` char(1) NOT NULL DEFAULT 'n',`email_notification_travel_promo` char(1) NOT NULL DEFAULT 'n',`email_notification_kwerkee_event` char(1) NOT NULL DEFAULT 'n',`email_notification_kwerkee_promo` char(1) NOT NULL DEFAULT 'n',`email_notification_vintage_event` char(1) NOT NULL,`email_notification_vintage_promo` char(1) NOT NULL,`title` enum('mr','mrs','ms','') DEFAULT NULL,`state` varchar(50) NOT NULL,`postal_code` varchar(10) NOT NULL,`town` varchar(50) NOT NULL,`mobile_phone_no` varchar(20) NOT NULL,`home_phone_no` varchar(20) NOT NULL,`other_phone_no` varchar(20) NOT NULL,`shoe_size` varchar(2) NOT NULL,`clothes_size` varchar(4) NOT NULL,`hear_from` text NOT NULL,`interested_in` text NOT NULL,`fave_brand` varchar(50) NOT NULL,`other_store` varchar(50) NOT NULL,`reminder_date` datetime DEFAULT NULL,`vertical_id` int(10) unsigned NOT NULL,`is_active_fashion` tinyint(1) NOT NULL,`is_active_city` tinyint(1) NOT NULL,`is_active_travel` tinyint(1) NOT NULL,`mxpp` int(1) NOT NULL DEFAULT '0',`fave_magazines` varchar(100) DEFAULT NULL,`fave_shopping_websites` varchar(100) DEFAULT NULL,`monthly_expend` varchar(30) DEFAULT NULL,`factor_to_visit` varchar(100) DEFAULT NULL,`suggest_improvement` varchar(100) DEFAULT NULL,`modified` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' ON UPDATE CURRENT_TIMESTAMP,`source_page` varchar(25) DEFAULT NULL,`source_media` varchar(25) DEFAULT NULL,`is_active_kwerky` tinyint(1) NOT NULL DEFAULT '0',`signup_method` varchar(25) DEFAULT NULL,`kwerkee_fb_share_purchase` char(1) DEFAULT 'n',`kwerkee_fb_share_fave` char(1) DEFAULT 'n',`kwerkee_fb_social` char(1) DEFAULT 'n',`kwerkee_twitter_social` char(1) DEFAULT NULL,`kwerkee_twitter_share_purchase` char(1) DEFAULT NULL,`kwerkee_twitter_share_fave` char(1) DEFAULT NULL,`source` varchar(25) DEFAULT NULL COMMENT 'store actual source string',PRIMARY KEY (`uid`),KEY `reg_type` (`reg_type`),KEY `activation_status` (`activation_status`),KEY `invite_code` (`invite_code`),KEY `referrer_member_uid` (`referrer_member_uid`),KEY `country` (`country`),KEY `occupation_new` (`occupation_new`),KEY `gender` (`gender`),KEY `income_range` (`income_range`),KEY `title` (`title`),KEY `activation_date` (`activation_date`),KEY `personal_link_invite` (`personal_link_invite`),KEY `vertical_id` (`vertical_id`),KEY `is_active_fashion` (`is_active_fashion`),KEY `is_active_city` (`is_active_city`),KEY `is_active_travel` (`is_active_travel`),KEY `email_notification_kwerkee_event` (`email_notification_kwerkee_event`, `email_notification_kwerkee_promo`),KEY `is_active_kwerky` (`is_active_kwerky`),KEY `kwerkee_twitter_social` (`kwerkee_twitter_social`),KEY `kwerkee_twitter_share_purchase` (`kwerkee_twitter_share_purchase`),KEY `kwerkee_twitter_share_fave` (`kwerkee_twitter_share_fave`),KEY `source` (`source`)) ENGINE=MyISAM DEFAULT CHARSET=utf8 /*#dve BROADCAST DISTRIBUTE */",
			};
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		for(String decl : decls) {
			tests.add(new StatementMirrorProc(decl));
		}
		tests.add(new StatementMirrorProc("SELECT count( 1 )  AS func_3 FROM  ( SELECT DISTINCT a.uid,count( DISTINCT b.order_id )  AS count_curr,count( DISTINCT y.order_id )  AS count_prev FROM `users` AS a INNER JOIN `uc_orders` AS b ON a.uid = b.uid INNER JOIN `uc_order_vertical` AS v ON b.order_id = v.order_id INNER JOIN `bs_user` AS x ON b.uid = x.uid LEFT OUTER JOIN `uc_orders` AS y ON b.uid = y.uid and from_unixtime( y.created )  < '2013-06-17 00:00:00' AND y.order_status in ( 'completed','on_delivery','packaging','payment_received','processing' ) WHERE from_unixtime( b.created )  BETWEEN '2013-06-17 00:00:00' AND '2013-06-25 00:00:00' AND x.country = 'Thailand' AND b.order_status in ( 'completed','on_delivery','packaging','payment_received','processing' ) AND v.vertical_id IN ( 434 ) GROUP BY a.uid ASC HAVING count( DISTINCT y.order_id )  = 0 ) yy"));
		runTest(tests);
	}
	
	@Test
	public void testPE801() throws Throwable {
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		tests.add(new StatementMirrorProc("CREATE TABLE `answers` (`id` int(11) NOT NULL AUTO_INCREMENT, `survey` int(11) NOT NULL, `question` int(11) NOT NULL, `defaultValue` text NOT NULL, PRIMARY KEY (`id`,`survey`,`question`)) ENGINE=MyISAM AUTO_INCREMENT=20 DEFAULT CHARSET=utf8"));
		tests.add(new StatementMirrorProc("INSERT INTO `answers` VALUES (1,1,1,'10'),(2,1,2,'200'),(3,1,3,'0;100;200'),(4,2,1,'Eat less'),(4,2,2,'Eat less'),(4,2,3,'Eat less'),(4,2,4,'Eat less'),(5,2,1,'Diet pills'),(5,2,2,'Diet pills'),(5,2,3,'Diet pills'),(5,2,4,'Diet pills'),(6,2,1,'Surgery'),(6,2,2,'Surgery'),(6,2,3,'Surgery'),(6,2,4,'Surgery'),(7,2,1,'Excercise'),(7,2,2,'Excercise'),(7,2,3,'Excercise'),(7,2,4,'Excercise'),(8,3,2,'Yes'),(9,3,2,'Possibly'),(10,3,2,'No'),(11,3,3,'Yes'),(12,3,3,'Possibly'),(13,3,3,'No'),(14,3,5,'Yes'),(15,3,5,'Possibly'),(16,3,5,'No'),(17,3,6,'Yes'),(18,3,6,'Possibly'),(19,3,6,'No')"));
		
		tests.add(new StatementMirrorProc("CREATE TABLE `questions` (`id` int(11) NOT NULL AUTO_INCREMENT, `survey` int(11) NOT NULL, `class_suffix` int(11) NOT NULL DEFAULT '0', `type` int(11) NOT NULL, `text` text NOT NULL, PRIMARY KEY (`id`,`survey`)) ENGINE=MyISAM AUTO_INCREMENT=10 DEFAULT CHARSET=utf8"));
		tests.add(new StatementMirrorProc("INSERT INTO `questions` VALUES (1,1,0,1,'What percent of time do you spend at doctor\\'s office?'),(1,2,0,3,'Select which type of diet you have taken.'),(1,3,0,4,'How many times a year do you...?'),(2,1,0,1,'How many days do you wait before you go back to doc office?'),(2,2,0,3,'Select which type of diet you have taken.'),(2,3,1,4,'Swim'),(3,1,0,2,'How much do you spend on diet stuff?'),(3,2,0,3,'Select which type of diet you have taken.'),(3,3,1,4,'Ski'),(4,2,0,3,'Select which type of diet you have taken.'),(4,3,0,4,'How many times a year do you...?'),(5,3,1,4,'Swim'),(6,3,1,4,'Ski')"));
		
		tests.add(new StatementMirrorProc("CREATE TABLE `results` (`id` int(11) NOT NULL AUTO_INCREMENT, `ip` varchar(15) NOT NULL, `answer_id` int(11) NOT NULL, `value` int(11) NOT NULL DEFAULT '0', PRIMARY KEY (`id`,`ip`,`answer_id`)) ENGINE=MyISAM AUTO_INCREMENT=74 DEFAULT CHARSET=utf8"));
		tests.add(new StatementMirrorProc("INSERT INTO `results` VALUES (65,'645893848',1,10),(66,'645893848',2,200),(67,'645893848',3,100),(68,'328536107',4,1),(69,'328536107',6,1),(70,'493510303',8,1),(71,'493510303',12,1),(72,'493510303',16,1),(73,'493510303',17,1)"));
		
		tests.add(new StatementMirrorProc("CREATE TABLE `surveys` (`id` int(11) NOT NULL AUTO_INCREMENT, `heading` tinytext NOT NULL, `text` text, `style` text, `votes` int(11) NOT NULL DEFAULT '0', PRIMARY KEY (`id`)) ENGINE=MyISAM AUTO_INCREMENT=4 DEFAULT CHARSET=utf8"));
		tests.add(new StatementMirrorProc("INSERT INTO `surveys` VALUES (1,'Survey 1','A sample result for this survey...','#SurveyForm { margin: auto; width: 75%; } #SurveyInfo { border: solid 1px; background-color: #DCDCDC; clear: both; margin: auto; text-align: center; width: 33%; } .question_0 { width: 80%; float: left; font-style: italic; margin: 2px 0 2px 0; padding: 0 10% 0 10%; } .answer { text-align: center; vertical-align: bottom; width: 5%; float: right; margin: 2px 0 2px 0; padding: 0; } #a3_control { margin: 4px 0 0 0; padding-right: 2%; width: 10%; } #ButtonPanel { text-align: center; clear: both; margin: auto; }',50),(2,'Survey 2',NULL,'#SurveyForm { margin: auto; width: 75%; } #SurveyHeading { text-align: center; } .answer { display: block; } .question_0 { width: 48%; margin-bottom: 2%; border-width: 2px; border-style: solid; } #q1 { float: left; border-color: #FF0000; background-color: #FFDCDC; } #q2 { float: right; border-color: #00FF00; border-style: dashed; background-color: #DCFFDC; } #q3 { float: left; border-color: #0000FF; border-style: dotted; background-color: #DCDCFF; } #q4 { float: right; border-color: #000000; background-color: #DCDCDC; } #ButtonPanel { text-align: center; clear: both; margin: auto; }',23),(3,'Survey 3',NULL,'#SurveyForm { width: 300px; }  #SurveyHeading { }  .question_0 { width: 100%; clear: both; text-decoration: underline; font-weight: bold; }  .question_1 { width: 100%; float: left; margin: 2px 0 2px 0; padding: 0 30px 0 20px; list-style-type:square; background-image: url(../images/pills.gif); background-repeat: no-repeat; background-position: left; }  .answer { vertical-align: bottom; width: 100px; float: right; margin: 2px 90px 2px 0; }  #ButtonPanel { float: right; width: 100px; margin-right:70px; }  #SubmitButton1 { width: 100%; }',14)"));
		
		tests.add(new StatementMirrorFun("SELECT CONCAT('$', CAST(AVG(value) AS CHAR)) FROM results INNER JOIN answers ON results.answer_id = answers.id INNER JOIN (SELECT DISTINCT(ip) distip FROM results INNER JOIN answers ON results.answer_id = answers.id WHERE (survey = '1') GROUP BY ip HAVING ((SUM((question = '1') AND (value < '25')) > 0) AND (SUM((question = '2') AND (value > '14')) > 0))) distip ON results.ip = distip WHERE ((survey = '1') AND (question = '3'))"));
		runTest(tests);
	}

	@Test
	public void testPE477() throws Throwable {
		String[] decls = new String[] { 
				"/*#dve create range if not exists order_range (int) persistent group " + sysDDL.getPersistentGroup().getName() + "*/",
				"CREATE TABLE if not exists `uc_order_line_items` (  `line_item_id` int(10) unsigned NOT NULL AUTO_INCREMENT,  `order_id` int(10) unsigned NOT NULL DEFAULT '0',  `type` varchar(32) NOT NULL DEFAULT '',  `title` varchar(128) NOT NULL DEFAULT '',  `amount` decimal(15,3) NOT NULL DEFAULT '0.000',  `weight` smallint(6) NOT NULL DEFAULT '0',  `data` text,  `payment_currency` char(3) DEFAULT NULL,  `payment_currency_amount` decimal(15,3) DEFAULT NULL,  `credit_deduction_id` int(11) DEFAULT NULL,  `credit_category` varchar(20) DEFAULT NULL,  PRIMARY KEY (`line_item_id`),  KEY `order_id` (`order_id`),  FULLTEXT KEY `type` (`type`),  FULLTEXT KEY `title` (`title`)) ENGINE=MyISAM AUTO_INCREMENT=722189 DEFAULT CHARSET=utf8 /*#dve  RANGE DISTRIBUTE ON (`order_id`) USING `order_range` */",
				"CREATE TABLE if not exists `uc_order_products` (`order_product_id` int(10) unsigned NOT NULL AUTO_INCREMENT,`order_id` int(10) unsigned NOT NULL DEFAULT '0',`nid` int(10) unsigned NOT NULL DEFAULT '0',`title` varchar(128) NOT NULL DEFAULT '',`manufacturer` varchar(32) NOT NULL DEFAULT '',`model` varchar(255) NOT NULL DEFAULT '',`qty` smallint(5) unsigned NOT NULL DEFAULT '0',`cost` decimal(15,3) NOT NULL DEFAULT '0.000',`price` decimal(15,3) NOT NULL DEFAULT '0.000',`weight` float NOT NULL DEFAULT '0',`data` text,`payment_currency` char(3) DEFAULT NULL,`payment_currency_amount` decimal(15,3) DEFAULT NULL,PRIMARY KEY (`order_product_id`), KEY `order_id` (`order_id`), KEY `nid` (`nid`), KEY `model` (`model`)) ENGINE=MyISAM DEFAULT CHARSET=utf8 AUTO_INCREMENT=676637 /*#dve BROADCAST DISTRIBUTE */",
				"INSERT INTO `uc_order_line_items` (`order_id`, `amount`, `type`) values (357704, 0.000, 'shipping')",
				"INSERT INTO `uc_order_products` (`order_id`, `price`, `qty`) values (357704, 677.630, 1)",
			};
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		for(String decl : decls) {
			tests.add(new StatementMirrorProc(decl));
		}
		tests.add(new StatementMirrorProc("SELECT SUM(price*qty) AS total_sales_amt,  (SELECT SUM(amount) from uc_order_line_items where order_id=357704 and type='tax') as tax,  (SELECT SUM(amount) from uc_order_line_items where order_id=357704 and type='shipping') as shipping  FROM uc_order_products  WHERE order_id=357704"));
		runTest(tests);
	}

	@Test
	public void testPE1130() throws Throwable {
		final String pgName = sysDDL.getPersistentGroup().getName();
		final String[] schema = new String[] {
				"/*#dve create range if not exists node_range (int) persistent group "
						+ pgName + "*/",
				"/*#dve create range if not exists node_revision_range (int) persistent group "
						+ pgName + "*/",
				"CREATE TABLE `node` (`nid` int(10) unsigned NOT NULL AUTO_INCREMENT, `vid` int(10) unsigned DEFAULT NULL, `type` varchar(32) NOT NULL DEFAULT '', `language` varchar(12) NOT NULL DEFAULT '', `title` varchar(255) NOT NULL DEFAULT '', `uid` int(11) NOT NULL DEFAULT '0', `status` int(11) NOT NULL DEFAULT '1', `created` int(11) NOT NULL DEFAULT '0', `changed` int(11) NOT NULL DEFAULT '0', `comment` int(11) NOT NULL DEFAULT '0', `promote` int(11) NOT NULL DEFAULT '0', `sticky` int(11) NOT NULL DEFAULT '0', `tnid` int(10) unsigned NOT NULL DEFAULT '0', `translate` int(11) NOT NULL DEFAULT '0', PRIMARY KEY (`nid`), UNIQUE KEY `vid` (`vid`), KEY `node_changed` (`changed`), KEY `node_created` (`created`), KEY `node_frontpage` (`promote`,`status`,`sticky`,`created`), KEY `node_status_type` (`status`,`type`,`nid`), KEY `node_title_type` (`title`,`type`(4)), KEY `node_type` (`type`(4)), KEY `uid` (`uid`), KEY `tnid` (`tnid`), KEY `translate` (`translate`)) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8 /*#dve  RANGE DISTRIBUTE ON (`nid`) USING `node_range` */",
				"CREATE TABLE `node_revision` (`nid` int(10) unsigned NOT NULL DEFAULT '0', `vid` int(10) unsigned NOT NULL AUTO_INCREMENT, `uid` int(11) NOT NULL DEFAULT '0', `title` varchar(255) NOT NULL DEFAULT '', `log` longtext NOT NULL, `timestamp` int(11) NOT NULL DEFAULT '0', `status` int(11) NOT NULL DEFAULT '1', `comment` int(11) NOT NULL DEFAULT '0', `promote` int(11) NOT NULL DEFAULT '0', `sticky` int(11) NOT NULL DEFAULT '0', PRIMARY KEY (`vid`), KEY `nid` (`nid`), KEY `uid` (`uid`)) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8 /*#dve  RANGE DISTRIBUTE ON (`vid`) USING `node_revision_range` */",
		};

		final String[] preparation = new String[] {
				"START TRANSACTION",
				"INSERT INTO node (vid, type, language, title, uid, status, created, changed, comment, promote, sticky) VALUES (NULL, 'group', 'und', 'AAAAAAAAAA', '1', '0', '1379690831', '1379690831', '1', '0', '0')",
				"INSERT INTO node_revision (nid, uid, title, log, timestamp, status, comment, promote, sticky) VALUES ('7', '1', 'AAAAAAAAAA', '', '1379690831', '0', '1', '0', '0')",
				"UPDATE node SET vid='7' WHERE  (nid = '7')"
		};

		final List<MirrorTest> tests = new ArrayList<MirrorTest>();
		loadMirrorProcTestsFromStatements(schema, tests);
		loadMirrorProcTestsFromStatements(preparation, tests);

		tests.add(new StatementMirrorFun(
				"SELECT revision.vid AS vid, base.uid AS uid, revision.title AS title, revision.log AS log, revision.status AS status, revision.comment AS comment, revision.promote AS promote, revision.sticky AS sticky, base.nid AS nid, base.type AS type, base.language AS language, base.created AS created, base.changed AS changed, base.tnid AS tnid, base.translate AS translate, revision.timestamp AS revision_timestamp, revision.uid AS revision_uid FROM node base INNER JOIN node_revision revision ON revision.vid = base.vid WHERE  (base.nid IN  ('7'))"));
		runTest(tests);
	}

	private void loadMirrorProcTestsFromStatements(final String[] statements, final List<MirrorTest> tests) {
		for (final String statement : statements) {
			tests.add(new StatementMirrorProc(statement));
		}
	}
	
	@Test
	public void testPE1325() throws Throwable {
		String[] decls = new String[] { 
				"/*#dve create range if not exists node_range (int) persistent group " + sysDDL.getPersistentGroup().getName() + "*/",
				"CREATE TABLE if not exists `uc_orders` (`order_id` int(10) unsigned NOT NULL AUTO_INCREMENT,`uid` int(10) unsigned NOT NULL DEFAULT '0',`order_status` varchar(32) NOT NULL DEFAULT '',`order_total` decimal(15,3) NOT NULL DEFAULT '0.000',`primary_email` varchar(96) NOT NULL DEFAULT '',`delivery_first_name` varchar(255) NOT NULL DEFAULT '',`delivery_last_name` varchar(255) NOT NULL DEFAULT '',`delivery_phone` varchar(255) NOT NULL DEFAULT '',`delivery_company` varchar(255) NOT NULL DEFAULT '',`delivery_street1` varchar(255) NOT NULL DEFAULT '',`delivery_street2` varchar(255) NOT NULL DEFAULT '',`delivery_city` varchar(255) NOT NULL DEFAULT '',`delivery_zone` mediumint(8) unsigned NOT NULL DEFAULT '0',`delivery_postal_code` varchar(255) NOT NULL DEFAULT '',`delivery_country` mediumint(8) unsigned NOT NULL DEFAULT '0',`billing_first_name` varchar(255) NOT NULL DEFAULT '',`billing_last_name` varchar(255) NOT NULL DEFAULT '',`billing_phone` varchar(255) NOT NULL DEFAULT '',`billing_company` varchar(255) NOT NULL DEFAULT '',`billing_street1` varchar(255) NOT NULL DEFAULT '',`billing_street2` varchar(255) NOT NULL DEFAULT '',`billing_city` varchar(255) NOT NULL DEFAULT '',`billing_zone` mediumint(8) unsigned NOT NULL DEFAULT '0',`billing_postal_code` varchar(255) NOT NULL DEFAULT '',`billing_country` mediumint(8) unsigned NOT NULL DEFAULT '0',`payment_method` varchar(32) NOT NULL DEFAULT '',`data` text,`created` int(11) NOT NULL DEFAULT '0',`modified` int(11) NOT NULL DEFAULT '0',`host` varchar(255) NOT NULL DEFAULT '',`payment_currency` char(3) DEFAULT NULL COMMENT 'reebonz order local currency',`payment_currency_amount` decimal(15,3) DEFAULT NULL,PRIMARY KEY (`order_id`),KEY `uid` (`uid`),KEY `order_status` (`order_status`),KEY `billing_country` (`billing_country`),KEY `delivery_country` (`delivery_country`),KEY `primary_email` (`primary_email`),KEY `billing_phone` (`billing_phone`),KEY `payment_method` (`payment_method`)) ENGINE=MyISAM AUTO_INCREMENT=456309 DEFAULT CHARSET=utf8 /*#dve  BROADCAST DISTRIBUTE */",
				"CREATE TABLE if not exists `uc_order_products` (`order_product_id` int(10) unsigned NOT NULL AUTO_INCREMENT,`order_id` int(10) unsigned NOT NULL DEFAULT '0',`nid` int(10) unsigned NOT NULL DEFAULT '0',`title` varchar(128) NOT NULL DEFAULT '',`manufacturer` varchar(32) NOT NULL DEFAULT '',`model` varchar(255) NOT NULL DEFAULT '',`qty` smallint(5) unsigned NOT NULL DEFAULT '0',`cost` decimal(15,3) NOT NULL DEFAULT '0.000',`price` decimal(15,3) NOT NULL DEFAULT '0.000',`weight` float NOT NULL DEFAULT '0',`data` text,`payment_currency` char(3) DEFAULT NULL,`payment_currency_amount` decimal(15,3) DEFAULT NULL,PRIMARY KEY (`order_product_id`), KEY `order_id` (`order_id`), KEY `nid` (`nid`), KEY `model` (`model`)) ENGINE=MyISAM DEFAULT CHARSET=utf8 AUTO_INCREMENT=676637 /*#dve BROADCAST DISTRIBUTE */",
				"CREATE TABLE if not exists `content_type_product` (  `vid` int(10) unsigned NOT NULL DEFAULT '0',  `nid` int(10) unsigned NOT NULL DEFAULT '0',  `field_product_type_value` varchar(255) DEFAULT NULL,  `hw_itemno` varchar(125) DEFAULT NULL,  `field_event_nid` int(10) unsigned DEFAULT NULL,  `field_item_list_fid` int(11) DEFAULT NULL,  `field_item_list_list` tinyint(4) DEFAULT NULL,  `field_item_list_data` text,  `field_color_value` longtext,  `field_was_price_value` decimal(10,2) DEFAULT NULL,  `field_weight_value` int(11) DEFAULT NULL,  `field_shipping_package_nid` int(10) unsigned DEFAULT NULL,  `field_dp_sdate_value` int(11) DEFAULT NULL,  `field_dp_edate_value` int(11) DEFAULT NULL,  `field_tag_no_value` varchar(25) DEFAULT NULL,  `field_supplier_sku_value` varchar(50) DEFAULT NULL,  `field_wholesale_price_value` float DEFAULT NULL,  `field_rrp_value` float DEFAULT NULL,  `field_rrp_currency_value` char(3) DEFAULT NULL,  `field_supplier_cost_value` float DEFAULT NULL,  `field_supplier_cost_currency_value` char(3) DEFAULT NULL,  `field_cogs_value` float DEFAULT NULL,  `field_was_discounted_price_value` float DEFAULT NULL,  `field_facebook_fans_choice_value` longtext,  `field_generic_name_value` longtext,  `field_multi_wholesale_australia_value` decimal(10,2) DEFAULT NULL,  `field_multi_wholesale_hongkong_value` decimal(10,2) DEFAULT NULL,  `field_multi_wholesale_indonesia_value` decimal(20,2) DEFAULT NULL,  `field_multi_wholesale_malaysia_value` decimal(10,2) DEFAULT NULL,  `field_multi_wholesale_korea_value` decimal(20,2) DEFAULT NULL,  `field_multi_wholesale_taiwan_value` decimal(10,2) DEFAULT NULL,  `field_multi_wholesale_brunei_value` decimal(10,2) DEFAULT NULL,  `field_multi_was_australia2_value` decimal(10,2) DEFAULT NULL,  `field_multi_was_hongkong2_value` decimal(10,2) DEFAULT NULL,  `field_multi_was_indonesia2_value` decimal(20,2) DEFAULT NULL,  `field_multi_was_malaysia2_value` decimal(10,2) DEFAULT NULL,  `field_multi_was_korea2_value` decimal(20,2) DEFAULT NULL,  `field_multi_was_taiwan2_value` decimal(10,2) DEFAULT NULL,  `field_multi_was_brunei2_value` decimal(10,2) DEFAULT NULL,  `field_multi_was_disc_australia_value` decimal(10,2) DEFAULT NULL,  `field_multi_was_disc_hongkong_value` decimal(10,2) DEFAULT NULL,  `field_multi_was_disc_indonesia_value` decimal(20,2) DEFAULT NULL,  `field_multi_was_disc_malaysia_value` decimal(10,2) DEFAULT NULL,  `field_multi_was_disc_korea_value` decimal(20,2) DEFAULT NULL,  `field_multi_was_disc_taiwan_value` decimal(10,2) DEFAULT NULL,  `field_multi_was_disc_brunei_value` decimal(10,2) DEFAULT NULL,  `field_multi_selling_australia_value` decimal(10,2) DEFAULT NULL,  `field_multi_selling_hongkong_value` decimal(10,2) DEFAULT NULL,  `field_multi_selling_indonesia_value` decimal(20,2) DEFAULT NULL,  `field_multi_selling_malaysia_value` decimal(10,2) DEFAULT NULL,  `field_multi_selling_korea_value` decimal(20,2) DEFAULT NULL,  `field_multi_selling_taiwan_value` decimal(10,2) DEFAULT NULL,  `field_multi_selling_brunei_value` decimal(10,2) DEFAULT NULL,  `field_name_australia_value` longtext,  `field_name_hongkong_value` longtext,  `field_name_indonesia_value` longtext,  `field_name_malaysia_value` longtext,  `field_name_korea_value` longtext,  `field_name_taiwan_value` longtext,  `field_name_brunei_value` longtext,  `field_desc_australia_value` longtext,  `field_desc_hongkong_value` longtext,  `field_desc_indonesia_value` longtext,  `field_desc_malaysia_value` longtext,  `field_desc_korea_value` longtext,  `field_desc_taiwan_value` longtext,  `field_desc_brunei_value` longtext,  `field_name_thailand_value` longtext,  `field_name_philippin_value` longtext,  `field_desc_thailand_value` longtext,  `field_desc_philippin_value` longtext,  `field_multi_wholesale_thailand_value` decimal(10,2) DEFAULT NULL,  `field_multi_wholesale_philippin_value` decimal(10,2) DEFAULT NULL,  `field_multi_was_thailand2_value` decimal(10,2) DEFAULT NULL,  `field_multi_was_philippin2_value` decimal(10,2) DEFAULT NULL,  `field_multi_was_disc_thailand_value` decimal(10,2) DEFAULT NULL,  `field_multi_was_disc_philippin_value` decimal(10,2) DEFAULT NULL,  `field_multi_selling_thailand_value` decimal(10,2) DEFAULT NULL,  `field_multi_selling_philippin_value` decimal(10,2) DEFAULT NULL,  `field_stylist_pick_value` longtext,  `field_bagaholic_boys_choice_value` longtext,  `field_grade_value` longtext,  `field_rarity_guide_value` longtext,  `field_max_qty_city_product_value` longtext,  `field_package_city_value` longtext,  `field_name_newzealand_value` longtext,  `field_desc_newzealand_value` longtext,  `field_multi_wholesale_newzealand_value` decimal(10,2) DEFAULT NULL,  `field_multi_was_newzealand2_value` decimal(10,2) DEFAULT NULL,  `field_multi_was_disc_newzealand_value` decimal(10,2) DEFAULT NULL,  `field_multi_selling_newzealand_value` decimal(10,2) DEFAULT NULL,  `field_multi_wholesale_canada_value` decimal(10,2) DEFAULT NULL,  `field_multi_was_canada2_value` decimal(10,2) DEFAULT NULL,  `field_multi_was_disc_canada_value` decimal(10,2) DEFAULT NULL,  `field_multi_selling_canada_value` decimal(10,2) DEFAULT NULL,  `field_name_canada_value` longtext,  `field_desc_canada_value` longtext,  `field_desc_usa_value` longtext COLLATE utf8_unicode_ci,  `field_name_usa_value` longtext COLLATE utf8_unicode_ci,  `field_multi_wholesale_usa_value` decimal(10,2) DEFAULT NULL,  `field_multi_selling_usa_value` decimal(10,2) DEFAULT NULL,  `field_multi_was_usa2_value` decimal(10,2) DEFAULT NULL,  `field_multi_was_disc_usa_value` decimal(10,2) DEFAULT NULL,  `field_name_china_value` longtext,  `field_desc_china_value` longtext,  `field_multi_wholesale_china_value` decimal(10,2) DEFAULT NULL,  `field_multi_was_china2_value` decimal(10,2) DEFAULT NULL,  `field_multi_was_disc_china_value` decimal(10,2) DEFAULT NULL,  `field_multi_selling_china_value` decimal(10,2) DEFAULT NULL,  `field_desc_uae_value` longtext COLLATE utf8_unicode_ci,  `field_name_uae_value` longtext COLLATE utf8_unicode_ci,  `field_multi_wholesale_uae_value` decimal(10,2) DEFAULT NULL,  `field_multi_selling_uae_value` decimal(10,2) DEFAULT NULL,  `field_multi_was_uae2_value` decimal(10,2) DEFAULT NULL,  `field_multi_was_disc_uae_value` decimal(10,2) DEFAULT NULL,  `field_desc_arabia_value` longtext COLLATE utf8_unicode_ci,  `field_name_arabia_value` longtext COLLATE utf8_unicode_ci,  `field_multi_wholesale_arabia_value` decimal(10,2) DEFAULT NULL,  `field_multi_selling_arabia_value` decimal(10,2) DEFAULT NULL,  `field_multi_was_arabia2_value` decimal(10,2) DEFAULT NULL,  `field_multi_was_disc_arabia_value` decimal(10,2) DEFAULT NULL,  `field_show_in_malaysia_value` int(11) DEFAULT '1',  `field_show_in_indonesia_value` int(11) DEFAULT '1',  `field_show_in_taiwan_value` int(11) DEFAULT '1',  `field_show_in_korea_value` int(11) DEFAULT '1',  `field_show_in_thailand_value` int(11) DEFAULT '1',  `field_show_in_brunei_value` int(11) DEFAULT '1',  `field_show_in_philippines_value` int(11) DEFAULT '1',  `field_show_in_newzealand_value` int(11) DEFAULT '1',  `field_show_in_arabia_value` int(11) DEFAULT '1',  `field_show_in_canada_value` int(11) DEFAULT '1',  `field_show_in_usa_value` int(11) DEFAULT '1',  `field_show_in_uae_value` int(11) DEFAULT '1',  `field_show_in_china_value` int(11) DEFAULT '1',  `field_show_in_australia_value` int(11) DEFAULT '1',  `field_show_in_hongkong_value` int(11) DEFAULT '1',  `field_name_macau_value` longtext,  `field_desc_macau_value` longtext,  `field_multi_wholesale_macau_value` decimal(10,2) DEFAULT NULL,  `field_multi_was_macau2_value` decimal(10,2) DEFAULT NULL,  `field_multi_was_disc_macau_value` decimal(10,2) DEFAULT NULL,  `field_multi_selling_macau_value` decimal(10,2) DEFAULT NULL,  `field_show_in_macau_value` int(11) DEFAULT NULL,  `field_name_india_value` longtext,  `field_desc_india_value` longtext,  `field_multi_wholesale_india_value` decimal(10,2) DEFAULT NULL,  `field_multi_was_india2_value` decimal(10,2) DEFAULT NULL,  `field_multi_was_disc_india_value` decimal(10,2) DEFAULT NULL,  `field_multi_selling_india_value` decimal(10,2) DEFAULT NULL,  `field_show_in_india_value` int(11) DEFAULT NULL,  `field_multi_selling_japan_value` decimal(20,2) DEFAULT NULL,  `field_multi_was_disc_japan_value` decimal(20,2) DEFAULT NULL,  `field_multi_was_japan2_value` decimal(20,2) DEFAULT NULL,  `field_multi_wholesale_japan_value` decimal(20,2) DEFAULT NULL,  `field_desc_japan_value` longtext,  `field_name_japan_value` longtext,  `field_show_in_japan_value` int(11) DEFAULT NULL,  `field_name_uk_value` longtext,  `field_desc_uk_value` longtext,  `field_multi_wholesale_uk_value` decimal(10,2) DEFAULT NULL,  `field_multi_was_uk2_value` decimal(10,2) DEFAULT NULL,  `field_multi_was_disc_uk_value` decimal(10,2) DEFAULT NULL,  `field_multi_selling_uk_value` decimal(10,2) DEFAULT NULL,  `field_show_in_uk_value` int(11) DEFAULT NULL,  `field_name_vietnam_value` longtext,  `field_desc_vietnam_value` longtext,  `field_multi_wholesale_vietnam_value` decimal(20,2) DEFAULT NULL,  `field_multi_was_vietnam2_value` decimal(20,2) DEFAULT NULL,  `field_multi_was_disc_vietnam_value` decimal(20,2) DEFAULT NULL,  `field_multi_selling_vietnam_value` decimal(20,2) DEFAULT NULL,  `field_show_in_vietnam_value` int(11) DEFAULT NULL,  `field_show_in_singapore_value` int(11) DEFAULT NULL,  `field_shipping_package_code_value` longtext,  `field_lux_watch_gender_value` longtext,  `field_lux_watch_category_value` longtext,  `field_show_in_kuwait_value` int(11) DEFAULT NULL,  `field_multi_selling_kuwait_value` decimal(10,2) DEFAULT NULL,  `field_name_kuwait_value` longtext,  `field_desc_kuwait_value` longtext,  `field_multi_wholesale_kuwait_value` decimal(10,2) DEFAULT NULL,  `field_multi_was_disc_kuwait_value` decimal(10,2) DEFAULT NULL,  `field_multi_was_kuwait2_value` decimal(10,2) DEFAULT NULL,  `field_virtual_location_value` longtext,  PRIMARY KEY (`vid`),  KEY `nid` (`nid`),  KEY `field_event_nid` (`field_event_nid`),  KEY `field_shipping_package_nid` (`field_shipping_package_nid`),  KEY `field_supplier_sku_value` (`field_supplier_sku_value`),  KEY `field_product_type_value` (`field_product_type_value`),  KEY `field_item_list_fid` (`field_item_list_fid`),  KEY `field_item_list_list` (`field_item_list_list`),  KEY `field_tag_no_value` (`field_tag_no_value`)) ENGINE=MyISAM DEFAULT CHARSET=utf8 /*#dve  RANGE DISTRIBUTE ON (`nid`) USING `node_range` */",
				"CREATE TABLE if not exists `content_type_event` (   `vid` int(10) unsigned NOT NULL DEFAULT '0',   `nid` int(10) unsigned NOT NULL DEFAULT '0',   `field_event_summary_value` longtext,   `field_start_date_value` datetime DEFAULT NULL,   `field_end_date_value` datetime DEFAULT NULL,   `field_event_image_fid` int(11) DEFAULT NULL,   `field_event_image_list` tinyint(4) DEFAULT NULL,   `field_event_image_data` text,   `field_boutique_image_fid` int(11) DEFAULT NULL,   `field_boutique_image_list` tinyint(4) DEFAULT NULL,   `field_boutique_image_data` text,   `field_logo_fid` int(11) DEFAULT NULL,   `field_logo_list` tinyint(4) DEFAULT NULL,   `field_logo_data` text,   `field_discount_image_fid` int(11) DEFAULT NULL,   `field_discount_image_list` tinyint(4) DEFAULT NULL,   `field_discount_image_data` text,   `field_brand_name_value` longtext,   `field_company_name_value` longtext,   `field_address_value` longtext,   `field_telephone_value` longtext,   `field_fax_value` longtext,   `field_web_value` longtext,   `field_email_value` longtext,   `field_event_status_value` longtext,   `field_flash_upload_fid` int(11) DEFAULT NULL,   `field_flash_upload_list` tinyint(4) DEFAULT NULL,   `field_flash_upload_data` text,   `field_flash_filefield` varchar(255) DEFAULT NULL,   `field_flash_width` int(10) unsigned DEFAULT NULL,   `field_flash_height` int(10) unsigned DEFAULT NULL,   `field_flash_substitution` longtext,   `field_flash_flashvars` longtext,   `field_flash_base` varchar(255) DEFAULT NULL,   `field_flash_params` varchar(255) DEFAULT NULL,   `field_flash_fid` int(10) unsigned DEFAULT NULL,   `field_flash_display` tinyint(3) unsigned DEFAULT NULL,   `field_feature_event_value` longtext,   `field_allow_uob_installment_value` varchar(1) DEFAULT NULL,   `field_not_allow_cart_value` varchar(1) DEFAULT NULL,   `field_event_cost_value` decimal(10,2) DEFAULT NULL,   `field_shipping_country` text NOT NULL,   `field_multi_was_australia_value` float DEFAULT NULL,   `field_multi_was_hongkong_value` float DEFAULT NULL,   `field_multi_was_indonesia_value` float DEFAULT NULL,   `field_multi_was_malaysia_value` float DEFAULT NULL,   `field_multi_australia_value` float DEFAULT NULL,   `field_multi_hongkong_value` float DEFAULT NULL,   `field_multi_indonesia_value` float DEFAULT NULL,   `field_multi_malaysia_value` float DEFAULT NULL,   `field_event_summary_format` int(10) unsigned DEFAULT NULL,   `field_consignment_event_value` longtext,   `field_commission_value` float DEFAULT NULL,   `field_no_return_refund_value` longtext,   `field_multi_was_korea_value` float DEFAULT NULL,   `field_multi_korea_value` float DEFAULT NULL,   `field_show_brand_name_value` longtext,   `field_event_image_iphone_fid` int(11) DEFAULT NULL,   `field_event_image_iphone_list` tinyint(4) DEFAULT NULL,   `field_event_image_iphone_data` text,   `field_promotion_summary_value` longtext,   `field_hw_event_id_value` varchar(10) DEFAULT NULL,   `field_multi_was_taiwan_value` float DEFAULT NULL,   `field_multi_taiwan_value` float DEFAULT NULL,   `field_allow_mys_installment_value` varchar(10) DEFAULT NULL,   `field_allow_my_installment_value` varchar(10) DEFAULT NULL,   `field_multi_was_brunei_value` float DEFAULT NULL,   `field_multi_was_thailand_value` float DEFAULT NULL,   `field_multi_brunei_value` float DEFAULT NULL,   `field_multi_thailand_value` float DEFAULT NULL,   `field_allow_th_installment_value` varchar(10) DEFAULT NULL,   `field_not_allow_ipp_value` longtext,   `field_at_a_glance_value` longtext,   `field_at_a_glance_format` int(10) unsigned DEFAULT NULL,   `field_op_hours_value` longtext,   `field_contact_value` longtext,   `field_tnc_value` longtext,   `field_gmap_lid` int(10) unsigned DEFAULT NULL,   `field_city_value` longtext,   `field_how_to_redeem_value` longtext,   `field_redeem_date_value` datetime DEFAULT NULL,   `field_edm_title_value` longtext,   `field_edm_summary_value` longtext,   `field_restrict_qty_value` longtext,   `field_mxpp_value` longtext,   `field_hw_event_status_value` longtext,   `field_hw_event_status_error_value` longtext,   `field_website_url_value` longtext,   `field_add_location_value` longtext,   `field_itinerary_value` longtext,   `field_itinerary_format` int(10) unsigned DEFAULT NULL,   `field_attraction_value` longtext,   `field_attraction_format` int(10) unsigned DEFAULT NULL,   `field_details_value` longtext,   `field_details_format` int(10) unsigned DEFAULT NULL,   `field_multi_was_philippin_value` float DEFAULT NULL,   `field_multi_philippin_value` float DEFAULT NULL,   `field_sold_counter_value` int(11) DEFAULT NULL,   `field_show_sold_counter_value` longtext,   `field_deal_of_the_day_value` longtext,   `field_tweet_summary_value` longtext,   `field_vintage_event_value` longtext,   `field_email_counter_value` int(11) DEFAULT NULL,   `field_popup_summary_value` longtext,   `field_image_taiwan_fid` int(11) DEFAULT NULL,   `field_image_taiwan_list` tinyint(4) DEFAULT NULL,   `field_image_taiwan_data` text,   `field_image_taiwan_iphone_fid` int(11) DEFAULT NULL,   `field_image_taiwan_iphone_list` tinyint(4) DEFAULT NULL,   `field_image_taiwan_iphone_data` text,   `field_item_descp_append_value` longtext,   `field_image_hongkong_fid` int(11) DEFAULT NULL,   `field_image_hongkong_list` tinyint(4) DEFAULT NULL,   `field_image_hongkong_data` text,   `field_image_hongkong_iphone_fid` int(11) DEFAULT NULL,   `field_image_hongkong_iphone_list` tinyint(4) DEFAULT NULL,   `field_image_hongkong_iphone_data` text,   `field_aesthetic_event_value` longtext,   `field_flash_sale_value` int(11) DEFAULT NULL,   `field_square_banner_fid` int(11) DEFAULT NULL,   `field_square_banner_list` tinyint(4) DEFAULT NULL,   `field_square_banner_data` text,   `field_mastercard_deals_value` int(11) DEFAULT NULL,   `field_multi_newzealand_value` float DEFAULT NULL,   `field_multi_was_newzealand_value` float DEFAULT NULL,   `field_city_frontpage_value` longtext,   `field_ipp_payment_my_value` longtext,   `field_ipp_payment_sg_value` longtext,   `field_reebonz_city_girl_value` int(11) DEFAULT NULL,   `field_ipp_payment_th_value` longtext,   `field_multi_was_canada_value` float DEFAULT NULL,   `field_multi_was_usa_value` float DEFAULT NULL,   `field_multi_canada_value` float DEFAULT NULL,   `field_multi_usa_value` float DEFAULT NULL,   `field_defective_event_value` longtext,   `field_multi_uae_value` float DEFAULT NULL,   `field_multi_arabia_value` float DEFAULT NULL,   `field_multi_was_uae_value` float DEFAULT NULL,   `field_multi_was_arabia_value` float DEFAULT NULL,   `field_multi_was_china_value` float DEFAULT NULL,   `field_multi_china_value` float DEFAULT NULL,   `field_tnc_format` int(10) unsigned DEFAULT NULL,   `field_image_china_fid` int(11) DEFAULT NULL,   `field_image_china_list` tinyint(4) DEFAULT NULL,   `field_image_china_data` text,   `field_catalogue_event_value` longtext,   `field_bespoke_image_fid` int(11) DEFAULT NULL,   `field_bespoke_image_list` tinyint(4) DEFAULT NULL,   `field_bespoke_image_data` text,   `field_bespoke_background_fid` int(11) DEFAULT NULL,   `field_bespoke_background_list` tinyint(4) DEFAULT NULL,   `field_bespoke_background_data` text,   `field_curator_text_value` longtext COLLATE utf8_unicode_ci,   `field_curator_name_value` longtext COLLATE utf8_unicode_ci,   `field_curator_pic_fid` int(11) DEFAULT NULL,   `field_curator_pic_list` tinyint(4) DEFAULT NULL,   `field_curator_pic_data` text COLLATE utf8_unicode_ci,   `field_multi_japan_value` float DEFAULT NULL,   `field_multi_was_japan_value` float DEFAULT NULL,   `field_multi_was_india_value` float DEFAULT NULL,   `field_multi_india_value` float DEFAULT NULL,   `field_multi_was_uk_value` float DEFAULT NULL,   `field_multi_uk_value` float DEFAULT NULL,   `field_multi_was_macau_value` float DEFAULT NULL,   `field_multi_macau_value` float DEFAULT NULL,   `field_multi_was_vietnam_value` float DEFAULT NULL,   `field_multi_vietnam_value` float DEFAULT NULL,   `field_ebay_category_value` longtext,   `field_curator_signature_value` longtext,   `field_curator_signature_link_value` longtext,   `field_event_weight_value` int(11) DEFAULT NULL,   `field_titan_event_id_value` longtext,   `field_multi_was_kuwait_value` float DEFAULT NULL,   `field_multi_kuwait_value` float DEFAULT NULL,   `field_subtitle_value` varchar(100) DEFAULT NULL,   PRIMARY KEY (`vid`),   KEY `nid` (`nid`),   KEY `field_start_date_value` (`field_start_date_value`),   KEY `field_end_date_value` (`field_end_date_value`),   KEY `field_not_allow_cart_value` (`field_not_allow_cart_value`),   KEY `field_allow_uob_installment_value` (`field_allow_uob_installment_value`),   KEY `field_event_weight_value` (`field_event_weight_value`),   FULLTEXT KEY `field_shipping_country` (`field_shipping_country`) ) ENGINE=MyISAM DEFAULT CHARSET=utf8 /*#dve  RANGE DISTRIBUTE ON (`nid`) USING `node_range` */",
				"CREATE TABLE if not exists `node_revisions` (`nid` int(10) unsigned NOT NULL DEFAULT '0',`vid` int(10) unsigned NOT NULL AUTO_INCREMENT,`uid` int(11) NOT NULL DEFAULT '0',`title` varchar(255) NOT NULL DEFAULT '',`body` longtext NOT NULL,`teaser` longtext NOT NULL,`log` longtext NOT NULL,`timestamp` int(11) NOT NULL DEFAULT '0',`format` int(11) NOT NULL DEFAULT '0',PRIMARY KEY (`vid`),KEY `nid` (`nid`),KEY `uid` (`uid`)) ENGINE=MyISAM AUTO_INCREMENT=78756037 DEFAULT CHARSET=utf8 /*#dve  RANGE DISTRIBUTE ON (`nid`) USING `node_range` */",
				"CREATE TABLE if not exists `uc_countries` (`country_id` int(10) unsigned NOT NULL,`country_name` varchar(255) NOT NULL DEFAULT '',`country_iso_code_2` char(2) NOT NULL DEFAULT '',`country_iso_code_3` char(3) NOT NULL DEFAULT '',`version` smallint(6) NOT NULL DEFAULT '0',PRIMARY KEY (`country_id`),KEY `country_name` (`country_name`),KEY `version` (`version`)) ENGINE=MyISAM DEFAULT CHARSET=utf8  /*#dve BROADCAST DISTRIBUTE */",
			};
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		for(String decl : decls) {
			tests.add(new StatementMirrorProc(decl));
		}
		tests.add(new StatementMirrorProc("SELECT nr.title AS title,COALESCE(c.country_name, 'Unknown') AS delivery_country FROM uc_orders o INNER JOIN uc_order_products op ON o.order_id = op.order_id INNER JOIN content_type_product p ON op.nid = p.nid INNER JOIN content_type_event e ON p.field_event_nid = e.nid INNER JOIN node_revisions nr ON e.nid = nr.nid LEFT JOIN uc_countries c ON o.delivery_country = c.country_id WHERE o.order_id = 604209 GROUP BY o.order_id"));
		runTest(tests);
	}

	@Test
	public void testPE1290() throws Throwable {
		String[] decls = new String[] { 
				"create table if not exists lhs1290 (`id` varchar(25) collate utf8_unicode_ci not null, `crud` int, primary key (`id`))",
				"create table if not exists rhs1290 (`id` varchar(25) collate utf8_unicode_ci not null, `crud` int, primary key (`id`))",
				"insert into lhs1290 (id,crud) values ('a',1),('b',2),('c',3)",
				"insert into rhs1290 (id,crud) values ('a',10),('b',20),('d',30)"
		};
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		for(String decl : decls) {
			tests.add(new StatementMirrorProc(decl));
		}
		tests.add(new StatementMirrorProc(
				"select l.crud, r.crud from lhs1290 l inner join rhs1290 r on l.id = r.id where r.crud = 'b' order by l.crud"));
		runTest(tests);
		
	}
	
	@Test
	public void testPE1369() throws Throwable {
		String[] decls = new String[] { 
				"create table bar1(col1 int)",
				"create table bar2(col1 int)",
		};
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		for(String decl : decls) {
			tests.add(new StatementMirrorProc(decl));
		}
		tests.add(new MirrorProc() {
			@Override
			public ResourceResponse execute(TestResource mr) throws Throwable {
				String dbName = mr.getDDL().getDatabases().get(0).getDatabaseName();
				String query = "select " + dbName + ".bar1.col1 from " + dbName + ".bar2, " + dbName + ".bar1 where " + dbName + ".bar1.col1=" + dbName + ".bar2.col1";
				return mr.getConnection().execute(query);			
			}
		});
		runTest(tests);
	}

	@Test
	public void testPE251() throws Throwable {
		final ArrayList<MirrorTest> basicTests = new ArrayList<MirrorTest>();
		basicTests.add(new StatementMirrorProc("CREATE table pe251A (id INT NOT NULL, value TEXT, PRIMARY KEY(id))"));
		basicTests.add(new StatementMirrorProc("CREATE table pe251B (value TEXT, id INT NOT NULL, PRIMARY KEY(id))"));
		
		basicTests.add(new StatementMirrorProc("INSERT INTO pe251A VALUES (1, 'one'), (2, 'two'), (3, 'three')"));
		basicTests.add(new StatementMirrorProc("INSERT INTO pe251B VALUES ('one', 1), ('two', 2), ('three', 3), ('four', 4)"));
		
		basicTests.add(new StatementMirrorFun(true, "SELECT * FROM pe251A a INNER JOIN pe251B b USING (id) ORDER BY a.id"));
		basicTests.add(new StatementMirrorFun(true, "SELECT * FROM pe251A a LEFT OUTER JOIN pe251B b USING (id) ORDER BY a.id"));
		basicTests.add(new StatementMirrorFun(true, "SELECT * FROM pe251A a RIGHT OUTER JOIN pe251B b USING (id) ORDER BY a.id"));
		
		runTest(basicTests);
		
		final ArrayList<MirrorTest> complexTests = new ArrayList<MirrorTest>();
		complexTests.add(new StatementMirrorProc("CREATE table pe251C (id1 INT NOT NULL, id2 INT NOT NULL, id3 INT NOT NULL, value TEXT, PRIMARY KEY(id1,id2,id3))"));
		complexTests.add(new StatementMirrorProc("CREATE table pe251D (id2 INT NOT NULL, id1 INT NOT NULL, id3 INT NOT NULL, value TEXT, PRIMARY KEY(id1,id2,id3))"));
		complexTests.add(new StatementMirrorProc("CREATE table pe251E (id3 INT NOT NULL, id2 INT NOT NULL, id1 INT NOT NULL, value TEXT, PRIMARY KEY(id1,id2,id3))"));
		
		complexTests.add(new StatementMirrorProc("INSERT INTO pe251C VALUES (1, 1, 1, 'c111'), (2, 1, 2, 'c212'), (3, 2, 1, 'c321'), (4, 1, 1, 'c411')"));
		complexTests.add(new StatementMirrorProc("INSERT INTO pe251D VALUES (1, 1, 1, 'd111'), (1, 2, 2, 'd212'), (1, 3, 1, 'd311'), (2, 4, 1, 'd421'), (2, 5, 2, 'd522')"));
		complexTests.add(new StatementMirrorProc("INSERT INTO pe251E VALUES (1, 1, 1, 'e111'), (1, 1, 2, 'e211'), (2, 1, 3, 'e312')"));
		
		complexTests.add(new StatementMirrorFun(true, "SELECT * FROM pe251C c INNER JOIN pe251D d USING (id3,id1,id2) ORDER BY c.id1"));
		complexTests.add(new StatementMirrorFun(true, "SELECT * FROM pe251C c LEFT OUTER JOIN pe251D d USING (id3,id1,id2) ORDER BY c.id1"));
		complexTests.add(new StatementMirrorFun(true, "SELECT * FROM pe251C c RIGHT OUTER JOIN pe251D d USING (id3,id1,id2) ORDER BY c.id1"));
		
		complexTests.add(new StatementMirrorFun(true, "SELECT * FROM pe251C c INNER JOIN pe251D d USING (id3,id1,id2) LEFT OUTER JOIN pe251E e USING (id1,id2,id3) ORDER BY c.id1"));
		complexTests.add(new StatementMirrorFun(true, "SELECT * FROM pe251C c INNER JOIN pe251D d USING (id3,id1,id2) RIGHT OUTER JOIN pe251E e USING (id1,id2,id3) ORDER BY c.id1"));
		
		complexTests.add(new StatementMirrorFun(true, "SELECT * FROM pe251C c RIGHT OUTER JOIN pe251D d USING (id3,id1,id2) INNER JOIN pe251E e USING (id1,id2,id3) ORDER BY c.id1"));
		complexTests.add(new StatementMirrorFun(true, "SELECT * FROM pe251C c LEFT OUTER JOIN pe251D d USING (id3,id1,id2) INNER JOIN pe251E e USING (id1,id2,id3) ORDER BY c.id1"));
		
		complexTests.add(new StatementMirrorFun(true, "SELECT * FROM pe251C c RIGHT OUTER JOIN pe251D d USING (id3,id1,id2) RIGHT OUTER JOIN pe251E e USING (id1,id2,id3) ORDER BY c.id1"));
		complexTests.add(new StatementMirrorFun(true, "SELECT * FROM pe251C c LEFT OUTER JOIN pe251D d USING (id3,id1,id2) RIGHT OUTER JOIN pe251E e USING (id1,id2,id3) ORDER BY c.id1"));
		complexTests.add(new StatementMirrorFun(true, "SELECT * FROM pe251C c RIGHT OUTER JOIN pe251D d USING (id3,id1,id2) LEFT OUTER JOIN pe251E e USING (id1,id2,id3) ORDER BY c.id1"));
		complexTests.add(new StatementMirrorFun(true, "SELECT * FROM pe251C c LEFT OUTER JOIN pe251D d USING (id3,id1,id2) LEFT OUTER JOIN pe251E e USING (id1,id2,id3) ORDER BY c.id1"));
		
		complexTests.add(new StatementMirrorFun(true, "SELECT * FROM pe251C c RIGHT JOIN pe251D d USING (id1,id2,id3) RIGHT OUTER JOIN pe251E e USING (id1,id3) ORDER BY c.id1"));
		complexTests.add(new StatementMirrorFun(true, "SELECT * FROM pe251C c LEFT JOIN pe251D d USING (id1,id2,id3) RIGHT OUTER JOIN pe251E e USING (id1,id3) ORDER BY c.id1"));
		complexTests.add(new StatementMirrorFun(true, "SELECT * FROM pe251C c RIGHT JOIN pe251D d USING (id1,id2,id3) LEFT OUTER JOIN pe251E e USING (id1,id3) ORDER BY c.id1"));
		complexTests.add(new StatementMirrorFun(true, "SELECT * FROM pe251C c LEFT JOIN pe251D d USING (id1,id2,id3) LEFT OUTER JOIN pe251E e USING (id1,id3) ORDER BY c.id1"));
		
		complexTests.add(new StatementMirrorFun(true, "SELECT * FROM pe251C c RIGHT JOIN pe251D d USING (id1,id2,id3) RIGHT OUTER JOIN pe251E e USING (id1) ORDER BY c.id1"));
		complexTests.add(new StatementMirrorFun(true, "SELECT * FROM pe251C c RIGHT JOIN pe251D d USING (id1,id2,id3) LEFT OUTER JOIN pe251E e USING (id1) ORDER BY c.id1"));
		
		runTest(complexTests);
	}
	
	@Ignore
	@Test
	public void testPE1459() throws Throwable {
		final ArrayList<MirrorTest> complexTests = new ArrayList<MirrorTest>();
		complexTests.add(new StatementMirrorProc("CREATE table pe251C (id1 INT NOT NULL, id2 INT NOT NULL, id3 INT NOT NULL, value TEXT, PRIMARY KEY(id1,id2,id3))"));
		complexTests.add(new StatementMirrorProc("CREATE table pe251D (id2 INT NOT NULL, id1 INT NOT NULL, id3 INT NOT NULL, value TEXT, PRIMARY KEY(id1,id2,id3))"));
		complexTests.add(new StatementMirrorProc("CREATE table pe251E (id3 INT NOT NULL, id2 INT NOT NULL, id1 INT NOT NULL, value TEXT, PRIMARY KEY(id1,id2,id3))"));
		
		complexTests.add(new StatementMirrorProc("INSERT INTO pe251C VALUES (1, 1, 1, 'c111'), (2, 1, 2, 'c212'), (3, 2, 1, 'c321'), (4, 1, 1, 'c411')"));
		complexTests.add(new StatementMirrorProc("INSERT INTO pe251D VALUES (1, 1, 1, 'd111'), (1, 2, 2, 'd212'), (1, 3, 1, 'd311'), (2, 4, 1, 'd421'), (2, 5, 2, 'd522')"));
		complexTests.add(new StatementMirrorProc("INSERT INTO pe251E VALUES (1, 1, 1, 'e111'), (1, 1, 2, 'e211'), (2, 1, 3, 'e312')"));
		
		complexTests.add(new StatementMirrorFun(true, "SELECT * FROM pe251C c LEFT JOIN pe251D d USING (id1,id2,id3) RIGHT OUTER JOIN pe251E e USING (id1) ORDER BY c.id1"));
		complexTests.add(new StatementMirrorFun(true, "SELECT * FROM pe251C c LEFT JOIN pe251D d USING (id1,id2,id3) LEFT OUTER JOIN pe251E e USING (id1) ORDER BY c.id1"));

		runTest(complexTests);
	}

	@Test
	public void testDuk() throws Throwable {
		String[] decls = new String[] { 
				"/*#dve create range if not exists node_range (int) persistent group " + sysDDL.getPersistentGroup().getName() + "*/",
				"CREATE TABLE simpletest642219book (`mlid` INT unsigned NOT NULL DEFAULT 0 COMMENT 'The book page’s simpletest642219menu_links.mlid.',`nid` INT unsigned NOT NULL DEFAULT 0 COMMENT 'The book page’s simpletest642219node.nid.',`bid` INT unsigned NOT NULL DEFAULT 0 COMMENT 'The book ID is the simpletest642219book.nid of the top-level page.',PRIMARY KEY (`mlid`),UNIQUE KEY `nid` (`nid`),INDEX `bid` (`bid`)) ENGINE = InnoDB DEFAULT CHARACTER SET utf8 COMMENT 'Stores book outline information. Uniquely connects each...' /*#dve  RANGE DISTRIBUTE ON (`nid`) USING `node_range` */",
				"CREATE TABLE simpletest642219menu_links (`menu_name` VARCHAR(32) NOT NULL DEFAULT '' COMMENT 'The menu name. All links with the same menu name (such as ’navigation’) are part of the same menu.', `mlid` INT unsigned NOT NULL auto_increment COMMENT 'The menu link ID (mlid) is the integer primary key.', `plid` INT unsigned NOT NULL DEFAULT 0 COMMENT 'The parent link ID (plid) is the mlid of the link above in the hierarchy, or zero if the link is at the top level in its menu.', `link_path` VARCHAR(255) NOT NULL DEFAULT '' COMMENT 'The Drupal path or external path this link points to.', `router_path` VARCHAR(255) NOT NULL DEFAULT '' COMMENT 'For links corresponding to a Drupal path (external = 0), this connects the link to a simpletest642219menu_router.path for joins.', `link_title` VARCHAR(255) NOT NULL DEFAULT '' COMMENT 'The text displayed for the link, which may be modified by a title callback stored in simpletest642219menu_router.', `options` BLOB NULL DEFAULT NULL COMMENT 'A serialized array of options to be passed to the url() or l() function, such as a query string or HTML attributes.', `module` VARCHAR(255) NOT NULL DEFAULT 'system' COMMENT 'The name of the module that generated this link.', `hidden` SMALLINT NOT NULL DEFAULT 0 COMMENT 'A flag for whether the link should be rendered in menus. (1 = a disabled menu item that may be shown on admin screens, -1 = a menu callback, 0 = a normal, visible link)', `external` SMALLINT NOT NULL DEFAULT 0 COMMENT 'A flag to indicate if the link points to a full URL starting with a protocol, like http:// (1 = external, 0 = internal).', `has_children` SMALLINT NOT NULL DEFAULT 0 COMMENT 'Flag indicating whether any links have this link as a parent (1 = children exist, 0 = no children).', `expanded` SMALLINT NOT NULL DEFAULT 0 COMMENT 'Flag for whether this link should be rendered as expanded in menus - expanded links always have their child links displayed, instead of only when the link is in the active trail (1 = expanded, 0 = not expanded)', `weight` INT NOT NULL DEFAULT 0 COMMENT 'Link weight among links in the same menu at the same depth.', `depth` SMALLINT NOT NULL DEFAULT 0 COMMENT 'The depth relative to the top level. A link with plid == 0 will have depth == 1.', `customized` SMALLINT NOT NULL DEFAULT 0 COMMENT 'A flag to indicate that the user has manually created or edited the link (1 = customized, 0 = not customized).', `p1` INT unsigned NOT NULL DEFAULT 0 COMMENT 'The first mlid in the materialized path. If N = depth, then pN must equal the mlid. If depth > 1 then p(N-1) must equal the plid. All pX where X > depth must equal zero. The columns p1 .. p9 are also called the parents.', `p2` INT unsigned NOT NULL DEFAULT 0 COMMENT 'The second mlid in the materialized path. See p1.', `p3` INT unsigned NOT NULL DEFAULT 0 COMMENT 'The third mlid in the materialized path. See p1.', `p4` INT unsigned NOT NULL DEFAULT 0 COMMENT 'The fourth mlid in the materialized path. See p1.', `p5` INT unsigned NOT NULL DEFAULT 0 COMMENT 'The fifth mlid in the materialized path. See p1.', `p6` INT unsigned NOT NULL DEFAULT 0 COMMENT 'The sixth mlid in the materialized path. See p1.', `p7` INT unsigned NOT NULL DEFAULT 0 COMMENT 'The seventh mlid in the materialized path. See p1.', `p8` INT unsigned NOT NULL DEFAULT 0 COMMENT 'The eighth mlid in the materialized path. See p1.', `p9` INT unsigned NOT NULL DEFAULT 0 COMMENT 'The ninth mlid in the materialized path. See p1.', `updated` SMALLINT NOT NULL DEFAULT 0 COMMENT 'Flag that indicates that this link was generated during the update from Drupal 5.', PRIMARY KEY (`mlid`), INDEX `path_menu` (`link_path`(128), `menu_name`), INDEX `menu_plid_expand_child` (`menu_name`, `plid`, `expanded`, `has_children`), INDEX `menu_parents` (`menu_name`, `p1`, `p2`, `p3`, `p4`, `p5`, `p6`, `p7`, `p8`, `p9`), INDEX `router_path` (`router_path`(128))) ENGINE = InnoDB DEFAULT CHARACTER SET utf8 COMMENT 'Contains the individual links within a menu.' /*#dve BROADCAST DISTRIBUTE */"
		};
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		for(String decl : decls) {
			tests.add(new StatementMirrorProc(decl));
		}
		tests.add(new StatementMirrorProc(
				"SELECT DISTINCT b.*, ml.* FROM simpletest642219book b INNER JOIN simpletest642219menu_links ml ON b.mlid = ml.mlid ORDER BY ml.weight ASC, ml.link_title ASC"));
		runTest(tests);
	}

	private final String netNamesQuery =
			"select distinct this_.id as y0_ "
			+"from RevisitSchedule this_ "
			+"inner join WebResult webresult1_ on this_.webResult_id=webresult1_.id "
			+"left outer join ScoreTag scoretag2_ on webresult1_.id=scoretag2_.webResult_id "
			+"where this_.revisitsRemaining>0 and scoretag2_.webResult_id not in "
			+"  (select this_.webResult_id as y0_ "
			+"   from ScoreTag this_ "
			+"   where (this_.tag_id=9 or this_.tag_id=6)) "
			+"and this_.nextRevisit<'2014-02-20 14:42:50' "
			+"order by this_.nextRevisit asc limit 10";
	
	@Test
	public void testDVE1494() throws Throwable {
		String[] decls = new String[] { 
				"/*#dve create range if not exists bigint_range (bigint) persistent group " + sysDDL.getPersistentGroup().getName() + "*/",
				"drop table if exists ScoreTag",
				"drop table if exists WebResult",
				"drop table if exists RevisitSchedule",
				"CREATE TABLE `RevisitSchedule` (   `id` bigint(20) NOT NULL AUTO_INCREMENT,   `nextRevisit` datetime DEFAULT NULL,   `revisitsRemaining` int(11) NOT NULL,   `revisitConfiguration_id` bigint(20) DEFAULT NULL,   `webResult_id` bigint(20) DEFAULT NULL,   `takedownsRemaining` int(11) NOT NULL,   `featureDefinition_id` bigint(20) DEFAULT NULL,   `cachePages` tinyint(1) NOT NULL,   `frequencyInDays` int(11) NOT NULL,   `stopIfHostIsDead` tinyint(1) NOT NULL,   `stopIfPageIsRemoved` tinyint(1) NOT NULL,   `alertStatus` int(11) DEFAULT NULL,   `complianceInProgress` tinyint(1) NOT NULL,   `email` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,   `reasonForStopping` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,   `revisitInProgress` tinyint(1) NOT NULL,   PRIMARY KEY (`id`),   UNIQUE KEY `webResult_id` (`webResult_id`),   KEY `FK1B295D8F7B382519` (`webResult_id`),   KEY `FK1B295D8FFB113E9B` (`revisitConfiguration_id`),   KEY `FK1B295D8F59D3C719` (`featureDefinition_id`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8 /*#dve  BROADCAST DISTRIBUTE */",
				"CREATE TABLE `WebResult` (   `id` bigint(20) NOT NULL AUTO_INCREMENT,   `abstract_` varchar(2048) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,   `deDocId` bigint(20) NOT NULL,   `deUrlId` bigint(20) NOT NULL,   `score` double NOT NULL,   `timeFirstSeen` datetime DEFAULT NULL,   `timeLastSeen` datetime DEFAULT NULL,   `title` varchar(2048) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,   `url` varchar(2048) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,   `country_id` bigint(20) DEFAULT NULL,   `host_id` bigint(20) DEFAULT NULL,   `isp_id` bigint(20) DEFAULT NULL,   `scan_id` bigint(20) DEFAULT NULL,   `cachedInTheLastVisit` bit(1) NOT NULL,   `urlHash` int(11) NOT NULL,   `lastCachedDocument_id` bigint(20) DEFAULT NULL,   `matchedImage_id` bigint(20) DEFAULT NULL,   PRIMARY KEY (`id`),   UNIQUE KEY `UK_5ym9txtpf6d5rtdq95vqq6j3` (`scan_id`, `urlHash`),   KEY `FK30EDD071BE981419` (`isp_id`),   KEY `FK30EDD071D8E4787B` (`host_id`),   KEY `FK30EDD0719534C5B` (`scan_id`),   KEY `FK30EDD071EC49D699` (`country_id`),   KEY `FK30EDD0712E2958E3` (`lastCachedDocument_id`),   KEY `FK30EDD071CF85855D` (`matchedImage_id`),   KEY `scan_id_urlHash` (`scan_id`, `urlHash`)) ENGINE=InnoDB AUTO_INCREMENT=6019 DEFAULT CHARSET=utf8 /*#dve  BROADCAST DISTRIBUTE */",
				"CREATE TABLE `ScoreTag` (   `id` bigint(20) NOT NULL AUTO_INCREMENT,   `feature` varchar(2048) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,   `score` double NOT NULL,   `tag_id` bigint(20) NOT NULL,   `webResult_id` bigint(20) NOT NULL,   `userTag_id` bigint(20) DEFAULT NULL,   `assignedByRules` tinyint(1) NOT NULL,   `timeAdded` datetime DEFAULT NULL,   PRIMARY KEY (`id`),   KEY `FKE6EA0BC87B382519` (`webResult_id`),   KEY `FKE6EA0BC8D05BCF99` (`tag_id`),   KEY `FKE6EA0BC87D3753D9` (`userTag_id`) ) ENGINE=InnoDB AUTO_INCREMENT=11771 DEFAULT CHARSET=utf8 /*#dve  RANGE DISTRIBUTE ON (`userTag_id`) USING `bigint_range` */"
		};
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		for(String decl : decls) {
			tests.add(new StatementMirrorProc(decl));
		}
		tests.add(new StatementMirrorProc(netNamesQuery));
		runTest(tests);
	}
	
	@Test
	public void testDVE1508() throws Throwable {
		String[] decls = new String[] {
				"/*#dve create range if not exists bigint_range (bigint) persistent group " + sysDDL.getPersistentGroup().getName() + "*/",
				"drop table if exists ScoreTag",
				"drop table if exists WebResult",
				"drop table if exists RevisitSchedule",
				"CREATE TABLE `RevisitSchedule` (   `id` bigint(20) NOT NULL AUTO_INCREMENT,   `nextRevisit` datetime DEFAULT NULL,   `revisitsRemaining` int(11) NOT NULL,   `revisitConfiguration_id` bigint(20) DEFAULT NULL,   `webResult_id` bigint(20) DEFAULT NULL,   `takedownsRemaining` int(11) NOT NULL,   `featureDefinition_id` bigint(20) DEFAULT NULL,   `cachePages` tinyint(1) NOT NULL,   `frequencyInDays` int(11) NOT NULL,   `stopIfHostIsDead` tinyint(1) NOT NULL,   `stopIfPageIsRemoved` tinyint(1) NOT NULL,   `alertStatus` int(11) DEFAULT NULL,   `complianceInProgress` tinyint(1) NOT NULL,   `email` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,   `reasonForStopping` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,   `revisitInProgress` tinyint(1) NOT NULL,   PRIMARY KEY (`id`),   UNIQUE KEY `webResult_id` (`webResult_id`),   KEY `FK1B295D8F7B382519` (`webResult_id`),   KEY `FK1B295D8FFB113E9B` (`revisitConfiguration_id`),   KEY `FK1B295D8F59D3C719` (`featureDefinition_id`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8 /*#dve  BROADCAST DISTRIBUTE */",
				"CREATE TABLE `WebResult` (   `id` bigint(20) NOT NULL AUTO_INCREMENT,   `abstract_` varchar(2048) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,   `deDocId` bigint(20) NOT NULL,   `deUrlId` bigint(20) NOT NULL,   `score` double NOT NULL,   `timeFirstSeen` datetime DEFAULT NULL,   `timeLastSeen` datetime DEFAULT NULL,   `title` varchar(2048) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,   `url` varchar(2048) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,   `country_id` bigint(20) DEFAULT NULL,   `host_id` bigint(20) DEFAULT NULL,   `isp_id` bigint(20) DEFAULT NULL,   `scan_id` bigint(20) DEFAULT NULL,   `cachedInTheLastVisit` bit(1) NOT NULL,   `urlHash` int(11) NOT NULL,   `lastCachedDocument_id` bigint(20) DEFAULT NULL,   `matchedImage_id` bigint(20) DEFAULT NULL,   PRIMARY KEY (`id`),   UNIQUE KEY `UK_5ym9txtpf6d5rtdq95vqq6j3` (`scan_id`, `urlHash`),   KEY `FK30EDD071BE981419` (`isp_id`),   KEY `FK30EDD071D8E4787B` (`host_id`),   KEY `FK30EDD0719534C5B` (`scan_id`),   KEY `FK30EDD071EC49D699` (`country_id`),   KEY `FK30EDD0712E2958E3` (`lastCachedDocument_id`),   KEY `FK30EDD071CF85855D` (`matchedImage_id`),   KEY `scan_id_urlHash` (`scan_id`, `urlHash`)) ENGINE=InnoDB AUTO_INCREMENT=6019 DEFAULT CHARSET=utf8 /*#dve  BROADCAST DISTRIBUTE */",
				"CREATE TABLE `ScoreTag` (   `id` bigint(20) NOT NULL AUTO_INCREMENT,   `feature` varchar(2048) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,   `score` double NOT NULL,   `tag_id` bigint(20) NOT NULL,   `webResult_id` bigint(20) NOT NULL,   `userTag_id` bigint(20) DEFAULT NULL,   `assignedByRules` tinyint(1) NOT NULL,   `timeAdded` datetime DEFAULT NULL,   PRIMARY KEY (`id`),   KEY `FKE6EA0BC87B382519` (`webResult_id`),   KEY `FKE6EA0BC8D05BCF99` (`tag_id`),   KEY `FKE6EA0BC87D3753D9` (`userTag_id`) ) ENGINE=InnoDB AUTO_INCREMENT=11771 DEFAULT CHARSET=utf8 /*#dve  RANGE DISTRIBUTE ON (`webResult_id`) USING `bigint_range` */"
		};
		ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
		for(String s : decls)
			tests.add(new StatementMirrorProc(s));
		tests.add(new StatementMirrorProc(netNamesQuery));
		runTest(tests);
	}

	@Test
	public void testPE1648() throws Throwable {
		final String[] decls = new String[] {
				"CREATE TABLE pe1648_t1 (i INT, j INT)",
				"CREATE TABLE pe1648_t2 (j INT, k INT)",
				"INSERT INTO pe1648_t1 VALUES(1,1)",
				"INSERT INTO pe1648_t1 VALUES(2,2)",
				"INSERT INTO pe1648_t1 VALUES(3,3)",
				"INSERT INTO pe1648_t2 VALUES(1,1)",
				"INSERT INTO pe1648_t2 VALUES(3,2)",
				"INSERT INTO pe1648_t2 VALUES(4,3)",
		};

		final List<MirrorTest> tests = new ArrayList<MirrorTest>();
		for (final String stmt : decls) {
			tests.add(new StatementMirrorProc(stmt));
		}
		tests.add(new StatementMirrorFun("SELECT * FROM pe1648_t1 t1 NATURAL JOIN pe1648_t2 t2 ORDER BY t1.i"));
		tests.add(new StatementMirrorFun("SELECT * FROM pe1648_t2 t2 NATURAL JOIN pe1648_t1 t1 ORDER BY t1.i"));
		tests.add(new StatementMirrorFun("SELECT * FROM pe1648_t1 t1 NATURAL LEFT JOIN pe1648_t2 t2 ORDER BY t1.i"));
		tests.add(new StatementMirrorFun("SELECT * FROM pe1648_t2 t2 NATURAL LEFT JOIN pe1648_t1 t1 ORDER BY t1.i"));

		//		Previously, this query would produce an error ERROR 1052 (23000): Column 'b' in where clause is ambiguous. Now the query produces the correct result.
		//		tests.add(new StatementMirrorFun("SELECT * FROM pe1648_t1 t1 NATURAL JOIN pe1648_t2 t2 WHERE j > 3"));

		runTest(tests);
	}
}
