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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testng.Assert;

import com.tesora.dve.db.mysql.portal.protocol.MSPComQueryRequestMessage;
import com.tesora.dve.sql.util.MirrorTest;
import com.tesora.dve.sql.util.NativeDDL;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.ResourceResponse;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.worker.WorkerGroup.WorkerGroupFactory;

public class LargeMaxPktTest extends MysqlConnSchemaMirrorTest {
	private static final int SITES = 5;

	private static final ProjectDDL sysDDL = new PEDDL("sysdb",
				new StorageGroupDDL("sys",SITES,"sysg"),
				"schema");
	static final NativeDDL nativeDDL = new NativeDDL("cdb");

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
		// in order to change the max_allowed_packet properly, we need to make sure we don't cache backend connections
		// that could contain the old value
		setup(sysDDL,null,nativeDDL,getSchema());
		WorkerGroupFactory.suppressCaching();
	}
	
	@AfterClass
	public static void cleanup() throws Throwable {
		WorkerGroupFactory.restoreCaching();
	}

	private static List<MirrorTest> getSchema() {
		ArrayList<MirrorTest> out = new ArrayList<MirrorTest>();
		return out;
	}
	
	@Test
	public void testPE1512() throws Throwable {
		Long saveMaxPkt = null;
		try {
			// We need to exceed the default value of 16M.
			ResourceResponse saveMaxPktRR = nativeResource.getConnection().execute("SHOW GLOBAL VARIABLES like 'max_allowed_packet'");
			saveMaxPkt = (Long.valueOf((String) saveMaxPktRR.getResults().get(0).getResultColumn(2).getColumnValue())).longValue();

			nativeResource.getConnection().execute("SET GLOBAL max_allowed_packet = 67108864");

			/* Refresh the 'max_allowed_packet' variable. */
			disconnect();
            TimeUnit.SECONDS.sleep(10);//TODO: hack to deal with race condition where fast disconnect/reconnect after a response still picks up old value. -sgossard
			connect();

			// Avoid out of heap.
			ResourceResponse.BLOB_COLUMN.useFormatedOutput(false);

			final ArrayList<MirrorTest> tests = new ArrayList<MirrorTest>();
			tests.add(new StatementMirrorProc("CREATE TABLE `pe1512` (`data` longblob)"));
			tests.add(new StatementMirrorProc("INSERT INTO `pe1512` VALUES ('" + StringUtils.repeat("0", 17000000) + "')"));
			tests.add(new StatementMirrorFun("SELECT length(data) FROM `pe1512`"));
			// TODO: There is a bug (PE-1515) with the MysqlTextResultChunkProvider (used in tests) 
			// that it doesn't handle extended packets properly 
			//tests.add(new StatementMirrorFun("SELECT data FROM `pe1512`"));

			runTest(tests);
		} finally {
			ResourceResponse.BLOB_COLUMN.useFormatedOutput(true);
			if (saveMaxPkt != null) {
				nativeResource.getConnection().execute("SET GLOBAL max_allowed_packet = " + saveMaxPkt);
			}
		}
	}

    @Test
    public void testComQueryMessageContinuationOverlap() throws Exception {
        int defaultMaxPacket = 0xFFFFFF;
        int payloadSize = (defaultMaxPacket * 4) + (4 * 40);  //four full packets and a little change, divisible by 4.
        ByteBuf source = Unpooled.buffer(payloadSize,payloadSize);
        Random rand = new Random(239873L);
        while (source.isWritable())
            source.writeInt(rand.nextInt());
        Assert.assertEquals(source.writableBytes(),0,"Oops, I intended to fill up the source buffer");

        ByteBuf dest = Unpooled.buffer(payloadSize);

        MSPComQueryRequestMessage outboundMessage = new MSPComQueryRequestMessage((byte)0, source.slice() );
        outboundMessage.writeTo(dest);

        int lengthOfNonUserdata = 5 + 4 + 4 + 4 + 4;
        Assert.assertEquals(dest.readableBytes(),payloadSize + lengthOfNonUserdata,"Number of bytes in destination buffer is wrong");

        Assert.assertEquals(dest.getUnsignedMedium(0),defaultMaxPacket,"First length should be maximum");
        Assert.assertEquals(dest.getByte(3),(byte)0,"First sequenceID should be zero");
        Assert.assertEquals(dest.getByte(4),(byte)MSPComQueryRequestMessage.TYPE_IDENTIFIER,"First byte of payload should be MSPComQueryRequestMessage.TYPE_IDENTIFIER");

        ByteBuf sliceFirstPayload = dest.slice(5, (0xFFFFFF - 1));
        Assert.assertEquals(sliceFirstPayload,source.slice(0,0xFFFFFF - 1));

    }

    @Test
    public void testComQueryMessageContinuationExact() throws Exception {
        int defaultMaxPacket = 0xFFFFFF;
        int payloadSize = (defaultMaxPacket * 4);  //four full packets exactly, requires an empty trailing packet.
        ByteBuf source = Unpooled.buffer(payloadSize,payloadSize);
        Random rand = new Random(239873L);
        while (source.isWritable())
            source.writeInt(rand.nextInt());
        Assert.assertEquals(source.writableBytes(),0,"Oops, I intended to fill up the source buffer");

        ByteBuf dest = Unpooled.buffer(payloadSize);

        MSPComQueryRequestMessage outboundMessage = new MSPComQueryRequestMessage((byte)0, source.slice() );
        outboundMessage.writeTo(dest);

        int lengthOfNonUserdata = 5 + 4 + 4 + 4 + 4;//last packet has zero length payload
        Assert.assertEquals(dest.readableBytes(),payloadSize + lengthOfNonUserdata,"Number of bytes in destination buffer is wrong");

        Assert.assertEquals(dest.getUnsignedMedium(0),defaultMaxPacket,"First length should be maximum");
        Assert.assertEquals(dest.getByte(3),(byte)0,"First sequenceID should be zero");
        Assert.assertEquals(dest.getByte(4),(byte)MSPComQueryRequestMessage.TYPE_IDENTIFIER,"First byte of payload should be MSPComQueryRequestMessage.TYPE_IDENTIFIER");

        ByteBuf sliceFirstPayload = dest.slice(5, (0xFFFFFF - 1));
        Assert.assertEquals(sliceFirstPayload,source.slice(0,0xFFFFFF - 1));

    }
}
