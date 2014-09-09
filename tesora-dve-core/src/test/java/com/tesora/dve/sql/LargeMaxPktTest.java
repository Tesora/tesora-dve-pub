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

import com.tesora.dve.db.mysql.portal.protocol.Packet;
import com.tesora.dve.sql.transform.strategy.RuntimeLimitSpecification;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testng.Assert;

import com.tesora.dve.db.mysql.portal.protocol.MSPComQueryRequestMessage;
import com.tesora.dve.sql.util.MirrorTest;
import com.tesora.dve.sql.util.NativeDDL;
import com.tesora.dve.sql.util.PEDDL;
import com.tesora.dve.sql.util.ProjectDDL;
import com.tesora.dve.sql.util.StorageGroupDDL;
import com.tesora.dve.worker.WorkerGroup.WorkerGroupFactory;

public class LargeMaxPktTest extends SchemaMirrorTest {
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
		final ExtendedPacketTester tester = new ExtendedPacketTester(67108864);
		tester.add(new StatementMirrorProc("CREATE TABLE `pe1512` (`data` longblob)"));
		tester.add(new StatementMirrorProc("INSERT INTO `pe1512` VALUES ('" + StringUtils.repeat("0", 17000000) + "')"));
		tester.add(new StatementMirrorFun("SELECT length(data) FROM `pe1512`"));
		// TODO: There is a bug (PE-1515) with the MysqlTextResultChunkProvider (used in tests) 
		// that it doesn't handle extended packets properly 
		//tester.add(new StatementMirrorFun("SELECT data FROM `pe1512`"));

		tester.runTests();
	}

	@Test
	public void testPE1559() throws Throwable {
		try {
//            final String payload = FileUtils.readFileToString(getFileFromLargeFileRepository("pe1559_payload.dat"));
            int desiredLength = 34000000;
            Assume.assumeTrue("Didn't have enough memory to be confident test would run, skipped.", Runtime.getRuntime().maxMemory() >= (desiredLength * 20L));

            final String payload = largeRandomString("testPE1559", desiredLength); //two full extended packets, plus ~500K

			final ExtendedPacketTester tester = new ExtendedPacketTester(67108864);
			tester.add(new StatementMirrorProc(
					"CREATE TABLE `cache_views` ("
							+ "`cid` varchar(255) NOT NULL DEFAULT '',"
							+ "`data` longblob,"
							+ "`expire` int(11) NOT NULL DEFAULT '0',"
							+ "`created` int(11) NOT NULL DEFAULT '0',"
							+ "`serialized` smallint(6) NOT NULL DEFAULT '0',"
							+ "PRIMARY KEY (`cid`),"
							+ "KEY `expire` (`expire`)"
							+ ") ENGINE=InnoDB DEFAULT CHARSET=utf8 /*#dve BROADCAST DISTRIBUTE */"));
			tester.add(new StatementMirrorProc("INSERT INTO `cache_views` (cid) VALUES ('views_data:en')"));
			tester.add(new StatementMirrorProc("UPDATE `cache_views` SET serialized='1', created='1403888529', expire='0', data='"
					+ payload + "' WHERE (cid = 'views_data:en')"));
			tester.add(new StatementMirrorFun("SELECT length(data) FROM `cache_views`"));

			tester.add(new StatementMirrorFun("SELECT data FROM `cache_views`"));

			tester.runTests();
		} catch (final LargeTestResourceNotAvailableException e) {
			System.err.println("WARNING: This test will be ignored: " + e.getMessage());
			return;
		}
	}

    private String largeRandomString(String testName, int desiredLength) throws LargeTestResourceNotAvailableException {
        Random rand = new Random(938398373L); //fix the seed, so we always generate the same string.

        StringBuilder builder = new StringBuilder(desiredLength);
        int remainingChars = desiredLength;
        while (remainingChars > 0){
            char entry = (char)('a' + rand.nextInt(26));//this generates only ASCII lowercase 'a' through 'z'.
            builder.append(entry);
            remainingChars--;
        }

//            final String payload = FileUtils.readFileToString(getFileFromLargeFileRepository("pe1559_payload.dat"));
        final String payload = builder.toString();
        return payload;
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

        MSPComQueryRequestMessage outboundMessage = MSPComQueryRequestMessage.newMessage(source.array() );
        Packet.encodeFullMessage((byte)0, outboundMessage, dest);

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

        MSPComQueryRequestMessage outboundMessage = MSPComQueryRequestMessage.newMessage(source.array() );
        Packet.encodeFullMessage((byte)0, outboundMessage, dest);

        int lengthOfNonUserdata = 5 + 4 + 4 + 4 + 4;//last packet has zero length payload
        Assert.assertEquals(dest.readableBytes(),payloadSize + lengthOfNonUserdata,"Number of bytes in destination buffer is wrong");

        Assert.assertEquals(dest.getUnsignedMedium(0),defaultMaxPacket,"First length should be maximum");
        Assert.assertEquals(dest.getByte(3),(byte)0,"First sequenceID should be zero");
        Assert.assertEquals(dest.getByte(4),(byte)MSPComQueryRequestMessage.TYPE_IDENTIFIER,"First byte of payload should be MSPComQueryRequestMessage.TYPE_IDENTIFIER");

        ByteBuf sliceFirstPayload = dest.slice(5, (0xFFFFFF - 1));
        Assert.assertEquals(sliceFirstPayload,source.slice(0,0xFFFFFF - 1));

    }
}
