package simpledb.tests;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferAbortException;
import simpledb.buffer.BufferMgr;
import simpledb.file.Block;
import simpledb.server.SimpleDB;
import simpledb.tx.recovery.LogRecordIterator;
import simpledb.tx.recovery.RecoveryMgr;

public class LogIteratorTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int lsn;
		SimpleDB.init("tpdb");
		Block blk[] = new Block[10];
		Buffer buff = new Buffer();
		new SimpleDB();
		BufferMgr basicBufferMgr = SimpleDB.bufferMgr();
		
		for(int i=0;i<10;i++){
			blk[i] = new Block("filename", i);
		}
		
		try {
			buff = basicBufferMgr.pin(blk[0]);
			System.out.println(buff.block().number()+" is pinned");
			buff = basicBufferMgr.pin(blk[1]);
			System.out.println(buff.block().number()+" is pinned");
		}
		catch (BufferAbortException e) {System.out.println(e);}
		
		RecoveryMgr rm = new RecoveryMgr(124);
		
		buff = basicBufferMgr.getMapping(blk[0]);
		lsn = rm.setInt(buff, 4, 1234);
		buff.setInt(4, 1234, 124, lsn);
		
		lsn = rm.setInt(buff, 4, 5678);
		buff.setInt(4, 5678, 124, lsn);
		
		buff = basicBufferMgr.getMapping(blk[1]);
		lsn = rm.setString(buff, 4, "abc");
		buff.setString(4, "abc", 124, lsn);
		
		lsn = rm.setString(buff, 4, "def");
		buff.setString(4, "def", 124, lsn);
		
		LogRecordIterator it = new LogRecordIterator();
		while(it.hasNext()){System.out.println(it.next());}
		
		System.out.println("SWITCHING TO FORWARD!!!!!!");
		
		it.switchModeToForward();
		while(it.hasNextForward()){System.out.println(it.nextForward());}
			
	}

}
