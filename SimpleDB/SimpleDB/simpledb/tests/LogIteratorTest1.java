package simpledb.tests;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferAbortException;
import simpledb.buffer.BufferMgr;
import simpledb.file.Block;
import simpledb.server.SimpleDB;
import simpledb.tx.recovery.LogRecordIterator;
import simpledb.tx.recovery.RecoveryMgr;

public class LogIteratorTest1 {

	public static void main(String[] args) {
		// Create a simpleDB client
		SimpleDB.init("tpdb");
		
		// Initialize required objects and variables
		int lsn;
		Block blk[] = new Block[2];
		Buffer buff = new Buffer();
		new SimpleDB();
		BufferMgr basicBufferMgr = SimpleDB.bufferMgr();
		blk[0] = new Block("temp", 0);
		blk[1] = new Block("temp", 1);
		
		// Pin block 0 and block 1 to the buffer
		try {
			buff = basicBufferMgr.pin(blk[0]);
			System.out.println(buff.block().number()+" is pinned");
			buff = basicBufferMgr.pin(blk[1]);
			System.out.println(buff.block().number()+" is pinned");
		}
		catch (BufferAbortException e) {System.out.println(e+ " | Buffer is full");}
		
		/* 
		 * Create recovery manager object for dummy transaction number 124
		 * add updates to the recovery manager object
		 */
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
		
		// Runs the original log iterator to print the log in reverse direction
		LogRecordIterator it = new LogRecordIterator();
		while(it.hasNext()){System.out.println(it.next());}
		
		System.out.println("SWITCHING TO FORWARD!!!!!!");
		
		// Runs the new log iterator to print the log in forward direction
		it.switchModeToForward();
		while(it.hasNextForward()){System.out.println(it.nextForward());}
			
	}

}
