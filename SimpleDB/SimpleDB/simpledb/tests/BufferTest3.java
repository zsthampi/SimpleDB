package simpledb.tests;
import simpledb.buffer.*;
import simpledb.file.Block;
import simpledb.server.SimpleDB;

/*
 *  created buffer pool with number of buffers available = 8
 *  pinned each buffer with a block and number of available buffers will get 0 and then pin block 2 again
 *  unpinned block 5 and 2 from the pool and now number of buffers available  = 1 as 2 has still pin count = 1
 *  pinned new blocks 9 and 8, block 9 will be pinned at 5 as 2 is still having pin count = 1 and block 8 will
 *  wait for the buffer but since there is no buffer available, it will abort
 */
public class BufferTest3 {

	public static void main(String[] args) throws NullPointerException{
		// Create a simpleDB client
		SimpleDB.init("tpdb");
		
		// Initialize required objects and variables
		Block blk[] = new Block[10];
		Buffer buff = new Buffer();
		new SimpleDB();
		BufferMgr basicBufferMgr = SimpleDB.bufferMgr();
		System.out.println("Number of Block available " + basicBufferMgr.available());
		
		// Create 8 new blocks and pin them to the buffer
		for(int i=0;i<8;i++){
			blk[i] = new Block("temp", i);
			try {
				buff = basicBufferMgr.pin(blk[i]);
				System.out.println(buff.block().number()+" is pinned");
			}
			catch (BufferAbortException e) {System.out.println(e+ " | Buffer is full");}
			System.out.println("Number of Block available " + basicBufferMgr.available());
		}
		
		// Pin block 2 to the buffer again increasing it pin count to 2
		try {
			buff = basicBufferMgr.pin(blk[2]);
			System.out.println(buff.block().number()+" is pinned");
		}
		catch (BufferAbortException e) {System.out.println(e+ " | Buffer is full");}
		System.out.println("Number of Block available " + basicBufferMgr.available());
		
		// Create 2 more blocks for testing later
		blk[9] = new Block("temp", 9);
		blk[8] = new Block("temp", 8);
		
		/*
		 *  Unpin block 5 and 2 from the buffer in that order
		 *  block 2 will now have a pin count of 1
		 */
		buff = basicBufferMgr.getMapping(blk[5]);
		basicBufferMgr.unpin(buff);
		System.out.println(buff.block().number()+ " is unpinned");
		System.out.println("Number of Block available " + basicBufferMgr.available());
		
		buff = basicBufferMgr.getMapping(blk[2]);
		basicBufferMgr.unpin(buff);
		System.out.println(buff.block().number()+ " is unpinned");
		System.out.println("Number of Block available " + basicBufferMgr.available());
		
		// Even though blocks 5 and 2 have been unpinned they will be available in the buffer
		for(int i=0;i<10;i++){
			System.out.println("Is " + blk[i].number()+ " is in pool: "+ basicBufferMgr.containsMapping(blk[i]));
		}
		
		/*
		 * Now pin block 9 and block 8 in that order.
		 * We will see in the buffer mapping that:
		 * 1. Block 9 is added to the buffer by replacing block 5
		 * 2. Block 8 gets timed out as no buffer will be available
		 * This shows that FIFO technique maintains the pin count in the buffers
		 */
		try{
			buff = basicBufferMgr.pin(blk[9]);
			System.out.println(buff.block().number()+" is pinned");
			for(int i=0;i<10;i++){
				System.out.println("Is " + blk[i].number()+ " is in pool: "+ basicBufferMgr.containsMapping(blk[i]));
			}
			buff = basicBufferMgr.pin(blk[8]);
			System.out.println(buff.block().number()+" is pinned");
			
		}
		catch (BufferAbortException e) {System.out.println(e+ " | Buffer is full");}
		for(int i=0;i<10;i++){
			System.out.println("Is " + blk[i].number()+ " is in pool: "+ basicBufferMgr.containsMapping(blk[i]));
		}
	}

}
