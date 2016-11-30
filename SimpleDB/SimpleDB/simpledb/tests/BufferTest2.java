package simpledb.tests;
import simpledb.buffer.*;
import simpledb.file.Block;
import simpledb.server.SimpleDB;

/*
 *  created buffer pool with number of buffers available = 8
 *  pinned first 7 buffers with a block and number of available buffers will get 1
 *  unpinned block 0 from the pool and now number of buffers available  = 2
 *  pinned new blocks 8 and 7, and these will be pinned respectively
 *  at the first available buffer which is 8 and 0 respectively.
 */
public class BufferTest2 {

	public static void main(String[] args) throws NullPointerException{
		// Create a simpleDB client
		SimpleDB.init("tpdb");
		
		// Initialize required objects and variables
		Block blk[] = new Block[10];
		Buffer buff = new Buffer();
		new SimpleDB();
		BufferMgr basicBufferMgr = SimpleDB.bufferMgr();
		System.out.println("Number of Block available " + basicBufferMgr.available());
		
		// Create 7 new blocks and pin them to the buffer
		for(int i=0;i<7;i++){
			blk[i] = new Block("temp", i);
			try {
				buff = basicBufferMgr.pin(blk[i]);
				System.out.println(buff.block().number()+" is pinned");
			}
			catch (BufferAbortException e) {System.out.println(e+ " | Buffer is full");}
			System.out.println("Number of Block available " + basicBufferMgr.available());
		}
		
		// Create 2 more blocks for testing later
		blk[8] = new Block("temp", 8);
		blk[7] = new Block("temp", 7);
		
		// Unpin block 0 from the buffer
		buff = basicBufferMgr.getMapping(blk[0]);
		basicBufferMgr.unpin(buff);
		System.out.println(buff.block().number()+ " is unpinned");
		System.out.println("Number of Block available " + basicBufferMgr.available());
		
		// Even though blocks 0 has been unpinned it will be available in the buffer
		for(int i=0;i<9;i++){
			System.out.println("Is " + blk[i].number()+ " is in pool: "+ basicBufferMgr.containsMapping(blk[i]));
		}
		
		/*
		 * Now pin block 8 and block 7 in that order.
		 * We will see in the buffer mapping that:
		 * 1. Block 8 is added to the buffer in the latest unpinned location 
		 * which is the last buffer.
		 * 2. Block 7 is added to the buffer by replacing block 0
		 * This shows that FIFO technique is used during Buffer Management
		 */
		try{
			buff = basicBufferMgr.pin(blk[8]);
			System.out.println(buff.block().number()+" is pinned");
			for(int i=0;i<9;i++){
				System.out.println("Is " + blk[i].number()+ " is in pool: "+ basicBufferMgr.containsMapping(blk[i]));
			}
			buff = basicBufferMgr.pin(blk[7]);
			System.out.println(buff.block().number()+" is pinned");
			
		}
		catch (BufferAbortException e) {System.out.println(e+ " | Buffer is full");}
		for(int i=0;i<9;i++){
			System.out.println("Is " + blk[i].number()+ " is in pool: "+ basicBufferMgr.containsMapping(blk[i]));
		}
	}

}
