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
		// TODO Auto-generated method stub
		SimpleDB.init("tpdb");
		Block blk[] = new Block[10];
		Buffer buff = new Buffer();
		new SimpleDB();
		BufferMgr basicBufferMgr = SimpleDB.bufferMgr();
	
		System.out.println("Number of Block available " + basicBufferMgr.available());
		for(int i=0;i<8;i++){
			blk[i] = new Block("temp", i);
			try {
				buff = basicBufferMgr.pin(blk[i]);
				//System.out.println("Block "+ i +" pinned");
				System.out.println(buff.block().number()+" is pinned");
			}
			catch (BufferAbortException e) {System.out.println(e);}
			System.out.println("Number of Block available " + basicBufferMgr.available());
		}
		
		try {
			buff = basicBufferMgr.pin(blk[2]);
			//System.out.println("Block "+ i +" pinned");
			System.out.println(buff.block().number()+" is pinned");
		}
		catch (BufferAbortException e) {System.out.println(e);}
		System.out.println("Number of Block available " + basicBufferMgr.available());
		
		blk[9] = new Block("temp", 9);
		blk[8] = new Block("temp", 8);
		
		//System.out.println(basicBufferMgr.containsMapping(blk[0]));
		buff = basicBufferMgr.getMapping(blk[5]);
		basicBufferMgr.unpin(buff);
		System.out.println(buff.block().number()+ " is unpinned");
		//System.out.println(basicBufferMgr.containsMapping(blk[5]));
		System.out.println("Number of Block available " + basicBufferMgr.available());
		
		buff = basicBufferMgr.getMapping(blk[2]);
		basicBufferMgr.unpin(buff);
		System.out.println(buff.block().number()+ " is unpinned");
		//System.out.println(basicBufferMgr.containsMapping(blk[0]));
		System.out.println("Number of Block available " + basicBufferMgr.available());
		
		for(int i=0;i<10;i++){
			System.out.println("Is " + blk[i].number()+ " is in pool: "+ basicBufferMgr.containsMapping(blk[i]));
		}
		
		try{
			buff = basicBufferMgr.pin(blk[9]);
			System.out.println(buff.block().number()+" is pinned");
			for(int i=0;i<10;i++){
				System.out.println("Is " + blk[i].number()+ " is in pool: "+ basicBufferMgr.containsMapping(blk[i]));
			}
			buff = basicBufferMgr.pin(blk[8]);
			System.out.println(buff.block().number()+" is pinned");
			
		}
		catch (BufferAbortException e) {System.out.println(e);}
		for(int i=0;i<10;i++){
			System.out.println("Is " + blk[i].number()+ " is in pool: "+ basicBufferMgr.containsMapping(blk[i]));
		}
	}

}
