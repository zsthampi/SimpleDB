package simpledb.tests;
import simpledb.buffer.*;
import simpledb.file.Block;
import simpledb.server.SimpleDB;

public class BufferTest {

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
