package simpledb.tests;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferMgr;
import simpledb.file.*;
import simpledb.server.SimpleDB;
import simpledb.tx.*;
import simpledb.tx.recovery.LogRecordIterator;

public class RecoveryTest2 {
	public static void main(String args[]) {
		// Create a simpleDB client
		SimpleDB.init("simpleDB");
		// This step calls the doRecovery method too! 
		
		// Create a block 
		Block blk = new Block("recovery_test",0);
		
		// Create 3 transactions - Recovery Manager objects
		// are created automatically for these.
		// SimpleDB class creates a dummy transaction with transaction number 1
		// Transaction numbers in logs are txn2 - 2, txn3 - 3, txn4 - 4
		Transaction txn2 = new Transaction();
		Transaction txn3 = new Transaction();
		Transaction txn4 = new Transaction();
		
		txn2.pin(blk);
		txn2.setString(blk,50,"This code should disappear!");
		txn2.unpin(blk);
		txn2.commit();
		
		txn3.pin(blk);
		txn3.setString(blk, 100, "This is the old value!");
		txn3.setString(blk, 100, "Hurray! It worked!");
		txn3.unpin(blk);
		txn3.commit();
		
		txn4.pin(blk);
		txn4.setInt(blk, 150, 0);
		txn4.setString(blk, 0, "This Recovery Manager will never work!");
		txn4.unpin(blk);
		txn4.commit();
		
		// Iterate through Log Records and print them 
		System.out.println("###### LOG CONTENTS ###########");
		LogRecordIterator iter = new LogRecordIterator();
		while (iter.hasNext()) {
			System.out.println(iter.next());
		}
		System.out.println("###############################");
			
		// Call the recovery - We just need to call it once (from any transaction)
		// It will check the common log file
		// This will print out the sequence of operations in the console. 
		// Please refer to method - RecoveryMgr.doRecover() for additional details.
		txn2.recover(); 
		
		// We can see the records in txn3 are undone (since it is not committed) 
		// Records for txn2 are committed 
		// Records for txn1 are rolled-back 
	}
}
