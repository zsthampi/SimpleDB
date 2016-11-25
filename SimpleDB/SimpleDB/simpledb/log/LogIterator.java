package simpledb.log;

import static simpledb.file.Page.INT_SIZE;
import static simpledb.file.Page.TWICE_INT_SIZE;
import simpledb.file.*;
import simpledb.server.SimpleDB;

import java.util.Iterator;

/**
 * A class that provides the ability to move through the records of the log file
 * in reverse order.
 * 
 * @author Edward Sciore
 */
public class LogIterator implements Iterator<BasicLogRecord> {
	private Block blk;
	private Page pg = new Page();
	private int currentrec;

	/**
	 * Creates an iterator for the records in the log file, positioned after the
	 * last log record. This constructor is called exclusively by
	 * {@link LogMgr#iterator()}.
	 */
	LogIterator(Block blk) {
		this.blk = blk;
		pg.read(blk);
		currentrec = pg.getInt(LogMgr.LAST_POS);
	}

	/**
	 * Determines if the current log record is the earliest record in the log
	 * file.
	 * 
	 * @return true if there is an earlier record
	 */
	public boolean hasNext() {
		return currentrec > 0 || blk.number() > 0;
	}

	public void switchModeToForward() {
		currentrec += INT_SIZE;
	}

	public boolean hasNextForward() {
		int totalBlocks = SimpleDB.fileMgr().size(blk.fileName()) - 1;
		return (pg.getInt(currentrec) != INT_SIZE) || (blk.number() < totalBlocks);
	}

	/**
	 * Moves to the next log record in reverse order. If the current log record
	 * is the earliest in its block, then the method moves to the next oldest
	 * block, and returns the log record from there.
	 * 
	 * @return the next earliest log record
	 */
	public BasicLogRecord next() {
		if (currentrec == 0)
			moveToNextBlock();
		currentrec = pg.getInt(currentrec);
		return new BasicLogRecord(pg, currentrec + TWICE_INT_SIZE);
	}

	public BasicLogRecord nextForward() {
		if (pg.getInt(currentrec) == INT_SIZE)
			moveToNextForwardBlock();
		int tempCurrentRec = currentrec;
		currentrec = pg.getInt(currentrec);
		return new BasicLogRecord(pg, tempCurrentRec + INT_SIZE);

	}

	public void remove() {
		throw new UnsupportedOperationException();
	}

	/**
	 * Moves to the next log block in reverse order, and positions it after the
	 * last record in that block.
	 */
	private void moveToNextBlock() {
		blk = new Block(blk.fileName(), blk.number() - 1);
		pg.read(blk);
		currentrec = pg.getInt(LogMgr.LAST_POS);
	}

	private void moveToNextForwardBlock() {
		blk = new Block(blk.fileName(), blk.number() + 1);
		pg.read(blk);
		currentrec = INT_SIZE;
	}
}