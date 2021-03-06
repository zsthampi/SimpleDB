package simpledb.buffer;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import simpledb.file.Block;
import simpledb.file.FileMgr;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * 
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {
  private Map<Block, Buffer> bufferPoolMap;
  private int numAvailable;
  private int maxSize;

  /**
   * Creates a buffer manager having the specified number of buffer slots.
   * This constructor depends on both the {@link FileMgr} and
   * {@link simpledb.log.LogMgr LogMgr} objects that it gets from the class
   * {@link simpledb.server.SimpleDB}. Those objects are created during system
   * initialization. Thus this constructor cannot be called until
   * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or is called
   * first.
   * 
   * @param numbuffs
   *            the number of buffer slots to allocate
   */
  BasicBufferMgr(int numbuffs) {
    // ManualChanges
    numAvailable = numbuffs;
    maxSize = numbuffs;
    bufferPoolMap = new LinkedHashMap<Block, Buffer>();
    // ManualChanges
  }

  /**
   * Flushes the dirty buffers modified by the specified transaction.
   * 
   * @param txnum
   *            the transaction's id number
   */
  synchronized void flushAll(int txnum) {
    for (Buffer buff : bufferPoolMap.values())
      if (buff.isModifiedBy(txnum))
        buff.flush();
  }

  /**
   * Pins a buffer to the specified block. If there is already a buffer
   * assigned to that block then that buffer is used; otherwise, an unpinned
   * buffer from the pool is chosen. Returns a null value if there are no
   * available buffers.
   * 
   * @param blk
   *            a reference to a disk block
   * @return the pinned buffer
   */
  synchronized Buffer pin(Block blk) {
    Buffer buff = findExistingBuffer(blk);
    if (buff == null) {
      buff = chooseUnpinnedBuffer();
      if (buff == null)
        return null;
      buff.assignToBlock(blk);
      bufferPoolMap.put(buff.block(), buff);
    }
    if (!buff.isPinned())
      numAvailable--;
    buff.pin();
    return buff;
  }

  /**
   * Allocates a new block in the specified file, and pins a buffer to it.
   * Returns null (without allocating the block) if there are no available
   * buffers.
   * 
   * @param filename
   *            the name of the file
   * @param fmtr
   *            a pageformatter object, used to format the new block
   * @return the pinned buffer
   */
  synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
    Buffer buff = chooseUnpinnedBuffer();
    if (buff == null)
      return null;
    buff.assignToNew(filename, fmtr);
    numAvailable--;
    buff.pin();
    // Adding Buffer to the map
    bufferPoolMap.put(buff.block(), buff);
    return buff;
  }

  /**
   * Unpins the specified buffer.
   * 
   * @param buff
   *            the buffer to be unpinned
   */
  synchronized void unpin(Buffer buff) {
    buff.unpin();
    if (!buff.isPinned()) {
      numAvailable++;
    }
  }

  /**
   * Returns the number of available (i.e. unpinned) buffers.
   * 
   * @return the number of available buffers
   */
  int available() {
    return numAvailable;
  }

  private Buffer findExistingBuffer(Block blk) {
    if (bufferPoolMap.containsKey(blk)) {
      return bufferPoolMap.get(blk);
    }
    return null;
  }

  private Buffer chooseUnpinnedBuffer() {
    // Adding a new buffer in the map
    if (maxSize > 0) {
      maxSize--;
      return new Buffer();
    }
    // Replacing a buffer using FIFO
    /*if (!bufferpool.entrySet().isEmpty()) {
      Entry<Block, Buffer> buff = bufferpool.entrySet().iterator().next();
      if (!buff.getValue().isPinned()) {
        bufferpool.remove(buff.getKey());
        return buff.getValue();
      }
    }*/
      
    Iterator<Entry<Block, Buffer>> iter = bufferPoolMap.entrySet().iterator();
      while(iter.hasNext()){
    	  Entry<Block, Buffer> buff = iter.next();
    	  if (!buff.getValue().isPinned()){
    		  bufferPoolMap.remove(buff.getKey());
    	      return buff.getValue();
    	  }
      }
      
    
    // Return Null if no empty buffer!
    return null;
  }

  //SGARG7 ADDED METHODS METIONED IN THE PROJECT DOCUMENT START
  /**
   * Determines whether the map has a mapping from the block to some buffer.
   * 
   * @paramblk the block to use as a key
   * @return true if there is a mapping; false otherwise
   */
  boolean containsMapping(Block blk) {
    return bufferPoolMap.containsKey(blk);
  }

  /**
   * Returns the buffer that the map maps the specified block to.
   * 
   * @paramblk the block to use as a key
   * @return the buffer mapped to if there is a mapping; null otherwise
   */
  Buffer getMapping(Block blk) {
    return bufferPoolMap.get(blk);
  }
  
  //SGARG7 ADDED METHODS METIONED IN THE PROJECT DOCUMENT START

}