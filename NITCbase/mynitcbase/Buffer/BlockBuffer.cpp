#include "BlockBuffer.h"

#include <cstdlib>
#include <cstring>
// the declarations for these functions can be found in "BlockBuffer.h"

BlockBuffer::BlockBuffer(int blockNum) {
  // initialise this.blockNum with the argument
  this->blockNum = blockNum;
}

// calls the parent class constructor
RecBuffer::RecBuffer(int blockNum) : BlockBuffer::BlockBuffer(blockNum) {}

// load the block header into the argument pointer
int BlockBuffer::getHeader(struct HeadInfo *head) {
  //first 3 lines-new pt of stage 3
  unsigned char *bufferPtr = nullptr;

  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) return ret;

  // populate the numEntries, numAttrs and numSlots fields in *head
  memcpy(&head->lblock,     bufferPtr + 8,  4);
  memcpy(&head->rblock,     bufferPtr + 12, 4);
  memcpy(&head->numEntries, bufferPtr + 16, 4);
  memcpy(&head->numAttrs,   bufferPtr + 20, 4);
  memcpy(&head->numSlots,   bufferPtr + 24, 4);

  return SUCCESS;
}

// load the record at slotNum into the argument pointer
int RecBuffer::getRecord(union Attribute *rec, int slotNum) {
  struct HeadInfo head;

  int ret = this->getHeader(&head);
  if (ret != SUCCESS) return ret;

  int attrCount = head.numAttrs;
  int slotCount = head.numSlots;

  unsigned char *bufferPtr = nullptr;
  ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) return ret;

  int recordSize  = attrCount * ATTR_SIZE;
  int slotMapSize = slotCount;

  unsigned char *slotPointer =bufferPtr + HEADER_SIZE + slotMapSize + (recordSize * slotNum);

  memcpy(rec, slotPointer, recordSize);
  return SUCCESS;
}

int RecBuffer::setRecord(union Attribute *rec, int slotNum) {
  unsigned char *bufferptr;

  int ret=this->loadBlockAndGetBufferPtr(&bufferptr);
  if(ret!=SUCCESS){
    return ret;
  }
  struct HeadInfo head;
  this->getHeader(&head);
  int numattrs=head.numAttrs;
  int numslots=head.numSlots;

  if(slotNum<0 || slotNum>=numslots){
    return E_OUTOFBOUND;
  }

  int recordSize  = numattrs * ATTR_SIZE;
  int slotMapSize = numslots;

  unsigned char *slotPointer = bufferptr + HEADER_SIZE + slotMapSize + (recordSize * slotNum);

  memcpy(slotPointer, rec, recordSize);

  ret=StaticBuffer::setDirtyBit(this->blockNum);
  if(ret!=SUCCESS){
    return ret;
  }

  return SUCCESS;
}

//new fn in stage 3
int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char **buffPtr) {
  // check whether the block is already present in the buffer
  int bufferNum = StaticBuffer::getBufferNum(this->blockNum);

  if (bufferNum!= E_BLOCKNOTINBUFFER) {
    // set the timestamp of the corresponding buffer to 0 and increment the
        // timestamps of all other occupied buffers in BufferMetaInfo.
        for(int bufferindex=0;bufferindex<BUFFER_CAPACITY;bufferindex++){
          if(bufferindex==bufferNum){
            StaticBuffer::metainfo[bufferindex].timeStamp=0;
          }
          else{
            StaticBuffer::metainfo[bufferindex].timeStamp++;
          }
        }
  }
  else{
    bufferNum=StaticBuffer::getFreeBuffer(this->blockNum);
    if(bufferNum==E_OUTOFBOUND){
      return E_OUTOFBOUND;
    }
    Disk::readBlock(StaticBuffer::blocks[bufferNum],this->blockNum);
  }
  *buffPtr = StaticBuffer :: blocks[bufferNum];
  return SUCCESS;
}
/* used to get the slotmap from a record block
NOTE: this function expects the caller to allocate memory for `*slotMap`
*/ 
//stage 4
int RecBuffer::getSlotMap(unsigned char *slotMap) {
  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) {
    return ret;
  }
  struct HeadInfo head;
  ret=getHeader(&head);
  if(ret!=SUCCESS){
    return ret;
  }
  int slotCount = head.numSlots;
  unsigned char *slotMapInBuffer = bufferPtr + HEADER_SIZE;

  memcpy(slotMap,slotMapInBuffer,slotCount);

  return SUCCESS;
}
int compareAttrs(union Attribute attr1, union Attribute attr2, int attrType) {
    if (attrType == STRING) {
        int diff = strcmp(attr1.sVal, attr2.sVal);

        if (diff > 0) return 1;
        if (diff < 0) return -1;
        return 0;
    }
    else {
        double diff = attr1.nVal - attr2.nVal;

        if (diff > 0) return 1;
        if (diff < 0) return -1;
        return 0;
    }
}
