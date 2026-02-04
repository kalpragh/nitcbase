#include "StaticBuffer.h"
unsigned char StaticBuffer::blocks[BUFFER_CAPACITY][BLOCK_SIZE];
struct BufferMetaInfo StaticBuffer::metainfo[BUFFER_CAPACITY];

StaticBuffer::StaticBuffer() {
  for (int bufferIndex = 0; bufferIndex<BUFFER_CAPACITY;bufferIndex++) {
    metainfo[bufferIndex].free = true;
    metainfo[bufferIndex].dirty=false;
    metainfo[bufferIndex].timeStamp=-1;
    metainfo[bufferIndex].blockNum = -1;
  }
}

StaticBuffer::~StaticBuffer() {
  for (int bufferIndex = 0; bufferIndex<BUFFER_CAPACITY;bufferIndex++) {
    if(metainfo[bufferIndex].free == false && metainfo[bufferIndex].dirty==true){
      Disk::writeBlock(StaticBuffer:: blocks[bufferIndex],metainfo[bufferIndex].blockNum);
    }
  }
}

int StaticBuffer::getFreeBuffer(int blockNum) {
  if (blockNum < 0 || blockNum > DISK_BLOCKS) {
    return E_OUTOFBOUND;
  }
  int allocatedBuffer;

  for(int i=0;i<BUFFER_CAPACITY;i++){
    if(metainfo[i].free){
        allocatedBuffer=i;
        break;
    }
  }

  metainfo[allocatedBuffer].free = false;
  metainfo[allocatedBuffer].blockNum = blockNum;

  return allocatedBuffer;
}

int StaticBuffer::getBufferNum(int blockNum) {
    if(blockNum<0 || blockNum>=DISK_BLOCKS){
        return E_OUTOFBOUND;
    }
    
   for (int i = 0; i < BUFFER_CAPACITY; i++) {
    if (!metainfo[i].free && metainfo[i].blockNum == blockNum) {
      return i;
    }
   } 
  return E_BLOCKNOTINBUFFER;
}