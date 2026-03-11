#include "BlockBuffer.h"
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <cstdio>

// BlockBuffer(int blockNum)
BlockBuffer :: BlockBuffer(int blockNum){
    // initialise this.blockNum with the argument
    this->blockNum = blockNum;
}

// RecBuffer(int blockNum), calls the parent class constructor
RecBuffer :: RecBuffer(int blockNum) : BlockBuffer :: BlockBuffer(blockNum) {}

/*
    Used to get the header of the block into the location pointed to by 'head'
    NOTE: This function expects the caller to allocate memory for 'head'
*/
int BlockBuffer :: getHeader(struct HeadInfo *head) {

    unsigned char *bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if(ret != SUCCESS){
        return ret;
    }

    // populate the numEntries, numAttrs and numSlots fields in *head
    memcpy(&head->numSlots, bufferPtr + 24, 4);
    memcpy(&head->numEntries, bufferPtr + 16, 4);
    memcpy(&head->numAttrs, bufferPtr + 20, 4);
    memcpy(&head->rblock, bufferPtr + 12, 4);
    memcpy(&head->lblock, bufferPtr + 8, 4);

    return SUCCESS;
}
/*
    Used to get the record at slot 'slotNum' into the array 'rec'
    NOTE: this function expects the caller to allocate memory for 'rec'
*/
int RecBuffer :: getRecord(union Attribute *rec, int slotNum){

    unsigned char *bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if(ret != SUCCESS){
        return ret;
    }

    struct HeadInfo head;

    // get the header using this.getHeader() function
    this->getHeader(&head);

    int attrCount = head.numAttrs;
    int slotCount = head.numSlots;

    /* record at slotNum will be at offset HEADER_SIZE + slotMapSize + (recordSize * slotNum)
        - each record will have size attrCount * ATTR_SIZE
        - slotMap will be of size slotCount
    */
    int recordSize = attrCount * ATTR_SIZE;
    unsigned char *slotPointer = bufferPtr + HEADER_SIZE + head.numSlots + (recordSize * slotNum);

    // load the record into the rec data structure
    memcpy(rec, slotPointer, recordSize);

    return SUCCESS;
}

/*  
    getSlotMap():
    used to get the slotmap from a record block
    NOTE: this functions expects the caller to allocate memory for "*slotmap"
*/
int RecBuffer::getSlotMap(unsigned char* slotMap){
    unsigned char* bufferPtr;

    // get the starting address of the buffer containing the block using loadBlockAndGetBufferPtr()
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if(ret != SUCCESS){
        return ret;
    }

    struct HeadInfo head;
    this->getHeader(&head);
    int slotCount = head.numSlots;

    // get a pointer to the beginning of the slotmap in memory by offsetting HEADER_SIZE
    unsigned char* slotMapInBuffer = bufferPtr + HEADER_SIZE;

    // copy the values from 'slotMapInBuffer' to 'slotMap' (size is 'slotCount')
    memcpy(slotMap, slotMapInBuffer,slotCount);

    return SUCCESS;
}

/*
    Used to load a block to the buffer and get a pointer to it.
    NOTE: this function expects the caller to allocate memory for the argument
*/
int BlockBuffer :: loadBlockAndGetBufferPtr(unsigned char **buffPtr){
    // check whether the block is already present in the buffer using StaticBuffer.getBufferNum();
    int bufferNum = StaticBuffer :: getBufferNum(this->blockNum);

    // if buffer is present
        // set the timeStamp of the corresponding buffer at bufferMetaInfo to 0, rest increment by one
    if(bufferNum != E_BLOCKNOTINBUFFER){
        for(int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY; bufferIndex++){
            if(bufferIndex == bufferNum){
                StaticBuffer::metainfo[bufferIndex].timeStamp = 0;
            }
            else{
                StaticBuffer::metainfo[bufferIndex].timeStamp += 1;
            }
        }
    }
    else{
        bufferNum = StaticBuffer :: getFreeBuffer(this->blockNum);
        if(bufferNum == E_OUTOFBOUND){
            return E_OUTOFBOUND;
        }

        Disk :: readBlock(StaticBuffer :: blocks[bufferNum], this->blockNum);
    }

    // store the pointer to this buffer (blocks[bufferNum]) in *buffPtr
    *buffPtr = StaticBuffer :: blocks[bufferNum];

    return SUCCESS;
}


// compareAttrs()
int compareAttrs(union Attribute attr1, union Attribute attr2, int attrType){
    double diff;
    if(attrType == STRING){
        diff = strcmp(attr1.sVal, attr2.sVal);
    }
    else{
        diff = attr1.nVal - attr2.nVal;
    }

    if(diff > 0){
        return 1;
    }
    else if(diff < 0){
        return -1;
    }
    else{
        return 0;
    }
}

/*
    RecBuffer::setRecord()
    Sets the slotNum th record entry of the block with the input record content
*/
int RecBuffer::setRecord(union Attribute *rec, int slotNum){
    unsigned char *bufferPtr;
    /*
        get the starting address of the buffer containing the block
        using loadBlockAndGetBufferPtr(&bufferPtr);
    */
    int res = BlockBuffer::loadBlockAndGetBufferPtr(&bufferPtr);

    if(res != SUCCESS){
        return res;
    }

    /* get the header of the block using the getHeader() */
    struct HeadInfo head;
    this->getHeader(&head);

    // get the number of attributes
    int attrCount = head.numAttrs;

    // get the number of slots in the block
    int slotCount = head.numSlots;

    // if input slotNum is not in the permitted range return E_OUTOFBOUND
    if(slotNum >= slotCount){
        return E_OUTOFBOUND;
    }

    /*
        Offset bufferPtr to point to the beginning of the record at required slot.
        The block contains the header, the slotmap, followed by all the records.
    */
    int recordSize = attrCount * ATTR_SIZE;
    unsigned char *slotPointer = bufferPtr + HEADER_SIZE + slotCount + (slotNum * recordSize);

    // copy the record from rec to the buffer using memcpy
    memcpy(slotPointer, rec, recordSize);

    // update the dirty bit using setDirtyBit()
    StaticBuffer::setDirtyBit(this->blockNum);

    return SUCCESS;
}

int BlockBuffer::setHeader(struct HeadInfo *head){

    unsigned char *bufferPtr;
    // get the starting address of the buffer containing the block using
    int ret=loadBlockAndGetBufferPtr(&bufferPtr);
    if(ret!=SUCCESS)return ret;

    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
        // return the value returned by the call.

    // cast bufferPtr to type HeadInfo*
    struct HeadInfo *bufferHeader = (struct HeadInfo *)bufferPtr;

    bufferHeader->blockType=head->blockType;
    bufferHeader->lblock=head->lblock;
    bufferHeader->numAttrs=head->numAttrs;
    bufferHeader->numEntries=head->numEntries;
    bufferHeader->numSlots=head->numSlots;
    bufferHeader->pblock=head->pblock;
    bufferHeader->rblock=head->rblock;
    // copy the fields of the HeadInfo pointed to by head (except reserved) to
    // the header of the block (pointed to by bufferHeader)
    //(hint: bufferHeader->numSlots = head->numSlots )

    ret=StaticBuffer::setDirtyBit(this->blockNum);
    if(ret!=SUCCESS)return ret;
    // update dirty bit by calling StaticBuffer::setDirtyBit()
    // if setDirtyBit() failed, return the error code

    return SUCCESS;
}

int BlockBuffer::setBlockType(int blockType){

    unsigned char *bufferPtr;
    int ret=loadBlockAndGetBufferPtr(&bufferPtr);
    if(ret!=SUCCESS)return ret;

    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
        // return the value returned by the call.

    // store the input block type in the first 4 bytes of the buffer.
    // (hint: cast bufferPtr to int32_t* and then assign it)
      *((int32_t *)bufferPtr) = blockType;

    // update the StaticBuffer::blockAllocMap entry corresponding to the
    // object's block number to `blockType`.
    StaticBuffer::blockAllocMap[this->blockNum] = (unsigned char)blockType;
    ret= StaticBuffer::setDirtyBit(this->blockNum);
    if(ret!=SUCCESS)return ret;
    return SUCCESS;
}

int BlockBuffer::getFreeBlock(int blockType){

    // iterate through the StaticBuffer::blockAllocMap and find the block number
    // of a free block in the disk.
    int freeblock=-1;
    for(int i=0;i<DISK_BLOCKS;i++){
        if(StaticBuffer::blockAllocMap[i]==UNUSED_BLK){
            freeblock=i;
            break;
        }
    }
    // if no block is free, return E_DISKFULL.

    if(freeblock==-1){
        return E_DISKFULL;
    }
    this->blockNum=freeblock;
    // set the object's blockNum to the block number of the free block.

    // find a free buffer using StaticBuffer::getFreeBuffer() .
    int buffernum=StaticBuffer::getFreeBuffer(freeblock);
    if(buffernum<0)return buffernum;
    struct HeadInfo head={};
    head.pblock=-1;
    head.lblock=-1;
    head.rblock=-1;
    head.numEntries=0;
    head.numAttrs=0;
    head.numSlots=0;
    int ret=this->setHeader(&head);
    if(ret!=SUCCESS)return ret;

    ret=this->setBlockType(blockType);
    if(ret!=SUCCESS)return ret;
    return freeblock;
    // initialize the header of the block passing a struct HeadInfo with values
    // pblock: -1, lblock: -1, rblock: -1, numEntries: 0, numAttrs: 0, numSlots: 0
    // to the setHeader() function.

    // update the block type of the block to the input block type using setBlockType().

    // return block number of the free block.
}
BlockBuffer::BlockBuffer(char blockType){ //constructor 1
    int intblocktype;
    if(blockType=='R'){
        intblocktype=REC;
    }
    else if(blockType=='I'){
        intblocktype=IND_INTERNAL;
    }else{

        intblocktype=IND_LEAF;
    }
    int ret = this->getFreeBlock(intblocktype);
    this->blockNum = ret;
}
RecBuffer::RecBuffer() : BlockBuffer('R'){}
// call parent non-default constructor with 'R' denoting record block.

int RecBuffer::setSlotMap(unsigned char *slotMap) {
    unsigned char *bufferPtr;
    int ret=BlockBuffer::loadBlockAndGetBufferPtr(&bufferPtr);
    if(ret!=SUCCESS){
        return ret;
    }// if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
        // return the value returned by the call.
    struct HeadInfo head;
    this->getHeader(&head);
    // get the header of the block using the getHeader() function

    int numSlots = head.numSlots;/* the number of slots in the block */;

    // the slotmap starts at bufferPtr + HEADER_SIZE. Copy the contents of the
    // argument `slotMap` to the buffer replacing the existing slotmap.
    // Note that size of slotmap is `numSlots`
    memcpy(bufferPtr + HEADER_SIZE,slotMap,numSlots);
    ret=StaticBuffer::setDirtyBit(this->blockNum);
    if(ret!=SUCCESS)return ret;
    // update dirty bit using StaticBuffer::setDirtyBit
    // if setDirtyBit failed, return the value returned by the call

    return SUCCESS;
}
int BlockBuffer::getBlockNum(){

    return this->blockNum;
}
void BlockBuffer::releaseBlock(){

    // if blockNum is INVALID_BLOCKNUM (-1), or it is invalidated already, do nothing
    if(this->blockNum==INVALID_BLOCKNUM){
        return;
    }
    /* get the buffer number of the buffer assigned to the block
    using StaticBuffer::getBufferNum().
    (this function return E_BLOCKNOTINBUFFER if the block is not
    currently loaded in the buffer)
    */

    int buffernum = StaticBuffer::getBufferNum(this->blockNum);
    if(buffernum != E_BLOCKNOTINBUFFER){
        StaticBuffer::metainfo[buffernum].free=true;
    }
    // if the block is present in the buffer, free the buffer
    // by setting the free flag of its StaticBuffer::tableMetaInfo entry
    // to true.

    // free the block in disk by setting the data type of the entry
    // corresponding to the block number in StaticBuffer::blockAllocMap
    // to UNUSED_BLK.
    StaticBuffer::blockAllocMap[blockNum]=UNUSED_BLK;
    blockNum=INVALID_BLOCKNUM;
    // set the object's blockNum to INVALID_BLOCK (-1)
    
}