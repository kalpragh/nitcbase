#include "BPlusTree.h"

#include <cstring>
extern int comparisoncount;

RecId BPlusTree::bPlusSearch(int relId, char attrName[ATTR_SIZE], Attribute attrVal, int op)
{
    // declare searchIndex which will be used to store search index for attrName.
    IndexId searchIndex;
    AttrCacheTable::getSearchIndex(relId, attrName, &searchIndex);

    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
    int block, index;

    if (searchIndex.block == -1 && searchIndex.index == -1)
    {
        // start the search from the first entry of root.
        block = attrCatEntry.rootBlock;
        index = 0;

        if (block == -1)
        {
            return RecId{-1, -1};
        }
    }
    else
    {
        /*a valid searchIndex points to an entry in the leaf index of the attribute's
        B+ Tree which had previously satisfied the op for the given attrVal.*/
        block = searchIndex.block;
        index = searchIndex.index + 1; // search is resumed from the next index.

        IndLeaf leaf(block); // load block into leaf using IndLeaf::IndLeaf().
        HeadInfo leafHead;
        leaf.getHeader(&leafHead);

        if (index >= leafHead.numEntries)
        {
            /* (all the entries in the block has been searched; search from the
            beginning of the next leaf index block. */
            block = leafHead.rblock;
            index = 0; // chk
            if (block == -1)
            {
                return RecId{-1, -1};
            }
        }
    }

    /******  Traverse through all the internal nodes according to value
             of attrVal and the operator op                             ******/

    /* (This section is only needed when
        - search restarts from the root block (when searchIndex is reset by caller)
        - root is not a leaf
        If there was a valid search index, then we are already at a leaf block
        and the test condition in the following loop will fail)
    */

    while (StaticBuffer::getStaticBlockType(block) == IND_INTERNAL)
    { // use StaticBuffer::getStaticBlockType()

        // load the block into internalBlk using IndInternal::IndInternal().
        IndInternal internalBlk(block);
        HeadInfo intHead;

        // load the header of internalBlk into intHead using BlockBuffer::getHeader()
        internalBlk.getHeader(&intHead);
        // declare intEntry which will be used to store an entry of internalBlk.
        InternalEntry intEntry;

        if (op == NE || op == LT || op == LE)
        {
            /*
            - NE: need to search the entire linked list of leaf indices of the B+ Tree,
            starting from the leftmost leaf index. Thus, always move to the left.

            - LT and LE: the attribute values are arranged in ascending order in the
            leaf indices of the B+ Tree. Values that satisfy these conditions, if
            any exist, will always be found in the left-most leaf index. Thus,
            always move to the left.
            */

            // load entry in the first slot of the block into intEntry
            // using
            internalBlk.getEntry(&intEntry, 0);
            block = intEntry.lChild;
        }
        else
        {
            /*
            - EQ, GT and GE: move to the left child of the first entry that is
            greater than (or equal to) attrVal
            (we are trying to find the first entry that satisfies the condition.
            since the values are in ascending order we move to the left child which
            might contain more entries that satisfy the condition)
            */

            /*
             traverse through all entries of internalBlk and find an entry that
             satisfies the condition.
             if op == EQ or GE, then intEntry.attrVal >= attrVal
             if op == GT, then intEntry.attrVal > attrVal
             Hint: the helper function compareAttrs() can be used for comparing
            */

            bool found = false;
            for (int i = 0; i < intHead.numEntries; i++)
            {
                internalBlk.getEntry(&intEntry, i);
                comparisoncount++;
                int compareval = compareAttrs(intEntry.attrVal, attrVal, attrCatEntry.attrType);
                if ((compareval >= 0 && (op == EQ || op == GE)) || (compareval > 0 || op == GT))
                { // for 2nd half does it hv to be (compareval > 0 && op == GT)
                    found = true;
                    break;
                }
            }
            if (found)
            {
                // move to the left child of that entry
                block = intEntry.lChild;
            }
            else
            {
                // move to the right child of the last entry of the block
                // i.e numEntries - 1 th entry of the block

                block = intEntry.rChild;
            }
        }
    }

    // NOTE: `block` now has the block number of a leaf index block.

    /******  Identify the first leaf index entry from the current position
                that satisfies our condition (moving right)             ******/

    while (block != -1)
    {
        // load the block into leafBlk using IndLeaf::IndLeaf().
        IndLeaf leafBlk(block);
        HeadInfo leafHead;

        // load the header to leafHead using BlockBuffer::getHeader().
        leafBlk.getHeader(&leafHead);
        // declare leafEntry which will be used to store an entry from leafBlk
        Index leafEntry;

        while (index < leafHead.numEntries)
        {

            // load entry corresponding to block and index into leafEntry
            // using IndLeaf::getEntry().
            leafBlk.getEntry(&leafEntry, index);
            comparisoncount++;
            int cmpVal = compareAttrs(leafEntry.attrVal, attrVal, attrCatEntry.attrType);

            if (
                (op == EQ && cmpVal == 0) ||
                (op == LE && cmpVal <= 0) ||
                (op == LT && cmpVal < 0) ||
                (op == GT && cmpVal > 0) ||
                (op == GE && cmpVal >= 0) ||
                (op == NE && cmpVal != 0))
            {
                searchIndex.block = block;
                searchIndex.index = index;
                AttrCacheTable::setSearchIndex(relId, attrName, &searchIndex);
                return RecId{leafEntry.block, leafEntry.slot};
            }
            else if ((op == EQ || op == LE || op == LT) && cmpVal > 0)
            {
                /*future entries will not satisfy EQ, LE, LT since the values
                    are arranged in ascending order in the leaves */

                return RecId{-1, -1};
            }

            // search next index.
            ++index;
        }

        /*only for NE operation do we have to check the entire linked list;
        for all the other op it is guaranteed that the block being searched
        will have an entry, if it exists, satisying that op. */
        if (op != NE)
        {
            break;
        }

        block = leafHead.rblock;
        index = 0;
    }

    return RecId{-1, -1};
}
int BPlusTree::bPlusCreate(int relId, char attrName[ATTR_SIZE])
{

    if (relId == RELCAT_RELID || relId == ATTRCAT_RELID)
    {
        return E_NOTPERMITTED;
    }

    AttrCatEntry attrcatentrybuffer;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrcatentrybuffer);

    if (ret != SUCCESS)
        return ret;

    if (attrcatentrybuffer.rootBlock != -1)
    {
        return SUCCESS;
    }

    IndLeaf rootBlockBuf;

    int rootBlock = rootBlockBuf.getBlockNum();

    if (rootBlock == E_DISKFULL)
    {
        return E_DISKFULL;
    }
    attrcatentrybuffer.rootBlock = rootBlock;
    AttrCacheTable::setAttrCatEntry(relId, attrName, &attrcatentrybuffer);

    RelCatEntry relCatEntryBuffer;

    RelCacheTable::getRelCatEntry(relId, &relCatEntryBuffer);

    int block = relCatEntryBuffer.firstBlk;

    while (block != -1)
    {

        RecBuffer blockbuffer(block);
        unsigned char slotMap[relCatEntryBuffer.numSlotsPerBlk];

        blockbuffer.getSlotMap(slotMap);

        for (int slot = 0; slot < relCatEntryBuffer.numSlotsPerBlk; slot++)
        {
            if (slotMap[slot] == SLOT_OCCUPIED)
            {
                Attribute record[relCatEntryBuffer.numAttrs];
                blockbuffer.getRecord(record, slot);

                RecId recid = RecId{block, slot};
                ret = bPlusInsert(relId, attrName, record[attrcatentrybuffer.offset], recid);
                if (ret == E_DISKFULL)
                {
                    return E_DISKFULL;
                }
            }
        }

        HeadInfo blockheader;
        blockbuffer.getHeader(&blockheader);
        block = blockheader.rblock;
    }

    return SUCCESS;
}

int BPlusTree::bPlusDestroy(int rootBlockNum)
{
    if (rootBlockNum < 0 || rootBlockNum >= DISK_BLOCKS)
    {
        return E_OUTOFBOUND;
    }

    int type = StaticBuffer::getStaticBlockType(rootBlockNum);

    if (type == IND_LEAF)
    {
        // declare an instance of IndLeaf for rootBlockNum using appropriate
        // constructor
        IndLeaf leafblk(rootBlockNum);

        leafblk.releaseBlock();

        return SUCCESS;
    }
    else if (type == IND_INTERNAL)
    {
        // declare an instance of IndInternal for rootBlockNum using appropriate
        // constructor

        IndInternal intblk(rootBlockNum);
        HeadInfo inthead;
        intblk.getHeader(&inthead);

        InternalEntry blockentry;
        intblk.getEntry(&blockentry, 0);
        BPlusTree::bPlusDestroy(blockentry.lChild);
        for (int entry = 0; entry < inthead.numEntries; entry++)
        {
            intblk.getEntry(&blockentry, entry);
            BPlusTree::bPlusDestroy(blockentry.rChild);
        }
        intblk.releaseBlock();
        return SUCCESS;
    }
    else
    {
        // (block is not an index block.)
        return E_INVALIDBLOCK;
    }
}
int BPlusTree::bPlusInsert(int relId, char attrName[ATTR_SIZE], Attribute attrVal, RecId recId)
{
    // get the attribute cache entry corresponding to attrName
    // using AttrCacheTable::getAttrCatEntry().

    AttrCatEntry attrcatentry;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrcatentry);
    if (ret != SUCCESS)
    {
        return ret;
    }
    int blockNum = attrcatentry.rootBlock;

    if (blockNum == -1)
    {
        return E_NOINDEX;
    }

    int leafBlkNum = findLeafToInsert(blockNum, attrVal, attrcatentry.attrType);

    Index indexentry;
    indexentry.attrVal = attrVal;
    indexentry.block = recId.block;
    indexentry.slot = recId.slot;
    int ret = insertIntoLeaf(relId, attrName, leafBlkNum, indexentry);
    if (ret == E_DISKFULL)
    {
        BPlusTree::bPlusDestroy(blockNum);

        attrcatentry.rootBlock = -1;
        AttrCacheTable::setAttrCatEntry(relId, attrName, &attrcatentry);

        return E_DISKFULL;
    }

    return SUCCESS;
}
int BPlusTree::findLeafToInsert(int rootBlock, Attribute attrVal, int attrType)
{
    int blockNum = rootBlock;
    while (StaticBuffer::getStaticBlockType(blockNum) != IND_LEAF)
    { // use StaticBuffer::getStaticBlockType()
        IndInternal internalblk(blockNum);
        HeadInfo blockheader;
        internalblk.getHeader(&blockheader);
        int index = 0;
        while (index < blockheader.numEntries)
        {
            InternalEntry entry;
            internalblk.getEntry(&entry, index);
            if (compareAttrs(attrVal, entry.attrVal, attrType) <= 0)
            {
                break;
            }
            index++;
        }

        if (index == blockheader.numEntries)
        {
            InternalEntry entry;
            internalblk.getEntry(&entry, blockheader.numEntries - 1);
            blockNum = entry.rChild;
            // (i.e. rightmost child of the block)
        }
        else
        {
            InternalEntry entry;
            internalblk.getEntry(&entry, index);
            blockNum = entry.lChild;
        }
    }

    return blockNum;
}
int BPlusTree::insertIntoLeaf(int relId, char attrName[ATTR_SIZE], int blockNum, Index indexEntry)
{
    // get the attribute cache entry corresponding to attrName
    // using AttrCacheTable::getAttrCatEntry().

    // declare an IndLeaf instance for the block using appropriate constructor

    HeadInfo blockHeader;
    // store the header of the leaf index block into blockHeader
    // using BlockBuffer::getHeader()

    // the following variable will be used to store a list of index entries with
    // existing indices + the new index to insert
    Index indices[blockHeader.numEntries + 1];

    /*
    Iterate through all the entries in the block and copy them to the array indices.
    Also insert `indexEntry` at appropriate position in the indices array maintaining
    the ascending order.
    - use IndLeaf::getEntry() to get the entry
    - use compareAttrs() declared in BlockBuffer.h to compare two Attribute structs
    */

    if (blockHeader.numEntries != MAX_KEYS_LEAF)
    {
        // (leaf block has not reached max limit)

        // increment blockHeader.numEntries and update the header of block
        // using BlockBuffer::setHeader().

        // iterate through all the entries of the array `indices` and populate the
        // entries of block with them using IndLeaf::setEntry().

        return SUCCESS;
    }

    // If we reached here, the `indices` array has more than entries than can fit
    // in a single leaf index block. Therefore, we will need to split the entries
    // in `indices` between two leaf blocks. We do this using the splitLeaf() function.
    // This function will return the blockNum of the newly allocated block or
    // E_DISKFULL if there are no more blocks to be allocated.

    int newRightBlk = splitLeaf(blockNum, indices);

    // if splitLeaf() returned E_DISKFULL
    //     return E_DISKFULL

    if (/* the current leaf block was not the root */)
    { // check pblock in header
      // insert the middle value from `indices` into the parent block using the
      // insertIntoInternal() function. (i.e the last value of the left block)

        // the middle value will be at index 31 (given by constant MIDDLE_INDEX_LEAF)

        // create a struct InternalEntry with attrVal = indices[MIDDLE_INDEX_LEAF].attrVal,
        // lChild = currentBlock, rChild = newRightBlk and pass it as argument to
        // the insertIntoInternalFunction as follows

        // insertIntoInternal(relId, attrName, parent of current block, new internal entry)
    }
    else
    {
        // the current block was the root block and is now split. a new internal index
        // block needs to be allocated and made the root of the tree.
        // To do this, call the createNewRoot() function with the following arguments

        // createNewRoot(relId, attrName, indices[MIDDLE_INDEX_LEAF].attrVal,
        //               current block, new right block)
    }

    // if either of the above calls returned an error (E_DISKFULL), then return that
    // else return SUCCESS
}
int BPlusTree::splitLeaf(int leafBlockNum, Index indices[])
{
    // declare rightBlk, an instance of IndLeaf using constructor 1 to obtain new
    // leaf index block that will be used as the right block in the splitting

    // declare leftBlk, an instance of IndLeaf using constructor 2 to read from
    // the existing leaf block

    int rightBlkNum = /* block num of right blk */;
    int leftBlkNum = /* block num of left blk */;

    if (/* newly allocated block has blockNum E_DISKFULL */)
    {
        //(failed to obtain a new leaf index block because the disk is full)
        return E_DISKFULL;
    }

    HeadInfo leftBlkHeader, rightBlkHeader;
    // get the headers of left block and right block using BlockBuffer::getHeader()

    // set rightBlkHeader with the following values
    // - number of entries = (MAX_KEYS_LEAF+1)/2 = 32,
    // - pblock = pblock of leftBlk
    // - lblock = leftBlkNum
    // - rblock = rblock of leftBlk
    // and update the header of rightBlk using BlockBuffer::setHeader()

    // set leftBlkHeader with the following values
    // - number of entries = (MAX_KEYS_LEAF+1)/2 = 32
    // - rblock = rightBlkNum
    // and update the header of leftBlk using BlockBuffer::setHeader() */

    // set the first 32 entries of leftBlk = the first 32 entries of indices array
    // and set the first 32 entries of newRightBlk = the next 32 entries of
    // indices array using IndLeaf::setEntry().

    return rightBlkNum;
}
int BPlusTree::insertIntoInternal(int relId, char attrName[ATTR_SIZE], int intBlockNum, InternalEntry intEntry)
{
    // get the attribute cache entry corresponding to attrName
    // using AttrCacheTable::getAttrCatEntry().

    // declare intBlk, an instance of IndInternal using constructor 2 for the block
    // corresponding to intBlockNum

    HeadInfo blockHeader;
    // load blockHeader with header of intBlk using BlockBuffer::getHeader().

    // declare internalEntries to store all existing entries + the new entry
    InternalEntry internalEntries[blockHeader.numEntries + 1];

    /*
    Iterate through all the entries in the block and copy them to the array
    `internalEntries`. Insert `indexEntry` at appropriate position in the
    array maintaining the ascending order.
        - use IndInternal::getEntry() to get the entry
        - use compareAttrs() to compare two structs of type Attribute

    Update the lChild of the internalEntry immediately following the newly added
    entry to the rChild of the newly added entry.
    */

    if (blockHeader.numEntries != MAX_KEYS_INTERNAL)
    {
        // (internal index block has not reached max limit)

        // increment blockheader.numEntries and update the header of intBlk
        // using BlockBuffer::setHeader().

        // iterate through all entries in internalEntries array and populate the
        // entries of intBlk with them using IndInternal::setEntry().

        return SUCCESS;
    }

    // If we reached here, the `internalEntries` array has more than entries than
    // can fit in a single internal index block. Therefore, we will need to split
    // the entries in `internalEntries` between two internal index blocks. We do
    // this using the splitInternal() function.
    // This function will return the blockNum of the newly allocated block or
    // E_DISKFULL if there are no more blocks to be allocated.

    int newRightBlk = splitInternal(intBlockNum, internalEntries);

    if (/* splitInternal() returned E_DISKFULL */)
    {

        // Using bPlusDestroy(), destroy the right subtree, rooted at intEntry.rChild.
        // This corresponds to the tree built up till now that has not yet been
        // connected to the existing B+ Tree

        return E_DISKFULL;
    }

    if (/* the current block was not the root */)
    { // (check pblock in header)
      // insert the middle value from `internalEntries` into the parent block
      // using the insertIntoInternal() function (recursively).

        // the middle value will be at index 50 (given by constant MIDDLE_INDEX_INTERNAL)

        // create a struct InternalEntry with lChild = current block, rChild = newRightBlk
        // and attrVal = internalEntries[MIDDLE_INDEX_INTERNAL].attrVal
        // and pass it as argument to the insertIntoInternalFunction as follows

        // insertIntoInternal(relId, attrName, parent of current block, new internal entry)
    }
    else
    {
        // the current block was the root block and is now split. a new internal index
        // block needs to be allocated and made the root of the tree.
        // To do this, call the createNewRoot() function with the following arguments

        // createNewRoot(relId, attrName,
        //               internalEntries[MIDDLE_INDEX_INTERNAL].attrVal,
        //               current block, new right block)
    }

    // if either of the above calls returned an error (E_DISKFULL), then return that
    // else return SUCCESS
}
int BPlusTree::splitInternal(int intBlockNum, InternalEntry internalEntries[])
{
    // declare rightBlk, an instance of IndInternal using constructor 1 to obtain new
    // internal index block that will be used as the right block in the splitting

    // declare leftBlk, an instance of IndInternal using constructor 2 to read from
    // the existing internal index block

    int rightBlkNum = /* block num of right blk */;
    int leftBlkNum = /* block num of left blk */;

    if (/* newly allocated block has blockNum E_DISKFULL */)
    {
        //(failed to obtain a new internal index block because the disk is full)
        return E_DISKFULL;
    }

    HeadInfo leftBlkHeader, rightBlkHeader;
    // get the headers of left block and right block using BlockBuffer::getHeader()

    // set rightBlkHeader with the following values
    // - number of entries = (MAX_KEYS_INTERNAL)/2 = 50
    // - pblock = pblock of leftBlk
    // and update the header of rightBlk using BlockBuffer::setHeader()

    // set leftBlkHeader with the following values
    // - number of entries = (MAX_KEYS_INTERNAL)/2 = 50
    // and update the header using BlockBuffer::setHeader()

    /*
    - set the first 50 entries of leftBlk = index 0 to 49 of internalEntries
      array
    - set the first 50 entries of newRightBlk = entries from index 51 to 100
      of internalEntries array using IndInternal::setEntry().
      (index 50 will be moving to the parent internal index block)
    */

    int type = /* block type of a child of any entry of the internalEntries array */;
    //            (use StaticBuffer::getStaticBlockType())

    for (/* each child block of the new right block */)
    {
        // declare an instance of BlockBuffer to access the child block using
        // constructor 2

        // update pblock of the block to rightBlkNum using BlockBuffer::getHeader()
        // and BlockBuffer::setHeader().
    }

    return rightBlkNum;
}
int BPlusTree::createNewRoot(int relId, char attrName[ATTR_SIZE], Attribute attrVal, int lChild, int rChild)
{
    // get the attribute cache entry corresponding to attrName
    // using AttrCacheTable::getAttrCatEntry().

    // declare newRootBlk, an instance of IndInternal using appropriate constructor
    // to allocate a new internal index block on the disk

    int newRootBlkNum = /* block number of newRootBlk */;

    if (newRootBlkNum == E_DISKFULL)
    {
        // (failed to obtain an empty internal index block because the disk is full)

        // Using bPlusDestroy(), destroy the right subtree, rooted at rChild.
        // This corresponds to the tree built up till now that has not yet been
        // connected to the existing B+ Tree

        return E_DISKFULL;
    }

    // update the header of the new block with numEntries = 1 using
    // BlockBuffer::getHeader() and BlockBuffer::setHeader()

    // create a struct InternalEntry with lChild, attrVal and rChild from the
    // arguments and set it as the first entry in newRootBlk using IndInternal::setEntry()

    // declare BlockBuffer instances for the `lChild` and `rChild` blocks using
    // appropriate constructor and update the pblock of those blocks to `newRootBlkNum`
    // using BlockBuffer::getHeader() and BlockBuffer::setHeader()

    // update rootBlock = newRootBlkNum for the entry corresponding to `attrName`
    // in the attribute cache using AttrCacheTable::setAttrCatEntry().

    return SUCCESS;
}