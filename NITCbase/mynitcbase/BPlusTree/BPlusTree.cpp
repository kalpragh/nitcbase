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
                if ((compareval >= 0 && (op == EQ || op == GE)) || (compareval > 0 && op == GT))
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
    ret = insertIntoLeaf(relId, attrName, leafBlkNum, indexentry);
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

int BPlusTree::insertIntoLeaf(int relId, char attrName[ATTR_SIZE], int leafBlockNum, Index indexEntry)
{
    AttrCatEntry attrCatEntryBuffer;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntryBuffer);

    IndLeaf leafBlock(leafBlockNum);

    HeadInfo blockHeader;
    leafBlock.getHeader(&blockHeader);

    Index indices[blockHeader.numEntries + 1];

    bool inserted = false;
    for (int entryindex = 0; entryindex < blockHeader.numEntries; entryindex++)
    {
        Index entry;
        leafBlock.getEntry(&entry, entryindex);

        if (compareAttrs(entry.attrVal, indexEntry.attrVal, attrCatEntryBuffer.attrType) <= 0)
        {
            indices[entryindex] = entry;
        }
        else
        {
            indices[entryindex] = indexEntry;
            inserted = true;

            for (entryindex++; entryindex <= blockHeader.numEntries; entryindex++)
            {
                leafBlock.getEntry(&entry, entryindex - 1);
                indices[entryindex] = entry;
            }
            break;
        }
    }

    if (!inserted)
        indices[blockHeader.numEntries] = indexEntry;

    if (blockHeader.numEntries < MAX_KEYS_LEAF)
    {
        blockHeader.numEntries++;
        leafBlock.setHeader(&blockHeader);

        for (int indicesIt = 0; indicesIt < blockHeader.numEntries; indicesIt++)
            leafBlock.setEntry(&indices[indicesIt], indicesIt);

        return SUCCESS;
    }

    int newRightBlk = splitLeaf(leafBlockNum, indices);

    if (newRightBlk == E_DISKFULL)
        return E_DISKFULL;

    if (blockHeader.pblock != -1)
    {
        InternalEntry middleEntry;
        middleEntry.attrVal = indices[MIDDLE_INDEX_LEAF].attrVal,
        middleEntry.lChild = leafBlockNum, middleEntry.rChild = newRightBlk;

        return insertIntoInternal(relId, attrName, blockHeader.pblock, middleEntry);
    }
    else
    {
        if (
            createNewRoot(relId, attrName, indices[MIDDLE_INDEX_LEAF].attrVal,
                          leafBlockNum, newRightBlk) == E_DISKFULL)
            return E_DISKFULL;
    }

    return SUCCESS;
}

int BPlusTree::splitLeaf(int leafBlockNum, Index indices[])
{
    // declare rightBlk, an instance of IndLeaf using constructor 1 to obtain new
    // leaf index block that will be used as the right block in the splitting

    IndLeaf rightBlk;
    IndLeaf leftBlk(leafBlockNum);
    // declare leftBlk, an instance of IndLeaf using constructor 2 to read from
    // the existing leaf block

    int rightBlkNum = rightBlk.getBlockNum();
    int leftBlkNum = leftBlk.getBlockNum();

    if (rightBlkNum == E_DISKFULL)
    {
        return E_DISKFULL;
    }

    HeadInfo leftBlkHeader, rightBlkHeader;
    leftBlk.getHeader(&leftBlkHeader);
    rightBlk.getHeader(&rightBlkHeader);

    rightBlkHeader.numEntries = (MAX_KEYS_LEAF + 1) / 2;
    rightBlkHeader.pblock = leftBlkHeader.pblock;
    rightBlkHeader.lblock = leftBlkNum;
    rightBlkHeader.rblock = leftBlkHeader.rblock;
    rightBlk.setHeader(&rightBlkHeader);

    leftBlkHeader.numEntries = (MAX_KEYS_LEAF + 1) / 2;
    leftBlkHeader.rblock = rightBlkNum;
    leftBlk.setHeader(&leftBlkHeader);

    // set the first 32 entries of leftBlk = the first 32 entries of indices array
    // and set the first 32 entries of newRightBlk = the next 32 entries of
    // indices array using IndLeaf::setEntry().

    for (int entryindex = 0; entryindex <= MIDDLE_INDEX_LEAF; entryindex++)
    {
        leftBlk.setEntry(&indices[entryindex], entryindex);
        rightBlk.setEntry(&indices[entryindex + MIDDLE_INDEX_LEAF + 1], entryindex);
    }
    return rightBlkNum;
}

int BPlusTree::insertIntoInternal(int relId, char attrName[ATTR_SIZE], int intBlockNum, InternalEntry intEntry)
{

    AttrCatEntry attrCatEntryBuffer;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntryBuffer);

    IndInternal internalBlock(intBlockNum);

    HeadInfo blockHeader;
    internalBlock.getHeader(&blockHeader);

    InternalEntry internalEntries[blockHeader.numEntries + 1];

    int insertedIndex = -1;
    for (int entryindex = 0; entryindex < blockHeader.numEntries; entryindex++)
    {
        InternalEntry internalBlockEntry;
        internalBlock.getEntry(&internalBlockEntry, entryindex);

        if (compareAttrs(internalBlockEntry.attrVal, intEntry.attrVal, attrCatEntryBuffer.attrType) <= 0)
        {
            internalEntries[entryindex] = internalBlockEntry;
        }
        else
        {
            internalEntries[entryindex] = intEntry;
            insertedIndex = entryindex;

            for (entryindex++; entryindex <= blockHeader.numEntries; entryindex++)
            {
                internalBlock.getEntry(&internalBlockEntry, entryindex - 1);
                internalEntries[entryindex] = internalBlockEntry;
            }

            break;
        }
    }

    if (insertedIndex == -1)
    {
        internalEntries[blockHeader.numEntries] = intEntry;
        insertedIndex = blockHeader.numEntries;
    }

    if (insertedIndex > 0)
    {
        internalEntries[insertedIndex - 1].rChild = intEntry.lChild;
    }

    if (insertedIndex < blockHeader.numEntries)
    {
        internalEntries[insertedIndex + 1].lChild = intEntry.rChild;
    }

    if (blockHeader.numEntries < MAX_KEYS_INTERNAL)
    {
        blockHeader.numEntries++;
        internalBlock.setHeader(&blockHeader);

        for (int entryindex = 0; entryindex < blockHeader.numEntries; entryindex++)
            internalBlock.setEntry(&internalEntries[entryindex], entryindex);

        return SUCCESS;
    }

    int newRightBlk = splitInternal(intBlockNum, internalEntries);

    if (newRightBlk == E_DISKFULL)
    {
        BPlusTree::bPlusDestroy(intEntry.rChild);

        return E_DISKFULL;
    }

    if (blockHeader.pblock != -1)
    {
        InternalEntry middleEntry;
        middleEntry.lChild = intBlockNum, middleEntry.rChild = newRightBlk;

        middleEntry.attrVal = internalEntries[MIDDLE_INDEX_INTERNAL].attrVal;

        return insertIntoInternal(relId, attrName, blockHeader.pblock, middleEntry);
    }
    else
    {

        return createNewRoot(relId, attrName, internalEntries[MIDDLE_INDEX_INTERNAL].attrVal, intBlockNum, newRightBlk);
    }
    return SUCCESS;
}

int BPlusTree::splitInternal(int intBlockNum, InternalEntry internalEntries[])
{
    IndInternal rightblock;
    IndInternal leftblock(intBlockNum);
    int rightBlkNum = rightblock.getBlockNum();
    int leftBlkNum = leftblock.getBlockNum();

    if (rightBlkNum == E_DISKFULL)
    {
        return E_DISKFULL;
    }

    HeadInfo leftBlkHeader, rightBlkHeader;
    // get the headers of left block and right block using BlockBuffer::getHeader()

    leftblock.getHeader(&leftBlkHeader);
    rightblock.getHeader(&rightBlkHeader);

    rightBlkHeader.numEntries = MAX_KEYS_INTERNAL / 2;
    rightBlkHeader.pblock = leftBlkHeader.pblock;
    rightblock.setHeader(&rightBlkHeader);
    // set leftBlkHeader with the following values
    leftBlkHeader.numEntries = (MAX_KEYS_INTERNAL) / 2;
    leftBlkHeader.rblock = rightBlkNum;
    leftblock.setHeader(&leftBlkHeader);
    // and update the header using BlockBuffer::setHeader()

    for (int entryindex = 0; entryindex < MIDDLE_INDEX_INTERNAL; entryindex++)
    {
        leftblock.setEntry(&internalEntries[entryindex], entryindex);
        rightblock.setEntry(&internalEntries[entryindex + MIDDLE_INDEX_INTERNAL + 1], entryindex);
    }

    int type = StaticBuffer::getStaticBlockType(internalEntries[0].lChild);

    BlockBuffer blockbuffer(internalEntries[MIDDLE_INDEX_INTERNAL + 1].lChild);

    HeadInfo blockHeader;
    blockbuffer.getHeader(&blockHeader);

    blockHeader.pblock = rightBlkNum;
    blockbuffer.setHeader(&blockHeader);

    for (int entryindex = 0; entryindex < MIDDLE_INDEX_INTERNAL; entryindex++)
    {
        // declare an instance of BlockBuffer to access the child block using
        // constructor 2
        BlockBuffer blockbuffer(internalEntries[entryindex + MIDDLE_INDEX_INTERNAL + 1].rChild);

        // update pblock of the block to rightBlkNum using BlockBuffer::getHeader()
        // and BlockBuffer::setHeader().
        // HeadInfo blockHeader;
        blockbuffer.getHeader(&blockHeader);

        blockHeader.pblock = rightBlkNum;
        blockbuffer.setHeader(&blockHeader);
    }

    return rightBlkNum;
}
int BPlusTree::createNewRoot(int relId, char attrName[ATTR_SIZE], Attribute attrVal, int lChild, int rChild)
{
    // get the attribute cache entry corresponding to attrName
    // using AttrCacheTable::getAttrCatEntry().
    AttrCatEntry attrcatentrybuf;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrcatentrybuf);

    // declare newRootBlk, an instance of IndInternal using appropriate constructor
    IndInternal newRootBlk;
    // to allocate a new internal index block on the disk

    int newRootBlkNum = newRootBlk.getBlockNum();

    if (newRootBlkNum == E_DISKFULL)
    {
        // (failed to obtain an empty internal index block because the disk is full)

        // Using bPlusDestroy(), destroy the right subtree, rooted at rChild.
        // This corresponds to the tree built up till now that has not yet been
        // connected to the existing B+ Tree

        BPlusTree::bPlusDestroy(rChild);
        return E_DISKFULL;
    }

    // update the header of the new block with numEntries = 1 using
    // BlockBuffer::getHeader() and BlockBuffer::setHeader()

    HeadInfo blockHeader;
    newRootBlk.getHeader(&blockHeader);
    blockHeader.numEntries = 1;
    newRootBlk.setHeader(&blockHeader);
    // create a struct InternalEntry with lChild, attrVal and rChild from the
    // arguments and set it as the first entry in newRootBlk using IndInternal::setEntry()

    InternalEntry internalentry;
    internalentry.lChild = lChild;
    internalentry.rChild = rChild;
    internalentry.attrVal = attrVal;

    newRootBlk.setEntry(&internalentry, 0);
    // declare BlockBuffer instances for the `lChild` and `rChild` blocks using
    // appropriate constructor and update the pblock of those blocks to `newRootBlkNum`
    // using BlockBuffer::getHeader() and BlockBuffer::setHeader()

    BlockBuffer leftchildblock(lChild);
    BlockBuffer rightchildblock(rChild);

    HeadInfo leftchildheader, rightchildheader;
    leftchildblock.getHeader(&leftchildheader);
    rightchildblock.getHeader(&rightchildheader);

    leftchildheader.pblock = newRootBlkNum;
    rightchildheader.pblock = newRootBlkNum;

    leftchildblock.setHeader(&leftchildheader);
    rightchildblock.setHeader(&rightchildheader);
    // update rootBlock = newRootBlkNum for the entry corresponding to `attrName`
    // in the attribute cache using AttrCacheTable::setAttrCatEntry().

    attrcatentrybuf.rootBlock = newRootBlkNum;
    AttrCacheTable::setAttrCatEntry(relId, attrName, &attrcatentrybuf);
    return SUCCESS;
}