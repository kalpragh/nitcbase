#include "BlockAccess.h"

#include <cstring>


RecId BlockAccess::linearSearch(int relId, char attrName[ATTR_SIZE], union Attribute attrVal, int op){

    // get the previous search index of the relation relId from the relation cache
    RecId prevRecId;
    RelCacheTable::getSearchIndex(relId,&prevRecId);

    // let block and slot denote the record id of the record beign currently checked
    int block, slot;
    // if the current search index record is invalid
    if(prevRecId.block == -1 && prevRecId.slot == -1){
        // it means no hits from the previous search, search should start from the first record itself
        // get the first record block of the relation from the relation cache using getRelCatEntry()
        RelCatEntry relCatEntry;
        RelCacheTable::getRelCatEntry(relId, &relCatEntry);

        // block = first record block of the relation
        // slot = 0
        block = relCatEntry.firstBlk;
        slot = 0;

    }
    else{
        // it means there is a hit from the previous search, should start from the record next to the search index record
        block = prevRecId.block;
        slot = prevRecId.slot + 1;
    }

    /*
        Now the following code will search for the next record in the relation
        that satisfies the given condition
        We will start from the record id (block, slot) and iterate over the
        remaining records of the relation
    */
    while( block != -1){
        // create a RecBuffer object for the block
        RecBuffer recBuffer(block);

        // get the record with id (block,slot) using RecBuffer::getRecord()
        // get header of the block using RecBuffer::getHeader()
        // get slot map of the block using RecBuffer::getSlotMap()
        struct HeadInfo head;
        recBuffer.getHeader(&head);
        Attribute record[head.numAttrs];
        recBuffer.getRecord(record,slot);
        unsigned char slotMap[head.numSlots];
        recBuffer.getSlotMap(slotMap);

        // if slot >= the number of slots per block
        if(slot >= head.numSlots){
            // update block = right block of block
            // update slot = 0
            block = head.rblock;
            slot = 0;
            continue;
        }
        /*
            my notes: if it is end of the record in a relation, this 
            loop will continue to reach the end of the block,
            eventually block = head.rblock = -1 and loop will terminate.
        */

        // if slot is free skip the loop
        // i.e. check if slot'th entry in slot map of block constains SLOT_UNOCCUPIED
        if(slotMap[slot] == SLOT_UNOCCUPIED){
            slot++;
            continue;
        }

        // now compare record's attribute value to the given attrVal as below:
        /*
            firstly get the attribute offset for the attrName attribute
            from the attribute cache entry of the relation using getAttrCatEntry
        */
        /* use the attribute offset to get the value of the attribute from
           current record */
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

        Attribute currentAttrVal = record[attrCatEntry.offset];
        
        // store the difference b/w the attributes.
        int cmpVal = compareAttrs(currentAttrVal, attrVal, attrCatEntry.attrType);

        /* Next task is to check whether this record satisfies the given condition.
           It is determined based on the output of previous comparison and
           the op value received.
           The following code sets the cond variable if the condition is satisfied.
        */
        if(
            (op == NE && cmpVal != 0) ||
            (op == LT && cmpVal < 0) ||
            (op == LE && cmpVal <=0) ||
            (op == EQ && cmpVal == 0) ||
            (op == GT && cmpVal > 0) ||
            (op == GE && cmpVal >= 0)
        ){
            /*
                set the search index in the relation cache as
                the record id of the record that satisfies the given condition
                (use RelCacheTable::setSearchIndex function)
            */
            RecId recid = {block,slot};
            RelCacheTable::setSearchIndex(relId, &recid);

            return recid;
        }

        slot++;
    }

    // no record in the relation with Id relid satisfies the given condition
    return RecId{-1,-1};
}

int BlockAccess::renameRelation(char oldRelName[ATTR_SIZE], char newRelName[ATTR_SIZE]){

    // reset the search index of the relation catalog
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute newRelationName; // set newRelationName with newName
    strcpy(newRelationName.sVal, newRelName);

    // search the relation catalog for an entry with "RelName" = newRelationName
    char relCatAttrRelName[ATTR_SIZE];;
    char attrCatAttrRelName[ATTR_SIZE];
    strcpy(relCatAttrRelName, RELCAT_ATTR_RELNAME);
    strcpy(attrCatAttrRelName, ATTRCAT_ATTR_RELNAME);

    RecId recId = BlockAccess::linearSearch(RELCAT_RELID, relCatAttrRelName, newRelationName, EQ);
    // check if relation already exist
    if(recId.block != -1 && recId.slot != -1){
        return E_RELEXIST;
    }

    // reset the search index of the relation catalog again
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute oldRelationName; // set oldRelationName with oldName
    strcpy(oldRelationName.sVal, oldRelName);

    // search the relation catalog for an entry with "RelName" = oldRelationName
    recId = BlockAccess::linearSearch(RELCAT_RELID, relCatAttrRelName, oldRelationName, EQ);
    // check if relation does not exist
    if(recId.block == -1 && recId.slot == -1){
        return E_RELNOTEXIST;
    }

    /*
        get the relation catalog record of the relation to rename using RecBuffer on
        the relation catalog [RELCAT_BLOCK] and RecBuffer.gerRecord function.
    */
    Attribute record[RELCAT_NO_ATTRS];
    RecBuffer recBuffer(recId.block);
    recBuffer.getRecord(record,recId.slot);

    /*
        update the relation name attribute in the record with newName
        (use RELCAT_REL_NAME_INDEX)
    */
    // then set back the reord value using RecBuffer.setRecord()
    strcpy(record[RELCAT_REL_NAME_INDEX].sVal, newRelName);
    recBuffer.setRecord(record, recId.slot);

    /*
        update all the attribute catalog entries in the attribute catalog corresponding
        to the relation with relation name oldName to the relation name newName.
    */

    //  reset the search index of the attribute catalog
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
            // Relation 'Student' can have 10 attributes, thus Attribute Catalog will have 10 entries correspoding to 'Student'
    int numOfAttrs = record[RELCAT_NO_ATTRIBUTES_INDEX].nVal;
    for(int i = 0; i < numOfAttrs; i++){
        RecId rec_id = BlockAccess::linearSearch(ATTRCAT_RELID, attrCatAttrRelName, oldRelationName, EQ);
        Attribute attrRecord[ATTRCAT_NO_ATTRS];
        RecBuffer attrBuffer(rec_id.block);
        attrBuffer.getRecord(attrRecord, rec_id.slot);

        attrRecord[ATTRCAT_REL_NAME_INDEX] = newRelationName;
        attrBuffer.setRecord(attrRecord, rec_id.slot);
    }

    return SUCCESS;
    
}


int BlockAccess::renameAttribute(char relName[ATTR_SIZE], char oldName[ATTR_SIZE], char newName[ATTR_SIZE]){

    // 1.   first of all we should check whether the given relation exist or not

    /* reset the searchIndex of the relation catalog*/
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute relNameAttr; // set relNameAtrr to relName
    strcpy(relNameAttr.sVal, relName);
    
    // Search for the relation with name relName in the Relation Catalog using linearSearch()
    char relCatAttrRelName[ATTR_SIZE];
    strcpy(relCatAttrRelName, RELCAT_ATTR_RELNAME);

    char attrCatAttrRelName[ATTR_SIZE]; 
    strcpy(attrCatAttrRelName, ATTRCAT_ATTR_RELNAME);

    RecId recId = BlockAccess::linearSearch(RELCAT_RELID,relCatAttrRelName,relNameAttr, EQ);

    // check if it doesn't exist
    if(recId.block == -1 && recId.slot == -1){
        return E_RELNOTEXIST;
    }

    /* reset the searchIndex of the attribute catalog */
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
    RecId attrToRenameRecId;
    attrToRenameRecId.block = -1;
    attrToRenameRecId.slot = -1;
    Attribute attrCatEntryRecord[ATTRCAT_NO_ATTRS];

    /* 
        1   Iterating over all the entries in the attribute catalog to find relation with relname = relName (paramter)
        2.  If found then the check if attrname == oldAttrname and take action accordingly.
    */
    while(true){
        // linearSearch
        RecId rec_id = BlockAccess::linearSearch(ATTRCAT_RELID, attrCatAttrRelName, relNameAttr, EQ);

        // if there is no more attribute left, then break
        if(rec_id.block == -1 && rec_id.slot == -1){
            break;
        }

        /* Get the record from the attribute catalog using RecBuffer.getRecord() */
        RecBuffer attrBuffer(rec_id.block);
        attrBuffer.getRecord(attrCatEntryRecord, rec_id.slot);

        // if attrCatEntryRecord's attrName == oldName
        //      set attrToRenameRecId with rec_id
        if(strcmp(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, oldName) == 0){
            attrToRenameRecId.block = rec_id.block;
            attrToRenameRecId.slot = rec_id.slot;
        }

        // if attrCatEntryRecord.attrName = newName
        //      return E_ATTREXIST
        if(strcmp(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, newName) == 0){
            return E_ATTREXIST;
        }
    }

    // if attrToRenameRecId = {-1,-1} => E_ATTRNOTEXIST
    if(attrToRenameRecId.block == -1 && attrToRenameRecId.slot == -1){
        return E_ATTRNOTEXIST;
    }

    // Update the entry corresponding to the attribute in the Attribute Catalog Relation.
    RecBuffer attrToRenameBuff(attrToRenameRecId.block);
    attrToRenameBuff.getRecord(attrCatEntryRecord, attrToRenameRecId.slot);
    strcpy(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, newName);
    // set back the record with RecBuffer.setRecord
    attrToRenameBuff.setRecord(attrCatEntryRecord, attrToRenameRecId.slot);

    return SUCCESS;
}
int BlockAccess::insert(int relId, Attribute *record){
    // get the relation catalog entry from the relation cache table
    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(relId, &relCatEntry);

    int blockNum = relCatEntry.firstBlk; // first record block of the relation.

    // rec_id will be used to store where the new record will be inserted.
    RecId rec_id = {-1, -1};

    int numOfSlots = relCatEntry.numSlotsPerBlk;    // number of slots per record block
    int numOfAttributes = relCatEntry.numAttrs;     // number of attributes of the relation
    int prevBlockNum = -1;                          // block number of the last element in the linked list

    /*
        Traversing the linked list of existing record block of the relation
        until a free slot is found or until the end of the list is reached.
    */
    while(blockNum != -1){
        //get the slotmap of the block and search for a free slot.
        RecBuffer recBuffer(blockNum);
        struct HeadInfo head;
        recBuffer.getHeader(&head);
        unsigned char slotMap[head.numSlots];
        recBuffer.getSlotMap(slotMap);

        // iterate over slotmap and store the free in the rec_id
        for(int i = 0; i < numOfSlots; i++){
            if(slotMap[i] == SLOT_UNOCCUPIED){
                rec_id.block = blockNum;
                rec_id.slot = i;
                break;
            }
        }

        // if slot is found, then break the traversal.
        if(rec_id.block != -1 && rec_id.slot != -1){
            break;
        }

        // otherwise, continue to check the next block by updating the block number
        prevBlockNum = blockNum;
        blockNum = head.rblock;
    }

    // if slot not found
    if(rec_id.block == -1 && rec_id.slot == -1){
        // if relation is RELCAT, do not allocate any more blocks
        if(relId == RELCAT_RELID){
            return E_MAXRELATIONS; // DOUBTFULL
        }

        // else, get a new record block
        // get the block number of newly allocated block
        RecBuffer newRecBuffer;
        int ret = newRecBuffer.getBlockNum();

        if(ret == E_DISKFULL){
            return E_DISKFULL;
        }

        // Assign rec_id.block = new block number (i.e. ret) and rec_id.slot = 0
        rec_id.block = ret;
        rec_id.slot = 0;

        /*
            set the header of the new record block such that it links with the existing record
            blocks of the relation
         */
        struct HeadInfo newHead;
        newHead.blockType = REC;
        newHead.pblock = -1;
        newHead.lblock = prevBlockNum;
        newHead.rblock = -1;
        newHead.numEntries = 0;
        newHead.numSlots = numOfSlots;
        newHead.numAttrs = numOfAttributes;

        newRecBuffer.setHeader(&newHead);

        /*
            set block's slot map with all slots marked as free
        */
        unsigned char newSlotMap[numOfSlots];
        for(int i = 0; i < numOfSlots; i++){
            newSlotMap[i] = SLOT_UNOCCUPIED;
        }
        newRecBuffer.setSlotMap(newSlotMap);

        // now if there was a previous block, then it's rblock should be set as rec_id.block
        if(prevBlockNum != -1){
            // create a RecBuffer object for prevBlockNum
            // get the header and update the rblock field.
            // then set the header using setHeader()
            RecBuffer prevRecBuffer(prevBlockNum);
            struct HeadInfo prevHead;
            prevRecBuffer.getHeader(&prevHead);
            prevHead.rblock = rec_id.block;
            prevRecBuffer.setHeader(&prevHead);
        }
        else{
            // means it is the 1st block of the relation
            // update first block field in the relCatEntry to the new block
            RelCatEntry relCatEntry;
            RelCacheTable::getRelCatEntry(relId, &relCatEntry);
            relCatEntry.firstBlk = rec_id.block;
            RelCacheTable::setRelCatEntry(relId, &relCatEntry);
        }

        // update the lastBlk field of the relCatEntry to the new block
        RelCatEntry relCatEntry;
        RelCacheTable::getRelCatEntry(relId, &relCatEntry);
        relCatEntry.lastBlk = rec_id.block;
        RelCacheTable::setRelCatEntry(relId, &relCatEntry);
    }

    // Now create a RecBuffer object for rec_id.block
    // and insert the record into the rec_id.slot using the setRecord()
    RecBuffer recBuffer(rec_id.block); // i am using the old recBuffer I created initially.
    recBuffer.setRecord(record, rec_id.slot);

    /*
        update the slotmap of the block by making the entry of the slot to which
        record was inserted as occupied
        Get the slotMap using getSlotMap() ==> update the rec_id.slot-th index as OCCUPIED ==> set the slotMap using setSlotMap()
    */
    struct HeadInfo head;
    recBuffer.getHeader(&head);
    unsigned char slotMap[head.numSlots];
    recBuffer.getSlotMap(slotMap);
    slotMap[rec_id.slot] = SLOT_OCCUPIED;
    recBuffer.setSlotMap(slotMap);

    /*
        increment the numEntries field in the header of the block to
        which record was inserted. Then set the header back using BlockBuffer::setHeader()
    */
    head.numEntries += 1;
    recBuffer.setHeader(&head);

    /*
        Increment the number of records field in the relation cache entry for the relation.
    */
    RelCacheTable::getRelCatEntry(relId, &relCatEntry);
    relCatEntry.numRecs += 1;
    RelCacheTable::setRelCatEntry(relId, &relCatEntry);

    return SUCCESS;
    
}
/*
NOTE: This function will copy the result of the search to the `record` argument.
      The caller should ensure that space is allocated for `record` array
      based on the number of attributes in the relation.
*/
int BlockAccess::search(int relId, Attribute *record, char attrName[ATTR_SIZE], Attribute attrVal, int op) {
    // Declare a variable called recid to store the searched record
    RecId recId;

    /* search for the record id (recid) corresponding to the attribute with
    attribute name attrName, with value attrval and satisfying the condition op
    using linearSearch() */
    recId=BlockAccess::linearSearch(relId,attrName,attrVal,op);

    // if there's no record satisfying the given condition (recId = {-1, -1})
    //    return E_NOTFOUND;

    if(recId.block==-1 && recId.slot==-1){
        return E_NOTFOUND;
    }
    /* Copy the record with record id (recId) to the record buffer (record)
       For this Instantiate a RecBuffer class object using recId and
       call the appropriate method to fetch the record
    */

    RecBuffer recbuffer(recId.block);
    recbuffer.getRecord(record,recId.slot);

    return SUCCESS;
}
int BlockAccess::deleteRelation(char relName[ATTR_SIZE]) {
    // if the relation to delete is either Relation Catalog or Attribute Catalog,
    //     return E_NOTPERMITTED
        // (check if the relation names are either "RELATIONCAT" and "ATTRIBUTECAT".
        // you may use the following constants: RELCAT_NAME and ATTRCAT_NAME)
    if(strcmp(RELCAT_RELNAME,relName)==0 || strcmp(ATTRCAT_RELNAME,relName)==0){
        return E_NOTPERMITTED;
    }
    /* reset the searchIndex of the relation catalog using
       RelCacheTable::resetSearchIndex() */
    RelCacheTable::resetSearchIndex(RELCAT_RELID);
    Attribute relNameAttr; // (stores relName as type union Attribute)
    strcpy(relNameAttr.sVal , relName);

    char relCatAttrRelname[] = RELCAT_ATTR_RELNAME;

    RecId recid= BlockAccess::linearSearch(RELCAT_RELID,relCatAttrRelname,relNameAttr,EQ);
    //  linearSearch on the relation catalog for RelName = relNameAttr

    // if the relation does not exist (linearSearch returned {-1, -1})
    //     return E_RELNOTEXIST

    if(recid.block==-1 &&recid.slot==-1){
        return E_RELNOTEXIST;
    }
    Attribute relCatEntryRecord[RELCAT_NO_ATTRS];
    /* store the relation catalog record corresponding to the relation in
       relCatEntryRecord using RecBuffer.getRecord */
    RecBuffer recbuffer(recid.block);
    recbuffer.getRecord(relCatEntryRecord,recid.slot);

    /* get the first record block of the relation (firstBlock) using the
       relation catalog entry record */
    /* get the number of attributes corresponding to the relation (numAttrs)
       using the relation catalog entry record */
    int firstblock=relCatEntryRecord[RELCAT_FIRST_BLOCK_INDEX].nVal;
    int numattrs=relCatEntryRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal;
    /*
     Delete all the record blocks of the relation
    */
    // for each record block of the relation:
    //     get block header using BlockBuffer.getHeader
    //     get the next block from the header (rblock)
    //     release the block using BlockBuffer.releaseBlock
    //
    //     Hint: to know if we reached the end, check if nextBlock = -1

    int currblock=firstblock;
    while(currblock!=-1){
        struct HeadInfo head;
        RecBuffer currRecbuffer(currblock);
        currRecbuffer.getHeader(&head);
        currblock=head.rblock;
        currRecbuffer.releaseBlock();
    }
    /***
        Deleting attribute catalog entries corresponding the relation and index
        blocks corresponding to the relation with relName on its attributes
    ***/


    // reset the searchIndex of the attribute catalog
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
    int numberOfAttributesDeleted = 0;

    while(true) {
        RecId attrCatRecId=BlockAccess::linearSearch(ATTRCAT_RELID,relCatAttrRelname,relNameAttr,EQ);
        if(attrCatRecId.block==-1 && attrCatRecId.slot==-1){
            break;
        }
        numberOfAttributesDeleted++;

        RecBuffer attrcatbuffer(attrCatRecId.block);

        struct HeadInfo head;
        attrcatbuffer.getHeader(&head);

        Attribute record[ATTRCAT_NO_ATTRS];
        attrcatbuffer.getRecord(record,attrCatRecId.slot);
    
        // if no more attributes to iterate over (attrCatRecId == {-1, -1})
        //     break;


        

        // create a RecBuffer for attrCatRecId.block
        // get the header of the block
        // get the record corresponding to attrCatRecId.slot

        // declare variable rootBlock which will be used to store the root
        // block field from the attribute catalog record.
       // int rootBlock = /* get root blockfrom the record */ ;

        int rootBlock = record[ATTRCAT_ROOT_BLOCK_INDEX].nVal;
        // (This will be used later to delete any indexes if it exists)

        unsigned char slotMap[head.numSlots];
        attrcatbuffer.getSlotMap(slotMap);
        slotMap[attrCatRecId.slot] = SLOT_UNOCCUPIED;
        attrcatbuffer.setSlotMap(slotMap);

        // Update the Slotmap for the block by setting the slot as SLOT_UNOCCUPIED
        // Hint: use RecBuffer.getSlotMap and RecBuffer.setSlotMap

        /* Decrement the numEntries in the header of the block corresponding to
           the attribute catalog entry and then set back the header
           using RecBuffer.setHeader */

        head.numEntries--;
        attrcatbuffer.setHeader(&head);
        /* If number of entries become 0, releaseBlock is called after fixing
           the linked list.
        */
        if (head.numEntries == 0) {
            /* Standard Linked List Delete for a Block
               Get the header of the left block and set it's rblock to this
               block's rblock
            */

            // create a RecBuffer for lblock and call appropriate methods

            
            if(head.lblock!=-1){
                RecBuffer leftrecbuffer(head.lblock);
                struct HeadInfo lefthead;
                leftrecbuffer.getHeader(&lefthead);
                lefthead.rblock=head.rblock;
                leftrecbuffer.setHeader(&lefthead);
            }
            
            if (head.rblock != -1 ) {
                /* Get the header of the right block and set it's lblock to
                   this block's lblock */
                // create a RecBuffer for rblock and call appropriate methods
                RecBuffer rightrecbuffer(head.rblock);
                struct HeadInfo righthead;
                rightrecbuffer.getHeader(&righthead);
                righthead.lblock=head.lblock;
                rightrecbuffer.setHeader(&righthead);
            }
            if (head.lblock == -1) {
                RelCatEntry attrcatEntry;
                RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &attrcatEntry);
                attrcatEntry.firstBlk = head.rblock;
                RelCacheTable::setRelCatEntry(ATTRCAT_RELID, &attrcatEntry);
            }

            if (head.rblock == -1) {
                RelCatEntry attrcatEntry;
                RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &attrcatEntry);
                attrcatEntry.lastBlk = head.lblock;
                RelCacheTable::setRelCatEntry(ATTRCAT_RELID, &attrcatEntry);
            }

            // (Since the attribute catalog will never be empty(why?), we do not
            //  need to handle the case of the linked list becoming empty - i.e
            //  every block of the attribute catalog gets released.)

            attrcatbuffer.releaseBlock();
        }

        // (the following part is only relevant once indexing has been implemented)
        // if index exists for the attribute (rootBlock != -1), call bplus destroy
        //if (rootBlock != -1) {
            // delete the bplus tree rooted at rootBlock using BPlusTree::bPlusDestroy()
        //}
    }


    /*** Delete the entry corresponding to the relation from relation catalog ***/
    // Fetch the header of Relcat block
    
        // if (rootBlock != -1) {
        //     BPlusTree::bPlusDestroy(rootBlock);
        // }

    struct HeadInfo relcathead;
    recbuffer.getHeader(&relcathead);
    relcathead.numEntries--;
    recbuffer.setHeader(&relcathead);

    /* Decrement the numEntries in the header of the block corresponding to the
       relation catalog entry and set it back */

    unsigned char slotMap[relcathead.numSlots];
    recbuffer.getSlotMap(slotMap);
    slotMap[recid.slot]=SLOT_UNOCCUPIED;
    recbuffer.setSlotMap(slotMap);
    /* Get the slotmap in relation catalog, update it by marking the slot as
       free(SLOT_UNOCCUPIED) and set it back. */

    /*** Updating the Relation Cache Table ***/
    /** Update relation catalog record entry (number of records in relation
        catalog is decreased by 1) **/
    
    RelCatEntry relcatentry;
    RelCacheTable::getRelCatEntry(RELCAT_RELID,&relcatentry);
    relcatentry.numRecs--;
    RelCacheTable::setRelCatEntry(RELCAT_RELID,&relcatentry);

    RelCatEntry attrcatentry;
    RelCacheTable::getRelCatEntry(ATTRCAT_RELID,&attrcatentry);
    attrcatentry.numRecs-=numberOfAttributesDeleted;
    RelCacheTable::setRelCatEntry(ATTRCAT_RELID,&attrcatentry);
    // Get the entry corresponding to relation catalog from the relation
    // cache and update the number of records and set it back
    // (using RelCacheTable::setRelCatEntry() function)

    /** Update attribute catalog entry (number of records in attribute catalog
        is decreased by numberOfAttributesDeleted) **/
    // i.e., #Records = #Records - numberOfAttributesDeleted

    // Get the entry corresponding to attribute catalog from the relation
    // cache and update the number of records and set it back
    // (using RelCacheTable::setRelCatEntry() function)

    return SUCCESS;
}
/*
NOTE: the caller is expected to allocate space for the argument `record` based
      on the size of the relation. This function will only copy the result of
      the projection onto the array pointed to by the argument.
*/
int BlockAccess::project(int relId, Attribute *record) {
    // get the previous search index of the relation relId from the relation
    // cache (use RelCacheTable::getSearchIndex() function)
    RecId prevRecId;
    RelCacheTable::getSearchIndex(relId,&prevRecId);
    // declare block and slot which will be used to store the record id of the
    // slot we need to check.
    int block, slot;

    /* if the current search index record is invalid(i.e. = {-1, -1})
       (this only happens when the caller reset the search index)
    */
    if (prevRecId.block == -1 && prevRecId.slot == -1)
    {
        // (new project operation. start from beginning)

        // get the first record block of the relation from the relation cache
        // (use RelCacheTable::getRelCatEntry() function of Cache Layer)

        RelCatEntry relcatentry;
        int ret=RelCacheTable::getRelCatEntry(relId,&relcatentry);

        if(ret!=SUCCESS){
            return ret;
        }
        block=relcatentry.firstBlk;
        slot=0;
        // block = first record block of the relation
        // slot = 0
    }
    else
    {
        // (a project/search operation is already in progress)

        // block = previous search index's block
        // slot = previous search index's slot + 1
        block=prevRecId.block;
        slot=prevRecId.slot+1;
    }


    // The following code finds the next record of the relation
    /* Start from the record id (block, slot) and iterate over the remaining
       records of the relation */
    while (block != -1)
    {
        // create a RecBuffer object for block (using appropriate constructor!)

        RecBuffer recbuffer(block);
        struct HeadInfo head;
        recbuffer.getHeader(&head);

        unsigned char slotmap[head.numSlots];
        recbuffer.getSlotMap(slotmap);

        // get header of the block using RecBuffer::getHeader() function
        // get slot map of the block using RecBuffer::getSlotMap() function

        if(slot>=head.numSlots)
        {
            // (no more slots in this block)
            // update block = right block of block
            block=head.rblock;
            slot=0;
            // update slot = 0
            // (NOTE: if this is the last block, rblock would be -1. this would
            //        set block = -1 and fail the loop condition )
        }
        else if (slotmap[slot]==SLOT_UNOCCUPIED)
        { // (i.e slot-th entry in slotMap contains SLOT_UNOCCUPIED)

            slot++;
            // increment slot
        }
        else {
            // (the next occupied slot / record has been found)
            break;
        }
    }

    if (block == -1){
        // (a record was not found. all records exhausted)
        return E_NOTFOUND;
    }

    // declare nextRecId to store the RecId of the record found
    RecId nextRecId{block, slot};

    RelCacheTable::setSearchIndex(relId,&nextRecId);
    RecBuffer recBuffer(nextRecId.block);
    recBuffer.getRecord(record, nextRecId.slot);
    // set the search index to nextRecId using RelCacheTable::setSearchIndex

    /* Copy the record with record id (nextRecId) to the record buffer (record)
       For this Instantiate a RecBuffer class object by passing the recId and
       call the appropriate method to fetch the record
    */

    return SUCCESS;
}