#include "BlockAccess.h"
#include <cstdlib>
#include <cstring>
RecId BlockAccess::linearSearch(int relId, char attrName[ATTR_SIZE], union Attribute attrVal, int op) {
  
    RecId prevRecId;
    RelCacheTable::getSearchIndex(relId, &prevRecId);
    
    int block, slot;

    if (prevRecId.block == -1 && prevRecId.slot == -1)
    {
        RelCatEntry relCatBuf;
        RelCacheTable::getRelCatEntry(relId, &relCatBuf);
        block = relCatBuf.firstBlk;
        slot = 0;
         
    }
    else
    {
       block = prevRecId.block;
       slot  = prevRecId.slot + 1;        
    }
     AttrCatEntry attrCatBuf;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatBuf);
    int offset = attrCatBuf.offset;

    while (block != -1)
    {
         RecBuffer buf(block);
         HeadInfo header;
         buf.getHeader(&header);

         unsigned char slotMap[header.numSlots];

         buf.getSlotMap(slotMap);

       if(slot >= header.numSlots)  // If slot >= the number of slots per block(i.e. no more slots in this block        
        {
            block = header.rblock;
            slot = 0;
            continue;  // continue to the beginning of this while loop
        }

        if(slotMap[slot] == SLOT_UNOCCUPIED)         // if slot is free skip the loop (i.e. check if slot'th entry in slot map of block contains SLOT_UNO
        {
          slot++;
          continue;
        }
         // compare record's attribute value to the the given attrVal as below:
        /*
            firstly get the attribute offset for the attrName attribute
            from the attribute cache entry of the relation using
            AttrCacheTable::getAttrCatEntry()
        */
        /* use the attribute offset to get the value of the attribute from
           current record */
        Attribute record[header.numAttrs];
        buf.getRecord(record, slot);
        Attribute recAttr = record[offset];

        int cmpVal = compareAttrs(recAttr, attrVal, attrCatBuf.attrType);
        
        if (
            (op == NE && cmpVal != 0) ||    // if op is "not equal to" 
            (op == LT && cmpVal < 0) ||     // if op is "less than"
            (op == LE && cmpVal <= 0) || // if op is "less than or equalto"
            (op == EQ && cmpVal == 0) ||    // if op is "equal to"
            (op == GT && cmpVal > 0) ||     // if op is "greater than"
            (op == GE && cmpVal >= 0)     // if op is "greater than or equalto"
        )
        {
            RecId currRecId = {block, slot};
            RelCacheTable::setSearchIndex(relId, &currRecId);

            return RecId{block, slot};
        }

        slot++;
    }

    // no record in the relation with Id relid satisfies the given condition
    
 return RecId{-1, -1};
}

int BlockAccess::renameRelation(char oldName[ATTR_SIZE], char newName[ATTR_SIZE]){
    
    RelCacheTable::resetSearchIndex(RELCAT_RELID);
    
    Attribute newRelationName;    // set newRelationName with newName
    strcpy(newRelationName.sVal,newName);
    // search the relation catalog for an entry with "RelName" = newRelationName
    char *relCatattrRelName;
    strcpy(relCatattrRelName,RELCAT_ATTR_RELNAME);
    RecId recId=BlockAccess::linearSearch(RELCAT_RELID,relCatattrRelName,newRelationName,EQ);
    if(recId.block!=-1 && recId.slot!=-1){
        return E_RELEXIST;
    }
    RelCacheTable::resetSearchIndex(RELCAT_RELID);
    /* reset the searchIndex of the relation catalog using
       RelCacheTable::resetSearchIndex() */

    Attribute oldRelationName;    // set oldRelationName with oldName
    strcpy(oldRelationName.sVal,oldName);
    // search the relation catalog for an entry with "RelName" = newRelationName
    recId=BlockAccess::linearSearch(RELCAT_RELID,relCatattrRelName,oldRelationName,EQ);
    if(recId.block!=-1 && recId.slot!=-1){
        return E_RELNOTEXIST;
    }// search the relation catalog for an entry with "RelName" = oldRelationName

    // If relation with name oldName does not exist (result of linearSearch is {-1, -1})
    //    return E_RELNOTEXIST;

    /* get the relation catalog record of the relation to rename using a RecBuffer
       on the relation catalog [RELCAT_BLOCK] and RecBuffer.getRecord function
    */
    Attribute record[RELCAT_NO_ATTRS];
    RecBuffer recBuffer(RELCAT_BLOCK);
    recBuffer.getRecord(record,recId.slot);
    /* update the relation name attribute in the record with newName.
       (use RELCAT_REL_NAME_INDEX) */
    // set back the record value using RecBuffer.setRecord
    strcpy(record[RELCAT_REL_NAME_INDEX].sVal, newName);
    recBuffer.setRecord(record, recId.slot);
    /*
    update all the attribute catalog entries in the attribute catalog corresponding
    to the relation with relation name oldName to the relation name newName
    */

    // reset the searchIndex of the attribute catalog using
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID); 
    int numOfAttrs = record[RELCAT_NO_ATTRIBUTES_INDEX].nVal;
    char *attrCatAttrRelName;
    strcpy(attrCatAttrRelName, ATTRCAT_ATTR_RELNAME);
    for(int i = 0 ; i< numOfAttrs ;i++){
        RecId rec_id = BlockAccess::linearSearch(ATTRCAT_RELID, attrCatAttrRelName, oldRelationName, EQ);
        Attribute attrRecord[ATTRCAT_NO_ATTRS];
        RecBuffer attrBuffer(rec_id.block);
        attrBuffer.getRecord(attrRecord, rec_id.slot);

        attrRecord[ATTRCAT_REL_NAME_INDEX] = newRelationName;
        attrBuffer.setRecord(attrRecord, rec_id.slot);
    }
    //    linearSearch on the attribute catalog for relName = oldRelationName
    //    get the record using RecBuffer.getRecord
    //
    //    update the relName field in the record to newName
    //    set back the record using RecBuffer.setRecord

    return SUCCESS;
}

int BlockAccess::renameAttribute(char relName[ATTR_SIZE], char oldName[ATTR_SIZE], char newName[ATTR_SIZE]) {

    /* reset the searchIndex of the relation catalog using
       RelCacheTable::resetSearchIndex() */

    Attribute relNameAttr;    // set relNameAttr to relName

    // Search for the relation with name relName in relation catalog using linearSearch()
    // If relation with name relName does not exist (search returns {-1,-1})
    //    return E_RELNOTEXIST;

    /* reset the searchIndex of the attribute catalog using
       RelCacheTable::resetSearchIndex() */

    /* declare variable attrToRenameRecId used to store the attr-cat recId
    of the attribute to rename */
    RecId attrToRenameRecId{-1, -1};
    Attribute attrCatEntryRecord[ATTRCAT_NO_ATTRS];

    /* iterate over all Attribute Catalog Entry record corresponding to the
       relation to find the required attribute */
    while (true) {
        // linear search on the attribute catalog for RelName = relNameAttr

        // if there are no more attributes left to check (linearSearch returned {-1,-1})
        //     break;

        /* Get the record from the attribute catalog using RecBuffer.getRecord
          into attrCatEntryRecord */

        // if attrCatEntryRecord.attrName = oldName
        //     attrToRenameRecId = block and slot of this record

        // if attrCatEntryRecord.attrName = newName
        //     return E_ATTREXIST;
    }

    // if attrToRenameRecId == {-1, -1}
    //     return E_ATTRNOTEXIST;


    // Update the entry corresponding to the attribute in the Attribute Catalog Relation.
    /*   declare a RecBuffer for attrToRenameRecId.block and get the record at
         attrToRenameRecId.slot */
    //   update the AttrName of the record with newName
    //   set back the record with RecBuffer.setRecord

    return SUCCESS;
}