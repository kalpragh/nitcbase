#include "OpenRelTable.h"
#include <cstdlib>   // malloc, free
#include <cstring>
#include "../BlockAccess/BlockAccess.h"


OpenRelTableMetaInfo OpenRelTable::tableMetaInfo[MAX_OPEN];

OpenRelTable::OpenRelTable() {

  for (int i = 0; i < MAX_OPEN; i++) {
    RelCacheTable::relCache[i] = nullptr;
    AttrCacheTable::attrCache[i] = nullptr;

    tableMetaInfo[i].free = true;
    tableMetaInfo[i].relName[0] = '\0';
  }

  
  RecBuffer relCatBlock(RELCAT_BLOCK);

  /************ RELATION CACHE ************/

  // RELCAT
  Attribute relCatRecord[RELCAT_NO_ATTRS];
  relCatBlock.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_RELCAT);

  RelCacheEntry relCacheEntry0;
  RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry0.relCatEntry);
  relCacheEntry0.recId.block = RELCAT_BLOCK;
  relCacheEntry0.recId.slot  = RELCAT_SLOTNUM_FOR_RELCAT;
  relCacheEntry0.searchIndex.block = -1;
  relCacheEntry0.searchIndex.slot  = -1;

  RelCacheTable::relCache[RELCAT_RELID] =(RelCacheEntry*)malloc(sizeof(RelCacheEntry));
  *(RelCacheTable::relCache[RELCAT_RELID]) = relCacheEntry0;

  // ATTRCAT (relation entry stored in RELCAT)
  Attribute attrRelRecord[RELCAT_NO_ATTRS]; 
  relCatBlock.getRecord(attrRelRecord, RELCAT_SLOTNUM_FOR_ATTRCAT);

  RelCacheEntry relCacheEntry1;
  RelCacheTable::recordToRelCatEntry(attrRelRecord, &relCacheEntry1.relCatEntry);
  relCacheEntry1.recId.block = RELCAT_BLOCK;
  relCacheEntry1.recId.slot  = RELCAT_SLOTNUM_FOR_ATTRCAT;
  relCacheEntry1.searchIndex.block = -1;
  relCacheEntry1.searchIndex.slot  = -1;

  RelCacheTable::relCache[ATTRCAT_RELID] =(RelCacheEntry*)malloc(sizeof(RelCacheEntry));
  *(RelCacheTable::relCache[ATTRCAT_RELID]) = relCacheEntry1;

  /************ ATTRIBUTE CACHE ************/
  RecBuffer attrCatBlock(ATTRCAT_BLOCK);
  Attribute attrCatRecord[ATTRCAT_NO_ATTRS];

  // RELCAT attrs: slots 0..5
  AttrCacheEntry* headRel = nullptr;
  AttrCacheEntry* tailRel = nullptr;

  for (int slot = 0; slot <= 5; slot++) {
    attrCatBlock.getRecord(attrCatRecord, slot);

    AttrCacheEntry* node = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
    AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &node->attrCatEntry);

    node->recId.block = ATTRCAT_BLOCK;
    node->recId.slot  = slot;
    node->next = nullptr;

    if (!headRel) headRel = tailRel = node;
    else { tailRel->next = node; tailRel = node; }
  }

  AttrCacheTable::attrCache[RELCAT_RELID] = headRel;

  // ATTRCAT attrs: slots 6-11
  AttrCacheEntry* headAttr = nullptr;
  AttrCacheEntry* tailAttr = nullptr;

  for (int slot = 6; slot <= 11; slot++) {
    attrCatBlock.getRecord(attrCatRecord, slot);

    AttrCacheEntry* node = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
    AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &node->attrCatEntry);

    node->recId.block = ATTRCAT_BLOCK;
    node->recId.slot  = slot;
    node->next = nullptr;

    if (!headAttr) headAttr = tailAttr = node;
    else { tailAttr->next = node; tailAttr = node; }
  }

  AttrCacheTable::attrCache[ATTRCAT_RELID] = headAttr;

  //meta info
  tableMetaInfo[RELCAT_RELID].free = false;
  strcpy(tableMetaInfo[RELCAT_RELID].relName, RELCAT_RELNAME);

  tableMetaInfo[ATTRCAT_RELID].free = false;
  strcpy(tableMetaInfo[ATTRCAT_RELID].relName, ATTRCAT_RELNAME);
}


OpenRelTable::~OpenRelTable() {

  // close all open relations (from rel-id = 2 onwards. Why?)
  for (int i = 2; i < MAX_OPEN; ++i) {
    if (!tableMetaInfo[i].free) {
      OpenRelTable::closeRel(i); // we will implement this function later
    }
  }

  // free the memory allocated for rel-id 0 and 1 in the caches
  if (RelCacheTable::relCache[RELCAT_RELID] != nullptr) {
    free(RelCacheTable::relCache[RELCAT_RELID]);
    RelCacheTable::relCache[RELCAT_RELID] = nullptr;
  }

  if (RelCacheTable::relCache[ATTRCAT_RELID] != nullptr) {
    free(RelCacheTable::relCache[ATTRCAT_RELID]);
    RelCacheTable::relCache[ATTRCAT_RELID] = nullptr;
  }
  for (int catRelId = 0; catRelId <= 1; ++catRelId) {
    AttrCacheEntry* cur = AttrCacheTable::attrCache[catRelId];
    while (cur != nullptr) {
      AttrCacheEntry* nxt = cur->next;
      free(cur);
      cur = nxt;
    }
    AttrCacheTable::attrCache[catRelId] = nullptr;
  }

  // optional cleanup for meta info
  for (int i = 0; i < MAX_OPEN; i++) {
    tableMetaInfo[i].free = true;
    tableMetaInfo[i].relName[0] = '\0';
  }
}
//implement these
//OpenRelTable :: getFreeOpenRelTableEntry
int OpenRelTable::getFreeOpenRelTableEntry() {
  for(int i=0;i<MAX_OPEN;i++){
    if(tableMetaInfo[i].free){
      return i;
    }
  }
  return E_CACHEFULL;
}
//OpenRelTable::getRelId()
int OpenRelTable::getRelId(char relName[ATTR_SIZE]) {
  for(int i=0;i<MAX_OPEN;i++){
    if(!tableMetaInfo[i].free && strcmp(tableMetaInfo[i].relName,relName)==0){
      return i;
    }
  }
  return E_RELNOTOPEN;
}

//OpenRelTable::openRel()
int OpenRelTable::openRel(char relName[ATTR_SIZE]) {

  // 1) If already open, return existing relId
  int already = OpenRelTable::getRelId(relName);
  if (already >= 0) return already;

  // 2) Find a free slot
  int relId = OpenRelTable::getFreeOpenRelTableEntry();
  if (relId < 0) return E_CACHEFULL;

  /************ RELATION CACHE ENTRY ************/

  // Build Attribute value for comparison: RelName == relName
  Attribute relNameAttr;
  strcpy(relNameAttr.sVal, relName);

  // Reset RELCAT searchIndex before searching RELCAT
  RelCacheTable::resetSearchIndex(RELCAT_RELID);

  // Search RELCAT for the relation's catalog record
  // attrName should be the attribute NAME in RELCAT: usually "RelName"
  char relcat_attr[ATTR_SIZE];
  strcpy(relcat_attr, "RelName");

  RecId relCatRecId = BlockAccess::linearSearch(RELCAT_RELID, relcat_attr, relNameAttr, EQ);

  if (relCatRecId.block == -1 && relCatRecId.slot == -1) {
    return E_RELNOTEXIST;
  }

  // Read the RELCAT record and build RelCacheEntry
  RecBuffer relCatBlock(relCatRecId.block);
  Attribute relCatRecord[RELCAT_NO_ATTRS];
  relCatBlock.getRecord(relCatRecord, relCatRecId.slot);

  RelCacheEntry relCacheEntry;
  RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);

  relCacheEntry.recId = relCatRecId;
  relCacheEntry.searchIndex.block = -1;
  relCacheEntry.searchIndex.slot  = -1;

  RelCacheTable::relCache[relId] = (RelCacheEntry*)malloc(sizeof(RelCacheEntry));
  *(RelCacheTable::relCache[relId]) = relCacheEntry;


  /************ ATTRIBUTE CACHE LINKED LIST ************/

  // Reset ATTRCAT searchIndex before first ATTRCAT scan
  RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

  AttrCacheEntry* listHead = nullptr;
  AttrCacheEntry* listTail = nullptr;

  // We expect exactly numAttrs attribute-catalog entries for this relation
  for (int i = 0; i < relCacheEntry.relCatEntry.numAttrs; i++) {

    // Search ATTRCAT for next attribute row with RelName == relName
    char attrcat_attr[ATTR_SIZE];
    strcpy(attrcat_attr, "RelName");

    RecId attrCatRecId = BlockAccess::linearSearch(ATTRCAT_RELID, attrcat_attr, relNameAttr, EQ);

    // If we don't find enough attribute entries, the catalog is inconsistent
    if (attrCatRecId.block == -1 && attrCatRecId.slot == -1) {

      // cleanup what we allocated for this relation before returning
      // free AttrCache list built so far
      AttrCacheEntry* cur = listHead;
      while (cur != nullptr) {
        AttrCacheEntry* nxt = cur->next;
        free(cur);
        cur = nxt;
      }
      AttrCacheTable::attrCache[relId] = nullptr;

      // free relation cache entry
      if (RelCacheTable::relCache[relId] != nullptr) {
        free(RelCacheTable::relCache[relId]);
        RelCacheTable::relCache[relId] = nullptr;
      }

      return E_RELNOTEXIST; // or a custom "catalog corrupted" error if you have one
    }

    // Read ATTRCAT record and build AttrCacheEntry
    RecBuffer attrCatBlock(attrCatRecId.block);
    Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
    attrCatBlock.getRecord(attrCatRecord, attrCatRecId.slot);

    AttrCacheEntry* node = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
    AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &node->attrCatEntry);

    node->recId = attrCatRecId;
    node->next  = nullptr;

    if (listHead == nullptr) {
      listHead = listTail = node;
    } else {
      listTail->next = node;
      listTail = node;
    }
  }

  AttrCacheTable::attrCache[relId] = listHead;


  /************ UPDATE tableMetaInfo ************/
  tableMetaInfo[relId].free = false;
  strcpy(tableMetaInfo[relId].relName, relName);

  return relId;
}

//close rel

int OpenRelTable::closeRel(int relId) {
  if (relId==RELCAT_RELID || relId==ATTRCAT_RELID) {
    return E_NOTPERMITTED;
  }

  if ( relId<0 || relId >= MAX_OPEN) {
    return E_OUTOFBOUND;
  }

  if (tableMetaInfo[relId].free==true) {
    return E_RELNOTOPEN;
  }

  // free the memory allocated in the relation and attribute caches which was
  // allocated in the OpenRelTable::openRel() functioy
  //freeing relcache entru
   if (RelCacheTable::relCache[relId] != nullptr) {
    free(RelCacheTable::relCache[relId]);
    RelCacheTable::relCache[relId] = nullptr;
  }
  //free attrcache entru
  AttrCacheEntry *cur = AttrCacheTable::attrCache[relId];
  while (cur != nullptr) {
    AttrCacheEntry *nxt = cur->next;
    free(cur);
    cur = nxt;
  }
  AttrCacheTable::attrCache[relId] = nullptr;

  tableMetaInfo[relId].free = true;
  tableMetaInfo[relId].relName[0] = '\0';
  return SUCCESS;
}
