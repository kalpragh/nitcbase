#include "Schema.h"

#include <cmath>
#include <cstring>

int Schema::openRel(char relName[ATTR_SIZE])
{
    int ret = OpenRelTable::openRel(relName);

    // OpenRelTable::openRel() will send rel-id [0,12].
    // If relation is not open, any error code will return negative integer.
    if (ret >= 0)
    {
        return SUCCESS;
    }

    return ret;
}

int Schema::closeRel(char relName[ATTR_SIZE])
{
    // if relation in relation catalog or attribute catalog, the not permited to close
    if (strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0)
    {
        return E_NOTPERMITTED;
    }

    int relId = OpenRelTable::getRelId(relName);
    if (relId == E_RELNOTOPEN)
    {
        return E_RELNOTOPEN;
    }

    return OpenRelTable::closeRel(relId);
}

// renameRel(): method to change the relation name of specified relation to a new specified name.
int Schema::renameRel(char oldRelName[ATTR_SIZE], char newRelName[ATTR_SIZE])
{
    // oldRelName and newRelName should not be same as relation catalog or attribute catalog name.
    if (strcmp(oldRelName, RELCAT_RELNAME) == 0 || strcmp(newRelName, RELCAT_RELNAME) == 0 || strcmp(oldRelName, ATTRCAT_RELNAME) == 0 || strcmp(newRelName, ATTRCAT_RELNAME) == 0)
    {
        return E_NOTPERMITTED;
    }

    // check if relation is closed or not
    // NOTE: Relation must be close to perform this function
    int relId = OpenRelTable::getRelId(oldRelName);
    if (relId >= 0)
    {
        return E_RELOPEN;
    }

    int retVal = BlockAccess::renameRelation(oldRelName, newRelName);
    return retVal;
}

// renameAttr(): method used to change attrname of a given relation
// NOTE: Relation must be close to perfrom this operation.
int Schema::renameAttr(char relName[ATTR_SIZE], char oldAttrName[ATTR_SIZE], char newAttrName[ATTR_SIZE])
{
    // check if relName is not same as RELCAT_NAME and ATTRCAT_NAME
    if (strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0)
    {
        return E_NOTPERMITTED;
    }

    // check if relation is closed or not
    int relId = OpenRelTable::getRelId(relName);
    if (relId >= 0)
    {
        return E_RELOPEN;
    }

    int retVal = BlockAccess::renameAttribute(relName, oldAttrName, newAttrName);
    return retVal;
}
int Schema::createRel(char relName[], int nAttrs, char attrs[][ATTR_SIZE], int attrtype[])
{

    Attribute relNameAsAttribute;
    strcpy(relNameAsAttribute.sVal, relName);
    // copy the relName into relNameAsAttribute.sVal
    RecId targetRelId;
    // declare a variable targetRelId of type RecId

    // Reset the searchIndex using RelCacheTable::resetSearhIndex()
    RelCacheTable::resetSearchIndex(RELCAT_RELID);
    char relcatattrrelname[] = RELCAT_ATTR_RELNAME;
    targetRelId = BlockAccess::linearSearch(RELCAT_RELID, relcatattrrelname, relNameAsAttribute, EQ);
    // Search the relation catalog (relId given by the constant RELCAT_RELID)
    // for attribute value attribute "RelName" = relNameAsAttribute using
    // BlockAccess::linearSearch() with OP = EQ

    if (targetRelId.block != -1 && targetRelId.slot != -1)
    {
        return E_RELEXIST;
    }
    // if a relation with name `relName` already exists  ( linearSearch() does
    //                                                     not return {-1,-1} )
    //     return E_RELEXIST;

    // compare every pair of attributes of attrNames[] array
    // if any attribute names have same string value,
    //     return E_DUPLICATEATTR (i.e 2 attributes have same value)

    for (int i = 0; i < nAttrs; i++)
    {
        for (int j = i + 1; j < nAttrs; j++)
        {
            if (strcmp(attrs[i], attrs[j]) == 0)
            {
                return E_DUPLICATEATTR;
            }
        }
    }
    /* declare relCatRecord of type Attribute which will be used to store the
       record corresponding to the new relation which will be inserted
       into relation catalog */
    Attribute relCatRecord[RELCAT_NO_ATTRS];
    // fill relCatRecord fields as given below
    // offset RELCAT_REL_NAME_INDEX: relName
    // offset RELCAT_NO_ATTRIBUTES_INDEX: numOfAttributes
    // offset RELCAT_NO_RECORDS_INDEX: 0
    // offset RELCAT_FIRST_BLOCK_INDEX: -1
    // offset RELCAT_LAST_BLOCK_INDEX: -1
    // offset RELCAT_NO_SLOTS_PER_BLOCK_INDEX: floor((2016 / (16 * nAttrs + 1)))
    // (number of slots is calculated as specified in the physical layer docs)

    strcpy(relCatRecord[RELCAT_REL_NAME_INDEX].sVal, relName);
    relCatRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal = nAttrs;
    relCatRecord[RELCAT_NO_RECORDS_INDEX].nVal = 0;
    relCatRecord[RELCAT_FIRST_BLOCK_INDEX].nVal = -1;
    relCatRecord[RELCAT_LAST_BLOCK_INDEX].nVal = -1;
    relCatRecord[RELCAT_NO_SLOTS_PER_BLOCK_INDEX].nVal = floor((2016 / (16 * nAttrs + 1)));
    // (number of slots is calculated as specified in the physical layer docs)

    int retVal = BlockAccess::insert(RELCAT_RELID, relCatRecord);
    if (retVal != SUCCESS)
        return retVal;
    // if BlockAccess::insert fails return retVal
    // (this call could fail if there is no more space in the relation catalog)

    // iterate through 0 to numOfAttributes - 1 :
    for (int i = 0; i < nAttrs; i++)
    {
        /* declare Attribute attrCatRecord[6] to store the attribute catalog
           record corresponding to i'th attribute of the argument passed*/
        Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
        // (where i is the iterator of the loop)
        // fill attrCatRecord fields as given below
        strcpy(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal, relName);
        strcpy(attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, attrs[i]);
        attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal = attrtype[i];
        attrCatRecord[ATTRCAT_PRIMARY_FLAG_INDEX].nVal = -1;
        attrCatRecord[ATTRCAT_ROOT_BLOCK_INDEX].nVal = -1;
        attrCatRecord[ATTRCAT_OFFSET_INDEX].nVal = i;

        retVal = BlockAccess::insert(ATTRCAT_RELID, attrCatRecord);
        if (retVal != SUCCESS)
        {
            Schema::deleteRel(relName);
            return E_DISKFULL;
            /* if attribute catalog insert fails:
                 delete the relation by calling deleteRel(targetrel) of schema layer
                 return E_DISKFULL
                 // (this is necessary because we had already created the
                 //  relation catalog entry which needs to be removed)
            */
        }
    }
    return SUCCESS;
}

int Schema::deleteRel(char *relName)
{
    if (strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0)
    {
        return E_NOTPERMITTED;
    }
    // if the relation to delete is either Relation Catalog or Attribute Catalog,
    //     return E_NOTPERMITTED
    // (check if the relation names are either "RELATIONCAT" and "ATTRIBUTECAT".
    // you may use the following constants: RELCAT_RELNAME and ATTRCAT_RELNAME)
    int relId = OpenRelTable::getRelId(relName);
    // get the rel-id using appropriate method of OpenRelTable class by
    // passing relation name as argument
    if (relId >= 0 && relId < MAX_OPEN)
    {
        return E_RELOPEN;
    }
    // if relation is opened in open relation table, return E_RELOPEN

    int ret = BlockAccess::deleteRelation(relName);
    return ret;

    // return the value returned by the above deleteRelation() call

    /* the only that should be returned from deleteRelation() is E_RELNOTEXIST.
       The deleteRelation call may return E_OUTOFBOUND from the call to
       loadBlockAndGetBufferPtr, but if your implementation so far has been
       correct, it should not reach that point. That error could only occur
       if the BlockBuffer was initialized with an invalid block number.
    */
}

int Schema::createIndex(char relName[ATTR_SIZE], char attrName[ATTR_SIZE])
{
    if (strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0)
    {
        return E_NOTPERMITTED;
    }
    int relid = OpenRelTable::getRelId(relName);
    if (relid < 0 || relid >= MAX_OPEN)
        return E_RELNOTOPEN;
    return BPlusTree::bPlusCreate(relid, attrName);
}
int Schema::dropIndex(char *relName, char *attrName)
{
    if (strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0)
    {
        return E_NOTPERMITTED;
    }
    int relid = OpenRelTable::getRelId(relName);
    if (relid < 0 || relid >= MAX_OPEN)
        return E_RELNOTOPEN;
    AttrCatEntry attrcatentry;
    int ret = AttrCacheTable::getAttrCatEntry(relid, attrName, &attrcatentry);

    if (ret != SUCCESS)
        return ret;

    int rootBlock = attrcatentry.rootBlock;
    if (rootBlock == -1)
    {
        return E_NOINDEX;
    }
    BPlusTree::bPlusDestroy(rootBlock);

    attrcatentry.rootBlock = -1;
    ret = AttrCacheTable::setAttrCatEntry(relid, attrName, &attrcatentry);

    if (ret != SUCCESS)
        return ret;
    return SUCCESS;
}