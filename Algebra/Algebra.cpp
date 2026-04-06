#include "Algebra.h"
#include <cstdio>
#include <cstdlib>
#include <cstring>
extern int comparisoncount;
bool isNumber(char *str);
/* used to select all the records that satisfy a condition.
the arguments of the function are
- srcRel - the source relation we want to select from
- targetRel - the relation we want to select into. (ignore for now)
- attr - the attribute that the condition is checking
- op - the operator of the condition
- strVal - the value that we want to compare against (represented as a string)
*/
// select for stage 8
//  int Algebra::select(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], char attr[ATTR_SIZE], int op, char strVal[ATTR_SIZE]) {
//    int srcRelId = OpenRelTable::getRelId(srcRel);      // we'll implement this later
//    if (srcRelId == E_RELNOTOPEN) {
//      return E_RELNOTOPEN;
//    }

//   AttrCatEntry attrCatEntry;
//   // get the attribute catalog entry for attr, using AttrCacheTable::getAttrcatEntry()
//   //    return E_ATTRNOTEXIST if it returns the error

//   int ret = AttrCacheTable::getAttrCatEntry(srcRelId, attr, &attrCatEntry);
//   if (ret == E_ATTRNOTEXIST) return E_ATTRNOTEXIST;
//   if (ret != SUCCESS) return ret;

//   /*** Convert strVal (string) to an attribute of data type NUMBER or STRING ***/
//   int type = attrCatEntry.attrType;
//   Attribute attrVal;
//   if (type == NUMBER) {
//     if (isNumber(strVal)) {       // the isNumber() function is implemented below
//       attrVal.nVal = atof(strVal);
//     } else {
//       return E_ATTRTYPEMISMATCH;
//     }
//   } else if (type == STRING) {
//     strcpy(attrVal.sVal, strVal);
//   } else {
//       return E_ATTRTYPEMISMATCH;
//   }

//   /*** Selecting records from the source relation ***/

//   // Before calling the search function, reset the search to start from the first hit
//   // using RelCacheTable::resetSearchIndex()
//   ret = RelCacheTable::resetSearchIndex(srcRelId);
//   if (ret != SUCCESS) return ret;

//   RelCatEntry relCatEntry;
//   // get relCatEntry using RelCacheTable::getRelCatEntry()
//   ret = RelCacheTable::getRelCatEntry(srcRelId, &relCatEntry);
//   if (ret != SUCCESS) return ret;
//   /************************
//   The following code prints the contents of a relation directly to the output
//   console. Direct console output is not permitted by the actual the NITCbase
//   specification and the output can only be inserted into a new relation. We will
//   be modifying it in the later stages to match the specification.
//   ************************/

//   printf("|");
//   for (int i = 0; i < relCatEntry.numAttrs; ++i) {
//     AttrCatEntry attrCatEntry;
//     // get attrCatEntry at offset i using AttrCacheTable::getAttrCatEntry()
//     ret = AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);
//     if(ret!=SUCCESS)return ret;
//     printf(" %s |", attrCatEntry.attrName);
//   }
//   printf("\n");

//   while (true) {
//     RecId searchRes = BlockAccess::linearSearch(srcRelId, attr, attrVal, op);

//     if (searchRes.block != -1 && searchRes.slot != -1) {
//         RecBuffer recBlock(searchRes.block);

//         Attribute record[relCatEntry.numAttrs];
//         recBlock.getRecord(record, searchRes.slot);

//          // print the attribute values in the same format as above
//         printf("|");
//         for (int i = 0; i < relCatEntry.numAttrs; i++) {
//         AttrCatEntry a;
//         // get attrCatEntry at offset i using AttrCacheTable::getAttrCatEntry()
//         AttrCacheTable::getAttrCatEntry(srcRelId, i, &a);

//         if (a.attrType == NUMBER) {
//             printf(" %.0lf |", record[i].nVal);
//         } else { // STRING
//             printf(" %s |", record[i].sVal);
//         }
//         }
//         printf("\n");

//     } else {

//       // (all records over)
//       break;
//     }
//   }

//   return SUCCESS;
// }

// select for stage 9
int Algebra::select(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], char attr[ATTR_SIZE], int op, char strVal[ATTR_SIZE])
{
  int srcRelId = OpenRelTable::getRelId(srcRel); // we'll implement this later
  if (srcRelId == E_RELNOTOPEN)
  {
    return E_RELNOTOPEN;
  }

  AttrCatEntry attrCatEntry;
  // get the attribute catalog entry for attr, using AttrCacheTable::getAttrcatEntry()
  //    return E_ATTRNOTEXIST if it returns the error

  int ret = AttrCacheTable::getAttrCatEntry(srcRelId, attr, &attrCatEntry);
  if (ret == E_ATTRNOTEXIST)
    return E_ATTRNOTEXIST;

  /*** Convert strVal (string) to an attribute of data type NUMBER or STRING ***/
  Attribute attrVal;
  int type = attrCatEntry.attrType;

  if (type == NUMBER)
  {
    if (isNumber(strVal))
    { // the isNumber() function is implemented below
      attrVal.nVal = atof(strVal);
    }
    else
    {
      return E_ATTRTYPEMISMATCH;
    }
  }
  else if (type == STRING)
  {
    strcpy(attrVal.sVal, strVal);
  }

  /*** Selecting records from the source relation ***/
  RelCatEntry relCatEntry;
  RelCacheTable::getRelCatEntry(srcRelId, &relCatEntry);
  comparisoncount = 0;
  int src_nAttrs = relCatEntry.numAttrs;

  /* let attr_names[src_nAttrs][ATTR_SIZE] be a 2D array of type char
      (will store the attribute names of rel). */
  // let attr_types[src_nAttrs] be an array of type int

  char attr_names[src_nAttrs][ATTR_SIZE];
  int attr_types[src_nAttrs];

  /*iterate through 0 to src_nAttrs-1 :
      get the i'th attribute's AttrCatEntry using AttrCacheTable::getAttrCatEntry()
      fill the attr_names, attr_types arrays that we declared with the entries
      of corresponding attributes
  */
  for (int i = 0; i < src_nAttrs; i++)
  {
    AttrCatEntry srcattrcatentry;
    AttrCacheTable::getAttrCatEntry(srcRelId, i, &srcattrcatentry);

    strcpy(attr_names[i], srcattrcatentry.attrName);
    attr_types[i] = srcattrcatentry.attrType;
  }
  /* Create the relation for target relation by calling Schema::createRel()
      by providing appropriate arguments */
  // if the createRel returns an error code, then return that value.

  int retval = Schema::createRel(targetRel, src_nAttrs, attr_names, attr_types);
  if (retval != SUCCESS)
  {
    return retval;
  }
  /* Open the newly created target relation by calling OpenRelTable::openRel()
     method and store the target relid */
  /* If opening fails, delete the target relation by calling Schema::deleteRel()
     and return the error value returned from openRel() */
  int targetrelid = OpenRelTable::openRel(targetRel);
  if (targetrelid < 0)
  {
    Schema::deleteRel(targetRel);
    return targetrelid;
  }
  /*** Selecting and inserting records into the target relation ***/
  /* Before calling the search function, reset the search to start from the
     first using RelCacheTable::resetSearchIndex() */

  Attribute record[src_nAttrs];
  /*
      The BlockAccess::search() function can either do a linearSearch or
      a B+ tree search. Hence, reset the search index of the relation in the
      relation cache using RelCacheTable::resetSearchIndex().
      Also, reset the search index in the attribute cache for the select
      condition attribute with name given by the argument `attr`. Use
      AttrCacheTable::resetSearchIndex().
      Both these calls are necessary to ensure that search begins from the
      first record.
  */
  RelCacheTable::resetSearchIndex(srcRelId);
  AttrCacheTable::resetSearchIndex(srcRelId, attr);

  // read every record that satisfies the condition by repeatedly calling
  // BlockAccess::search() until there are no more records to be read

  while (BlockAccess::search(srcRelId, record, attr, attrVal, op) == SUCCESS)
  {

    ret = BlockAccess::insert(targetrelid, record);

    if (ret != SUCCESS)
    {
      //     close the targetrel(by calling
      Schema::closeRel(targetRel);
      //     delete targetrel (by calling
      Schema::deleteRel(targetRel);
      return ret;
    }
  }

  // Close the targetRel by calling
  Schema::closeRel(targetRel);
  printf("number of comparisons: %d\n", comparisoncount);
  return SUCCESS;
}

// will return if a string can be parsed as a floating point number
bool isNumber(char *str)
{
  int len;
  float ignore;
  /*
    sscanf returns the number of elements read, so if there is no float matching
    the first %f, ret will be 0, else it'll be 1

    %n gets the number of characters read. this scanf sequence will read the
    first float ignoring all the whitespace before and after. and the number of
    characters read that far will be stored in len. if len == strlen(str), then
    the string only contains a float with/without whitespace. else, there's other
    characters.
  */
  int ret = sscanf(str, "%f %n", &ignore, &len);
  return ret == 1 && len == strlen(str);
}

int Algebra::insert(char relName[ATTR_SIZE], int nAttrs, char record[][ATTR_SIZE])
{
  // if relName is equal to "RELATIONCAT" or "ATTRIBUTECAT"
  // return E_NOTPERMITTED;
  if (strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0)
  {
    return E_NOTPERMITTED;
  }

  // get the relation's rel-id using OpenRelTable::getRelId() method
  int relId = OpenRelTable::getRelId(relName);

  if (relId == E_RELNOTOPEN)
  {
    return E_RELNOTOPEN;
  }

  // if relation is not open in open relation table, return E_RELNOTOPEN
  // (check if the value returned from getRelId function call = E_RELNOTOPEN)
  // get the relation catalog entry from relation cache
  // (use RelCacheTable::getRelCatEntry() of Cache Layer)
  RelCatEntry relCatEntry;
  RelCacheTable::getRelCatEntry(relId, &relCatEntry);
  if (relCatEntry.numAttrs != nAttrs)
  {
    return E_NATTRMISMATCH;
  }

  // let recordValues[numberOfAttributes] be an array of type union Attribute
  union Attribute recordValues[nAttrs];
  /*
      Converting 2D char array of record values to Attribute array recordValues
   */
  for (int i = 0; i < nAttrs; i++)
  {
    // get the attr-cat entry for the i'th attribute from the attr-cache
    AttrCatEntry attrcatentry;
    AttrCacheTable::getAttrCatEntry(relId, i, &attrcatentry);

    int type = attrcatentry.attrType;

    if (type == NUMBER)
    {
      // if the char array record[i] can be converted to a number
      // (check this using isNumber() function)
      if (isNumber(record[i]) == true)
      {
        recordValues[i].nVal = atof(record[i]);
      }
      else
      {
        return E_ATTRTYPEMISMATCH;
      }
    }
    else if (type == STRING)
    {
      strcpy(recordValues[i].sVal, record[i]);
    }
  }

  // insert the record by calling BlockAccess::insert() function
  // let retVal denote the return value of insert call
  int retVal = BlockAccess::insert(relId, recordValues);

  return retVal;
}
int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE])
{

  int srcRelId = OpenRelTable::getRelId(srcRel);
  if (srcRelId < 0)
    return srcRelId;

  // if srcRel is not open in open relation table, return E_RELNOTOPEN

  // get RelCatEntry of srcRel using RelCacheTable::getRelCatEntry()

  RelCatEntry srcrelcatentry;
  RelCacheTable::getRelCatEntry(srcRelId, &srcrelcatentry);
  int numAttrs = srcrelcatentry.numAttrs;
  // get the no. of attributes present in relation from the fetched RelCatEntry.

  // attrNames and attrTypes will be used to store the attribute names
  // and types of the source relation respectively
  char attrNames[numAttrs][ATTR_SIZE];
  int attrTypes[numAttrs];

  /*iterate through every attribute of the source relation :
      - get the AttributeCat entry of the attribute with offset.
        (using AttrCacheTable::getAttrCatEntry())
      - fill the arrays `attrNames` and `attrTypes` that we declared earlier
        with the data about each attribute
  */

  for (int i = 0; i < numAttrs; i++)
  {
    AttrCatEntry attrcatentry;
    AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrcatentry);
    strcpy(attrNames[i], attrcatentry.attrName);
    attrTypes[i] = attrcatentry.attrType;
  }

  /*** Creating and opening the target relation ***/

  // Create a relation for target relation by calling Schema::createRel()

  int ret = Schema::createRel(targetRel, numAttrs, attrNames, attrTypes);
  if (ret != SUCCESS)
  {
    return ret;
  }
  // if the createRel returns an error code, then return that value.

  // Open the newly created target relation by calling OpenRelTable::openRel()
  // and get the target relid

  int targetrelid = OpenRelTable::openRel(targetRel);
  if (targetrelid < 0)
  {
    Schema::deleteRel(targetRel);
    return targetrelid;
  }
  // If opening fails, delete the target relation by calling Schema::deleteRel() of
  // return the error value returned from openRel().

  /*** Inserting projected records into the target relation ***/

  // Take care to reset the searchIndex before calling the project function
  // using
  RelCacheTable::resetSearchIndex(srcRelId);

  Attribute record[numAttrs];

  while (BlockAccess::project(srcRelId, record) == SUCCESS)
  {
    // record will contain the next record

    ret = BlockAccess::insert(targetrelid, record);

    if (ret != SUCCESS)
    {
      // close the targetrel by calling
      Schema::closeRel(targetRel);
      // delete targetrel by calling
      Schema::deleteRel(targetRel);
      return ret;
    }
  }

  // Close the targetRel by calling
  Schema::closeRel(targetRel);

  return SUCCESS;
}

int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], int tar_nAttrs, char tar_Attrs[][ATTR_SIZE])
{

  int srcRelId = OpenRelTable::getRelId(srcRel);

  if (srcRelId < 0)
  {
    return srcRelId;
  }
  // if srcRel is not open in open relation table, return E_RELNOTOPEN
  RelCatEntry srcRelCatentry;
  RelCacheTable::getRelCatEntry(srcRelId, &srcRelCatentry);
  int numattrs = srcRelCatentry.numAttrs;

  // get the no. of attributes present in relation from the fetched RelCatEntry.

  // declare attr_offset[tar_nAttrs] an array of type int.
  int attr_offset[tar_nAttrs];
  int attr_types[tar_nAttrs];
  // where i-th entry will store the offset in a record of srcRel for the
  // i-th attribute in the target relation.

  // let attr_types[tar_nAttrs] be an array of type int.
  // where i-th entry will store the type of the i-th attribute in the
  // target relation.

  for (int i = 0; i < tar_nAttrs; i++)
  {
    AttrCatEntry attrcatentry;
    int ret = AttrCacheTable::getAttrCatEntry(srcRelId, tar_Attrs[i], &attrcatentry);
    if (ret != SUCCESS)
    {
      return ret;
    }
    attr_offset[i] = attrcatentry.offset;
    attr_types[i] = attrcatentry.attrType;
  }

  /*** Checking if attributes of target are present in the source relation
       and storing its offsets and types ***/

  /*iterate through 0 to tar_nAttrs-1 :
      - get the attribute catalog entry of the attribute with name tar_attrs[i].
      - if the attribute is not found return E_ATTRNOTEXIST
      - fill the attr_offset, attr_types arrays of target relation from the
        corresponding attribute catalog entries of source relation
  */

  /*** Creating and opening the target relation ***/

  // Create a relation for target relation by calling
  int ret = Schema::createRel(targetRel, tar_nAttrs, tar_Attrs, attr_types);
  if (ret != SUCCESS)
  {
    return ret;
  }

  // if the createRel returns an error code, then return that value.

  // Open the newly created target relation by calling OpenRelTable::openRel()
  // and get the target relid

  int targetrelid = OpenRelTable::openRel(targetRel);

  // If opening fails, delete the target relation by calling Schema::deleteRel()
  // and return the error value from openRel()
  if (targetrelid < 0)
  {
    Schema::deleteRel(targetRel);
    return targetrelid;
  }

  /*** Inserting projected records into the target relation ***/

  // Take care to reset the searchIndex before calling the project function
  // using
  RelCacheTable::resetSearchIndex(srcRelId);
  Attribute record[numattrs];

  while (BlockAccess::project(srcRelId, record) == SUCCESS)
  {
    // the variable `record` will contain the next record

    Attribute proj_record[tar_nAttrs];

    // iterate through 0 to tar_attrs-1:
    for (int i = 0; i < tar_nAttrs; i++)
    {
      proj_record[i] = record[attr_offset[i]];
    }

    ret = BlockAccess::insert(targetrelid, proj_record);

    if (ret != SUCCESS)
    {
      // close the targetrel by calling
      Schema::closeRel(targetRel);
      // delete targetrel by calling
      Schema::deleteRel(targetRel);
      return ret;
    }
  }

  // Close the targetRel by calling
  Schema::closeRel(targetRel);

  return SUCCESS;
}

int Algebra::join(char srcRelation1[ATTR_SIZE], char srcRelation2[ATTR_SIZE], char targetRelation[ATTR_SIZE], char attribute1[ATTR_SIZE], char attribute2[ATTR_SIZE])
{

  // get the srcRelation1's rel-id using OpenRelTable::getRelId() method

  relId srcrel1id = OpenRelTable::getRelId(srcRelation1);
  // get the srcRelation2's rel-id using OpenRelTable::getRelId() method
  relId srcrel2id = OpenRelTable::getRelId(srcRelation2);
  // if either of the two source relations is not open
  //     return E_RELNOTOPEN
  if (srcrel1id < 0 || srcrel1id >= MAX_OPEN || srcrel2id < 0 || srcrel2id >= MAX_OPEN)
  {
    return E_RELNOTOPEN;
  }

  AttrCatEntry attrCatEntry1, attrCatEntry2;
  // get the attribute catalog entries for the following from the attribute cache
  // (using AttrCacheTable::getAttrCatEntry())
  // - attrCatEntry1 = attribute1 of srcRelation1
  if (AttrCacheTable::getAttrCatEntry(srcrel1id, attribute1, &attrCatEntry1) == E_ATTRNOTEXIST)
  {
    return E_ATTRNOTEXIST;
  }
  if (AttrCacheTable::getAttrCatEntry(srcrel2id, attribute2, &attrCatEntry2) == E_ATTRNOTEXIST)
  {
    return E_ATTRNOTEXIST;
  }
  // - attrCatEntry2 = attribute2 of srcRelation2

  // if attribute1 is not present in srcRelation1 or attribute2 is not
  // present in srcRelation2 (getAttrCatEntry() returned E_ATTRNOTEXIST)
  //     return E_ATTRNOTEXIST.

  // if attribute1 and attribute2 are of different types return E_ATTRTYPEMISMATCH

  if (attrCatEntry1.attrType != attrCatEntry2.attrType)
  {
    return E_ATTRTYPEMISMATCH;
  }
  // iterate through all the attributes in both the source relations and check if
  // there are any other pair of attributes other than join attributes
  // (i.e. attribute1 and attribute2) with duplicate names in srcRelation1 and
  // srcRelation2 (use AttrCacheTable::getAttrCatEntry())
  // If yes, return E_DUPLICATEATTR
  RelCatEntry relCatEntry1, relCatEntry2;
  RelCacheTable::getRelCatEntry(srcrel1id, &relCatEntry1);
  RelCacheTable::getRelCatEntry(srcrel2id, &relCatEntry2);

  AttrCatEntry temp1, temp2;
  for (int i = 0; i < relCatEntry2.numAttrs; i++)
  {
    if (i == attrCatEntry2.offset)
    {
      continue;
    }
    AttrCacheTable::getAttrCatEntry(srcrel2id, i, &temp2);
    for (int j = 0; j < relCatEntry1.numAttrs; j++)
    {
      AttrCacheTable::getAttrCatEntry(srcrel1id, j, &temp1);
      if (strcmp(temp2.attrName, temp1.attrName) == 0)
      {
        return E_DUPLICATEATTR;
      }
    }
  }

  // get the relation catalog entries for the relations from the relation cache
  // (use RelCacheTable::getRelCatEntry() function)

  int numOfAttributes1 = relCatEntry1.numAttrs;
  int numOfAttributes2 = relCatEntry2.numAttrs;

  // if rel2 does not have an index on attr2
  //     create it using BPlusTree:bPlusCreate()
  //     if call fails, return the appropriate error code
  //     (if your implementation is correct, the only error code that will
  //      be returned here is E_DISKFULL) reset

  if (attrCatEntry2.rootBlock == -1)
  {
    int ret = BPlusTree::bPlusCreate(srcrel2id, attribute2);
    if (ret != SUCCESS)
      return E_DISKFULL;
  }
  int numOfAttributesInTarget = numOfAttributes1 + numOfAttributes2 - 1;
  // declare the following arrays to store the details of the target relation
  char targetRelAttrNames[numOfAttributesInTarget][ATTR_SIZE];
  int targetRelAttrTypes[numOfAttributesInTarget];

  // iterate through all the attributes in both the source relations and
  // update targetRelAttrNames[],targetRelAttrTypes[] arrays excluding attribute2
  // in srcRelation2 (use AttrCacheTable::getAttrCatEntry())

  for (int i = 0; i < numOfAttributes1; i++)
  {
    AttrCacheTable::getAttrCatEntry(srcrel1id, i, &temp1);
    strcpy(targetRelAttrNames[i], temp1.attrName);
    targetRelAttrTypes[i] = temp1.attrType;
  }
  for (int j = 0; j < attrCatEntry2.offset; j++)
  {
    AttrCacheTable::getAttrCatEntry(srcrel2id, j, &temp2);
    strcpy(targetRelAttrNames[numOfAttributes1 + j], temp2.attrName);
    targetRelAttrTypes[numOfAttributes1 + j] = temp2.attrType;
  }

  for (int j = attrCatEntry2.offset + 1; j < numOfAttributes2; j++)
  {
    AttrCacheTable::getAttrCatEntry(srcrel2id, j, &temp2);
    strcpy(targetRelAttrNames[numOfAttributes1 + j - 1], temp2.attrName);
    targetRelAttrTypes[numOfAttributes1 + j - 1] = temp2.attrType;
  }
  // create the target relation using the
  int ret = Schema::createRel(targetRelation, numOfAttributesInTarget, targetRelAttrNames, targetRelAttrTypes);
  if (ret != SUCCESS)
  {
    return ret;
  }
  // if createRel() returns an error, return that error

  // Open the targetRelation using
  int targetrelid = OpenRelTable::openRel(targetRelation);
  if (targetrelid < 0 || targetrelid >= MAX_OPEN)
  {
    Schema::deleteRel(targetRelation);
    return targetrelid;
  }

  Attribute record1[numOfAttributes1];
  Attribute record2[numOfAttributes2];
  Attribute targetRecord[numOfAttributesInTarget];

  // this loop is to get every record of the srcRelation1 one by one
  while (BlockAccess::project(srcrel1id, record1) == SUCCESS)
  {

    // reset the search index of `srcRelation2` in the relation cache
    // using
    RelCacheTable::resetSearchIndex(srcrel2id);
    // reset the search index of `attribute2` in the attribute cache
    // using
    AttrCacheTable::resetSearchIndex(srcrel2id, attribute2);

    // this loop is to get every record of the srcRelation2 which satisfies
    // the following condition:
    // record1.attribute1 = record2.attribute2 (i.e. Equi-Join condition)
    while (BlockAccess::search(srcrel2id, record2, attribute2, record1[attrCatEntry1.offset], EQ) == SUCCESS)
    {

      // copy srcRelation1's and srcRelation2's attribute values(except
      // for attribute2 in rel2) from record1 and record2 to targetRecord
      for (int i = 0; i < numOfAttributes1; i++)
      {
        targetRecord[i] = record1[i];
      }
      for (int i = 0; i < attrCatEntry2.offset; i++)
      {
        targetRecord[numOfAttributes1 + i] = record2[i];
      }
      for (int i = attrCatEntry2.offset + 1; i < numOfAttributes2; i++)
      {
        targetRecord[numOfAttributes1 + i - 1] = record2[i];
      }
      ret = BlockAccess::insert(targetrelid, targetRecord);
      if (ret != SUCCESS)
      {
        OpenRelTable::closeRel(targetrelid);
        Schema::deleteRel(targetRelation);
        return E_DISKFULL;
      }
    }
  }
  OpenRelTable::closeRel(targetrelid);
  return SUCCESS;
}