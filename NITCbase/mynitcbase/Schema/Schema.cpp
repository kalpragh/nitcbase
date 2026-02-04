#include "Schema.h"

#include <cmath>
#include <cstring>
int Schema::openRel(char relName[ATTR_SIZE]) {
  int ret = OpenRelTable::openRel(relName);

  // the OpenRelTable::openRel() function returns the rel-id if successful
  // a valid rel-id will be within the range 0 <= relId < MAX_OPEN and any
  // error codes will be negative
  if(ret >= 0){
    return SUCCESS;
  }

  //otherwise it returns an error message
  return ret;
}

int Schema::closeRel(char relName[ATTR_SIZE]) {
  if (strcmp(relName,RELCAT_RELNAME)==0 || strcmp(relName,ATTRCAT_RELNAME)==0) {
    return E_NOTPERMITTED;
  }

  // this function returns the rel-id of a relation if it is open or
  // E_RELNOTOPEN if it is not. we will implement this later.
  int relId = OpenRelTable::getRelId(relName);

  if (relId<0) {
    return E_RELNOTOPEN;
  }

  return OpenRelTable::closeRel(relId);
}
int renameRel(char oldRelName[ATTR_SIZE], char newRelName[ATTR_SIZE]) {
    if(strcmp(oldRelName,RELCAT_RELNAME)==0 ||strcmp(oldRelName,ATTRCAT_RELNAME)==0 || strcmp(newRelName,RELCAT_RELNAME)==0 ||strcmp(newRelName,ATTRCAT_RELNAME)==0){
      return E_NOTPERMITTED;
    }
    int relid=OpenRelTable::getRelId(oldRelName);
    if(relid!=E_RELNOTOPEN){
      return E_RELOPEN;
    }
    return BlockAccess::renameRelation(oldRelName, newRelName);
}
int Schema::renameAttr(char *relName, char *oldAttrName, char *newAttrName) {
    // if the relName is either Relation Catalog or Attribute Catalog,
        // return E_NOTPERMITTED
        // (check if the relation names are either "RELATIONCAT" and "ATTRIBUTECAT".
        // you may use the following constants: RELCAT_RELNAME and ATTRCAT_RELNAME)
    if(strcmp(relName,RELCAT_RELNAME)==0 ||strcmp(relName,ATTRCAT_RELNAME)==0 ){
        return E_NOTPERMITTED;
      }
    int relid=OpenRelTable::getRelId(relName);
    if(relid!=E_RELNOTOPEN){
      return E_RELOPEN;
    }
    return BlockAccess::renameAttribute(relName,oldAttrName,newAttrName);

}