#ifndef QUERYABLE_H
#define QUERYABLE_H

#include "schema.h"

/**
 * Stores the header of a registry. The header is saved for each registry
 * e.g: | HEADER | ROW_1_COL_1 | ROW_1_COL_2 | HEADER | ROW_2_COL1 | ROW_2_COL_2 | 
 */
struct RegistryHeader {
    char table_name[255];
    unsigned registry_size; // size of the registry, header included
    time_t time_stamp;
};

/**
 * Stores the position of every RegistryHeader of a table
 * e.g: for a database like | HEADER | 64_BITS_BODY | HEADER | 32_BITS_BODY | HEADER | ...
 *      the Header file will be like | ID_0 | 0 | ID_1 | 64 + HEADER_SIZE | ID_2 | 64 + 32 + 2 * HEADER_SIZE | ...
 * Note that the header file contains registry_positions with FIXED size, so each position is
 * stored by the same amount of bits (in this case, long long (64 bits))
 */
struct HeaderFile {
    long long _id;
    long long registry_position;
    string path;
};

typedef vector<pair<decltype(HeaderFile::_id), decltype(HeaderFile::registry_position)> > header_t;

class Queryable {
public:
  virtual vector<string> getRow(long long registry_position) =0;
  virtual vector<string> getRowById(long long _id) =0;
  virtual Schema getSchema() =0;
  virtual header_t* getHeader() =0;
};

#endif 