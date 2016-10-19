#ifndef SCHEME_H
#define SCHEME_H

#include <iostream>
#include <fstream>
#include <map>
#include <algorithm>
#include "util.h"

using namespace std;

enum SchemeType {
    INT32,
    INT64,
    CHAR,
    FLOAT,
    DOUBLE,
    FOREIGN_KEY
    // BOOLEAN
};

struct SchemeCol {
    string key;
    SchemeType type;
    unsigned array_size;
    
    unsigned getSize() {
        switch (type) {
            case INT32:
            case FLOAT:
                return sizeof(float) * (array_size + 1);
            case INT64:
            case FOREIGN_KEY:
            case DOUBLE:
                return sizeof(double) * (array_size + 1);
            // case BOOLEAN:
            //     return sizeof(bool) * (array_size + 1);
            case CHAR:
                return sizeof(char) * (array_size + 1);
            default:
                return 0;
        }
    }
};

/**
 * Defines the structure of a table row. Note that the _id is inserted automatically and
 * it's used as the primary key
 * e.g.:
 * | _id:int64 (primary key) | name:char:255 | lastname:char:255 | age:int32 |
 * 
 * When using a foreign key, the first attribute is the name of the table
 * e.g.:
 * | _id:int64 (primary key) | telephone_number:int64 | person:foreign_key |
 *
 * The 'person' is a table and the foreign_key states that ONLY ONE _id on 
 * the person table will be stored (one-to-one relation)
 *
 * OBS: One-to-many relation is NOT SUPPORTED yet
 */
class Scheme {
private:
    vector<SchemeCol> cols;
    unsigned size;
    
public:
    /**
     *@constructor
     */
    Scheme();
    
    /**
    * Import a scheme file, where each line is defined by <key>:<type>:<optional_array_size>
    * e.g.:
    * name:char:255 is a char *[255]
    * name:int32 is an int
    */
    void import(const string & path);
    
    /**
     * Get the scheme columns
     * @return the columns vector
     */
     vector<SchemeCol> * getCols();
     
     /**
      * Add a column
      */
     void addCol(string key, SchemeType type);
     void addCol(string key, SchemeType type, unsigned array_size);
      
     /**
      * @return the number of columns on the scheme
      */
     int getNumberOfCols();
     
     /**
      * e.g.: If there are two columns, a char [255] and a float, the
      * method will return sizeof(char) * 255 + sizeof(float)
      * @return the total size of the scheme
      */
      unsigned getSize(); 
};

Scheme::Scheme() {
    size = -1;
    SchemeCol _id;
    _id.key = "_id";
    _id.type = INT64;
    _id.array_size = 0;
    
    cols.push_back(_id);
}

void Scheme::import(const string & path) {
    ifstream file;
    file.open(path.c_str());
    string line;
    
    if (file.is_open()) {
        //Lines
        while (getline(file, line)) {
            vector<string> words = split(line, ':');
            SchemeCol col;
            unsigned size = words.size();
            
            if (size > 0) {
                //Key
                col.key = words.at(0);
                
                //Type
                string type = words.at(1);
                
                if (type == "int32") {
                    col.type = INT32;
                } else if (type == "char") {
                    col.type = CHAR;
                } else if (type == "float") {
                    col.type = FLOAT;
                } else if (type == "double") {
                    col.type = DOUBLE;
                } else if (type == "foreign_key") {
                    col.type = FOREIGN_KEY;
                }
                // else if (type == "boolean") {
                //     col.type = BOOLEAN;
                // }
                
                //Array size
                if (size == 3) {
                    col.array_size = atoi(words.at(2).c_str());
                } else {
                    col.array_size = 0;
                }
                
                //Push the column to the cols vector
                cout << col.key << " " << col.type << " " << col.array_size << endl;
                cols.push_back(col);
            }
        }
        file.close();
    } else {
        cout << "Unable to open file - " << path << endl;
    }
    
}

vector<SchemeCol> * Scheme::getCols() {
    return &cols;
}

void Scheme::addCol(string key, SchemeType type) {
    addCol(key, type, 0);
}

void Scheme::addCol(string key, SchemeType type, unsigned array_size) {
    SchemeCol col;
    col.key = key;
    col.type = type;
    col.array_size = array_size;
    cols.push_back(col);
}

unsigned Scheme::getSize() {
    if (size == -1) {
        // Get the size for the first time
        size = 0;
        for (vector<SchemeCol>::iterator it = cols.begin(); it != cols.end(); it++) {
            size += (*it).getSize();
        }
    }
    
    return size;
}

int Scheme::getNumberOfCols() {
    return cols.size();
}
 
 #endif //SCHEME_H