#ifndef Schema_H
#define Schema_H

#include <iostream>
#include <fstream>
#include <map>
#include <algorithm>
#include "util.h"

using namespace std;

enum SchemaType {
    INT32,
    INT64,
    CHAR,
    FLOAT,
    DOUBLE,
    FOREIGN_KEY
    // BOOLEAN
};

struct SchemaCol {
    string key;
    SchemaType type;
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
 * The 'person' is a table and the foreign_key states that ONLY ONE _id will be stored on 
 * the person table (one-to-one relation)
 *
 * Note that in order to make a one-to-many or many-to-many relashionsip,
 * a junction table should be used. @See https://en.wikipedia.org/wiki/Associative_entity 
 * 
 * e.g.:
 * actor table
 * | _id:int64 (primary key) | name:char:255 |
 * 
 * film table
 * | _id:int64 (primary key) | title:char:255 | genre:char:255 |
 *
 * actor_film_mapping table
 * | _id:int64 (primary key) | actor_id:foreign_key | film_id:foreign_key |
 */
class Schema {
private:
    vector<SchemaCol> cols;
    unsigned size;
    
public:
    /**
     *@constructor
     */
    Schema();
    
    /**
    * Import a schema file, where each line is defined by <key>:<type>:<optional_array_size>
    * e.g.:
    * name:char:255 is a char *[255]
    * name:int32 is an int
    */
    void import(const string & path);
    
    /**
     * Get the schema columns
     * @return the columns vector
     */
     vector<SchemaCol> * getCols();
     
     /**
      * Add a column
      */
     void addCol(string key, SchemaType type);
     void addCol(string key, SchemaType type, unsigned array_size);
      
     /**
      * @return the number of columns on the schema
      */
     int getNumberOfCols();
     
     /**
      * e.g.: If there are two columns, a char [255] and a float, the
      * method will return sizeof(char) * 255 + sizeof(float)
      * @return the total size of the schema
      */
      unsigned getSize();
      int getColPosition(string column_name);
};

Schema::Schema() {
    size = -1;
    SchemaCol _id;
    _id.key = "_id";
    _id.type = INT64;
    _id.array_size = 0;
    
    cols.push_back(_id);
}

void Schema::import(const string & path) {
    ifstream file;
    file.open(path.c_str());
    string line;
    
    if (file.is_open()) {
        //Lines
        while (getline(file, line)) {
            vector<string> words = split(line, ':');
            SchemaCol col;
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

vector<SchemaCol> * Schema::getCols() {
    return &cols;
}

void Schema::addCol(string key, SchemaType type) {
    addCol(key, type, 0);
}

void Schema::addCol(string key, SchemaType type, unsigned array_size) {
    SchemaCol col;
    col.key = key;
    col.type = type;
    col.array_size = array_size;
    cols.push_back(col);
}

unsigned Schema::getSize() {
    if (size == -1) {
        // Get the size for the first time
        size = 0;
        for (vector<SchemaCol>::iterator it = cols.begin(); it != cols.end(); it++) {
            size += (*it).getSize();
        }
    }
    
    return size;
}

int Schema::getNumberOfCols() {
    return cols.size();
}

int Schema::getColPosition(string column_name){
    int col_position = 0;
    for(col_position; col_position < size; col_position++){
        if(cols.at(col_position).key == column_name)
            return col_position;
    }

    return -1;

}
 
 #endif //Schema_H
