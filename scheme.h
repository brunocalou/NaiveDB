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
    CHAR,
    FLOAT,
    DOUBLE//,
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
      * e.g.: If there are two columns, a char [255] and a float, the
      * method will return sizeof(char) * 255 + sizeof(float)
      * @return the total size of the scheme
      */
      unsigned getSize(); 
};

Scheme::Scheme() {
    size = -1;
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
 
 #endif //SCHEME_H