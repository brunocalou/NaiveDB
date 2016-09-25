#ifndef TABLE_H
#define TABLE_H

#include "util.h"
#include "scheme.h"
#include <fstream>
#include <time.h>
#include <string.h>

struct RegistryHeader {
    char table_name[255];
    unsigned registry_size; // size of the registry, header included
    time_t time_stamp;
};

class Table {
private:
    static const unsigned HEADER_SIZE = sizeof(RegistryHeader);
    
    /**
     * Hashmap to store the table. It's used only when importing or exporting the
     * whole table
     * @see Table::importCSV, Table::exportBin, Table::importBin
     */
    map<string, vector<string> > table;
    Scheme scheme;
    string name;
    string path;
    
    /**
     * Save a value to the file using the correct type and size. The file
     * will not be opened nor closed inside this method
     */
    void convertAndSave(ofstream *file, string * value, SchemeCol * scheme_col);
    
public:

    /**
     * @constructor
     */
    Table(string name, string table_path);

    /**
    * Import a CSV file and convert it to a map of vectors. The first element
    * is the map key and the others are the elements of the vector. Note that
    * the table will be stored in memory
    */
    void importCSV(const string & path);
    
    /**
     * Import the scheme using the Scheme standard method
     */
    void importScheme(const string & path);
    
    /**
     * Set the scheme to be used
     * @see Scheme
     */
    void setScheme(Scheme scheme);
    
    /**
     * Import a table from a binary file to the memory. The binary file must have been generated
     * using the exportBin method. Note that all the table will be stored in memory
     * @see Table::exportBin
     */
    void importBin();
    
    /**
     * Export a table to a binary file. The header is exported for each registry
     * e.g.: | HEADER | ROW_1_COL_1 | ROW_1_COL_2 | HEADER | ROW_2_COL1 | ROW_2_COL_2
     * The HEADER is saved as | TABLE_NAME | REGISTRY_SIZE | TIME_STAMP |
     * Note that the data must have been imported from a CSV file for this method run
     * correctly because operations like Table::insert will not affect the Table::table variable
     * @see Table::importCSV
     * @return true if the export has succeeded, false otherwise
     */
    bool exportBin();
    
    /**
     * Performs a query
     */
     bool insert(string key, vector<string> row);
};

Table::Table(string name, string table_path) {
    this->name = name;
    this->path = table_path;
}

void Table::importCSV(const string & path) {
    string line;
    
    ifstream file;
    file.open(path.c_str());
    
    
    if (file.is_open()) {
        //Header
        getline(file, line);
        
        //Lines
        while (getline(file, line)) {
            vector<string> words = split(line, ',');
            string key = words.at(0);
            
            //Erase the key from the vector
            words.erase(words.begin());
            
            //Add the vector to the map
            table[key] = words; 
        }
        file.close();
    } else {
        cout << "Unable to open file - " << path << endl;
    }
    
    for (std::map<string, vector<string> >::iterator it = table.begin(); it!=table.end(); ++it) {
        std::cout << it->first << endl;
        
        vector<string> cols = it->second;
        
        for (std::vector<string>::iterator v_it = cols.begin(); v_it != cols.end(); v_it++) {
            cout << "  " << *v_it;
        }
        cout << endl;
    }
}

void Table::importScheme(const string & path) {
    scheme.import(path);
}

void Table::setScheme(Scheme scheme) {
    this->scheme = scheme;
}

void Table::importBin() {
    ifstream file;
    file.open(path.c_str(), ios::binary);
    
    cout << "Imported" << endl;
    
    vector<SchemeCol>* scheme_cols = scheme.getCols();
    
    string key;
    
    while (!file.eof()) {
        //Import the header
        RegistryHeader header;
        file.read(header.table_name, sizeof(header.table_name));
        file.read(reinterpret_cast<char *> (& header.registry_size), sizeof(header.registry_size));
        file.read(reinterpret_cast<char *> (& header.time_stamp), sizeof(header.time_stamp));
        
        cout << "  | " << header.table_name << " " << header.registry_size << " " << header.time_stamp << " | ";
    
        //Read and convert the values from the file
        for (vector<SchemeCol>::iterator it = scheme_cols->begin(); it != scheme_cols->end(); it++) {
            SchemeCol & scheme_col = *it;
            ostringstream stream;
            
            if (scheme_col.type == INT32) {
                int value;
                file.read(reinterpret_cast<char *> (&value), scheme_col.getSize());
                cout << "INT32 " << value << " | ";
                stream << value;
            } else if (scheme_col.type == CHAR) {
                char value[scheme_col.getSize()];
                file.read(reinterpret_cast<char *> (&value), scheme_col.getSize());
                cout << "CHAR " << value << " | ";
                stream << value;
            } else if (scheme_col.type == FLOAT) {
                float value;
                file.read(reinterpret_cast<char *> (&value), scheme_col.getSize());
                cout << "FLOAT " << value << " | ";
                stream << value;
            } else if (scheme_col.type == DOUBLE) {
                double value;
                file.read(reinterpret_cast<char *> (&value), scheme_col.getSize());
                cout << "DOUBLE " << value << " | ";
                stream << value;
            }
            
            string string_value;
            string_value = stream.str();
            
            //Add the values to the table
            if (it == scheme_cols->begin()) {
                key = string_value;
                vector<string> vec;
                table[key] = vec;
            } else {
                table[key].push_back(string_value);
            }
        }
        
        //TODO: find where the bug is
        //the last element is garbage, for some reason. So, delete it
        // table.erase(key);
        cout << endl;
    }
    
    file.close();
}

void Table::convertAndSave(ofstream *file, string * string_value, SchemeCol *scheme_col) {
    if (scheme_col->type == INT32) {
        int value = atoi((*string_value).c_str());
        file->write(reinterpret_cast<char *> (&value), scheme_col->getSize());
        cout << "INT32 " << value << "(" << *string_value << ")" << " | ";
    } else if (scheme_col->type == CHAR) {
        char value[scheme_col->getSize()];
        strncpy(value, &string_value->c_str()[0], scheme_col->getSize());
        file->write(reinterpret_cast<char *> (&value), scheme_col->getSize());
        cout << "CHAR " << value << "(" << *string_value << ")" << " | ";
    } else if (scheme_col->type == FLOAT) {
        float value = atof((*string_value).c_str());
        file->write(reinterpret_cast<char *> (&value), scheme_col->getSize());
        cout << "FLOAT " << value << "(" << *string_value << ")" << " | ";
    } else if (scheme_col->type == DOUBLE) {
        double value = atof((*string_value).c_str());
        file->write(reinterpret_cast<char *> (&value), scheme_col->getSize());
        cout << "DOUBLE " << value << "(" << *string_value << ")" << " | ";
    }
}

bool Table::exportBin() {
    cout << "Exported " << endl;
    
    for (map<string, vector<string> >::iterator table_it = table.begin(); table_it != table.end(); table_it ++) {
        //Iterate through the map
        string key = table_it->first;
        vector <string> & row = table_it->second;
        
        insert(key, row);
    }
    
    return true;
}

bool Table::insert(string key, vector<string> row) {
    //TODO: create a insert method that receives the file as parameter to improve the performance while adding many rows
    ofstream file;
    file.open(path.c_str(), ios::binary | ios::app);
    
    // table[key] = row;
    
    //Save the header
    RegistryHeader header;
    strncpy(header.table_name, &name.c_str()[0], sizeof(header.table_name));
    header.registry_size = HEADER_SIZE + scheme.getSize();
    time (& header.time_stamp);
    
    file.write(header.table_name, sizeof(header.table_name));
    file.write(reinterpret_cast<char *> (& header.registry_size), sizeof(header.registry_size));
    file.write(reinterpret_cast<char *> (& header.time_stamp), sizeof(header.time_stamp));
    
    cout << "  | " << header.table_name << " " << header.registry_size << " " << header.time_stamp << " | ";
    
    //Export the table according to the scheme
    vector<SchemeCol>* scheme_cols = scheme.getCols();
    
    //Save the key
    convertAndSave(&file, &key, &scheme_cols->at(0));
    
    int scheme_col_position = 1;
    
    for (vector<string>::iterator row_it = row.begin(); row_it != row.end(); row_it++) {
        //Iterate through the row and save the values
        //TODO: Consider the array size
        convertAndSave(&file, &(*row_it), &scheme_cols->at(scheme_col_position));
        scheme_col_position ++;
    }
    cout << endl;
    
    file.close();
    
    return true;
}

#endif //TABLE_H