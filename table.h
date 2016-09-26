#ifndef TABLE_H
#define TABLE_H

#include "util.h"
#include "scheme.h"
#include "cursor.h"
#include "tablebenchmark.h"
#include <fstream>
#include <time.h>
#include <string.h>
#include <algorithm>
#include <utility> //std::pair

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

class Table {
private:
    unsigned HEADER_SIZE;

    Scheme scheme;
    string name;
    string path;
    string header_file_path;
    vector<pair<decltype(HeaderFile::_id), decltype(HeaderFile::registry_position)> > * header; // _id, registry_position
    
    friend class TableBenchmark;
    
    /**
     * Save a value to the file using the correct type and size. The file
     * will not be opened nor closed inside this method
     */
    void convertAndSave(ofstream *file, string * value, SchemeCol * scheme_col);
     
    /**
     * Inserts the registry_position on the header file. The insertion will
     * append the new registry_position to the end of the header file and will
     * update the header variable
     */
    bool insertOnHeaderFile(HeaderFile * header_file);
     
    /**
     * Load the table header from the memory
     */
    void loadHeader();
    
public:

    /**
     * The constructor loads the table header from the memory, if any
     * @constructor
     * @see Table::loadHeader
     */
    Table(string name);
    
    /**
     * @destructor
     */
    ~Table();
    
    /**
     * Import the scheme using the Scheme standard method
     */
    void importScheme(const string & path);
    
    /**
     * Set the scheme to be used
     * @see Scheme
     */
    void setScheme(Scheme scheme);
    
    /*****************************************
     ************* QUERY METHODS *************
     *****************************************/
    
    /**
     * Insert a row at the end of the table. Note that no consistency test
     * is made to ensure uniqueness of any element and the primary key is managed
     * internally (auto increment)
     * @param row - the table row (primary key not included). Note that
     *              the order of elements is important and it's expected
     *              to be the same as the order on the scheme
     * @return the _id of the inserted item (used as primary key)
     */
    int insert(vector<string> row);
     
    /**
     * Perform a query. Note that the string is case insensitive and the FROM clause is omitted
     * because the FROM is for the table instance.
     * Supported arguments: SELECT, *, WHERE, =, <, >, <=, >=, !=
     * e.g.: query("SELECT * WHERE _id=123") -> returns the only row where the _id is equals to 123
     * e.g.2: query("select name, age where age > 10 and name='bruno'") -> returns the name and age where
     *        the age > 10 and the name is equal to bruno (case insensitive)
     * e.g.3: query("SELECT *") -> returns all the columns
     * @param q - the query on a raw string format
     * @return the cursor associated with the query
     */
    Cursor query(string q);
     
    /**
     * Perform a query
     * @see Table::query(string)
     * @return the cursor associated with the query
     */
    Cursor query(
            vector<string> & select,
            vector<string> & where_args,
            vector<string> & where_comparators,
            vector<string> & where_values);
     
    /*****************************************
     ********** CONVENIENCE METHODS **********
     *****************************************/
     
    /**
     * Read a CSV file, convert and export it to a binary file
     */
    void convertFromCSV(const string & path);
    
    /**
     * Print the table binary file (for debugging only)
     * @param number_of_values the number of values to print. If set to -1
     *        all the file wil be printed
     */
    void print(int number_of_values = -1);
    
    /**
     * Print the header file (for debugging only)
     * @param number_of_values the number of values to print. If set to -1
     *        all the file wil be printed
     */
    void printHeaderFile(int number_of_values = -1);
};

Table::Table(string name) {
    this->name = name;
    this->path = name + ".dat";
    this->header_file_path = name + "_h.dat";
    this->header = new vector<pair<decltype(HeaderFile::_id), decltype(HeaderFile::registry_position)> >();
    loadHeader();
    
    RegistryHeader reg_header;
    Table::HEADER_SIZE = sizeof(reg_header.table_name) + sizeof(reg_header.registry_size) + sizeof(reg_header.time_stamp);
    cout << "HEADER_SIZE = " << HEADER_SIZE << endl;
    
}

Table::~Table() {
    delete this->header;
}

void Table::importScheme(const string & path) {
    scheme.import(path);
}

void Table::setScheme(Scheme scheme) {
    this->scheme = scheme;
}

void Table::loadHeader() {
    ifstream file;
    file.open(header_file_path.c_str(), ios::binary);
    
    while (file.good()) {
        HeaderFile header;
        file.read(reinterpret_cast<char *> (&header._id), sizeof(header._id));
        file.read(reinterpret_cast<char *> (&header.registry_position), sizeof(header.registry_position));
        this->header->push_back(pair<decltype(header._id), decltype(header.registry_position) > (header._id, header.registry_position));
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
    } else if (scheme_col->type == INT64) {
        long long value = std::stoll((*string_value).c_str());
        file->write(reinterpret_cast<char *> (&value), scheme_col->getSize());
        cout << "INT64 " << value << "(" << *string_value << ")" << " | ";
    }
}

int Table::insert(vector<string> row) {
    //TODO: create a insert method that receives the file as parameter to improve the performance while adding many rows
    //TODO: Handle exceptions and return 0 on failure
    ofstream file;
    file.open(path.c_str(), ios::binary | ios::app);
    
    //Save the header position on the header file
    HeaderFile header_file;
    header_file.path = this->header_file_path;
    header_file._id = this->header->size();
    header_file.registry_position = file.tellp(); // Get the current position on the file stream
    insertOnHeaderFile(&header_file);
    
    //Save the header
    RegistryHeader header;
    strncpy(header.table_name, &name.c_str()[0], sizeof(header.table_name));
    header.registry_size = HEADER_SIZE + scheme.getSize();
    time (& header.time_stamp);
    
    file.write(header.table_name, sizeof(header.table_name));
    file.write(reinterpret_cast<char *> (& header.registry_size), sizeof(header.registry_size));
    file.write(reinterpret_cast<char *> (& header.time_stamp), sizeof(header.time_stamp));
    
    cout << "  | " << header.table_name << " " << header.registry_size << " " << header.time_stamp << " | ";
    
    //Push the _id to the row
    string _id_str;
    
    std::stringstream strstream;
    strstream << header_file._id;
    strstream >> _id_str;
    
    //Insert the _id on the first position so it matches the SchemeCol
    row.insert(row.begin(), _id_str);
    
    //Export the table according to the scheme
    vector<SchemeCol>* scheme_cols = scheme.getCols();
    
    int scheme_col_position = 0;
    
    for (vector<string>::iterator row_it = row.begin(); row_it != row.end(); row_it++) {
        //Iterate through the row and save the values
        //TODO: Consider the array size
        convertAndSave(&file, &(*row_it), &scheme_cols->at(scheme_col_position));
        scheme_col_position ++;
    }
    cout << endl;
    
    file.close();
    
    return 1;
}

bool Table::insertOnHeaderFile(HeaderFile * header_file) {
    ofstream file;
    file.open(header_file->path.c_str(), ios::binary | ios::app);
    file.write(reinterpret_cast<char *> (& header_file->_id), sizeof(header_file->_id));
    file.write(reinterpret_cast<char *> (& header_file->registry_position), sizeof(header_file->registry_position));
    
    header->push_back(
        pair<decltype(header_file->_id), decltype(header_file->registry_position)> (
            header_file->_id,
            header_file->registry_position));
    
    file.close();
}

void Table::printHeaderFile(int number_of_values) {
    ifstream file;
    file.open(header_file_path.c_str(), ios::binary);
    int counter = 0;
    
    while (file.good() && counter != number_of_values) {
        HeaderFile header;
        file.read(reinterpret_cast<char *> (&header._id), sizeof(header._id));
        file.read(reinterpret_cast<char *> (&header.registry_position), sizeof(header.registry_position));
        cout << header._id << " " << header.registry_position << endl;
        counter ++;
    }
    
    file.close();
}

void Table::print(int number_of_values) {
    ifstream file;
    file.open(path.c_str(), ios::binary);
    
    cout << "Print" << endl;
    
    vector<SchemeCol>* scheme_cols = scheme.getCols();
    int counter = 0;
    
    while (file.good() && counter != number_of_values) {
        //Import the header
        RegistryHeader header;
        file.read(header.table_name, sizeof(header.table_name));
        file.read(reinterpret_cast<char *> (& header.registry_size), sizeof(header.registry_size));
        file.read(reinterpret_cast<char *> (& header.time_stamp), sizeof(header.time_stamp));
        
        cout << "  | " << header.table_name << " " << header.registry_size << " " << header.time_stamp << " | ";
    
        //Read and convert the values from the file
        for (vector<SchemeCol>::iterator it = scheme_cols->begin(); it != scheme_cols->end(); it++) {
            SchemeCol & scheme_col = *it;
            // ostringstream stream;
            
            if (scheme_col.type == INT32) {
                int value;
                file.read(reinterpret_cast<char *> (&value), scheme_col.getSize());
                cout << "INT32 " << value << " | ";
                // stream << value;
            } else if (scheme_col.type == CHAR) {
                char value[scheme_col.getSize()];
                file.read(reinterpret_cast<char *> (&value), scheme_col.getSize());
                cout << "CHAR " << value << " | ";
                // stream << value;
            } else if (scheme_col.type == FLOAT) {
                float value;
                file.read(reinterpret_cast<char *> (&value), scheme_col.getSize());
                cout << "FLOAT " << value << " | ";
                // stream << value;
            } else if (scheme_col.type == DOUBLE) {
                double value;
                file.read(reinterpret_cast<char *> (&value), scheme_col.getSize());
                cout << "DOUBLE " << value << " | ";
                // stream << value;
            }  else if (scheme_col.type == INT64) {
                long long value;
                file.read(reinterpret_cast<char *> (&value), scheme_col.getSize());
                cout << "INT64 " << value << " | ";
                // stream << value;
            }
            
            // string string_value;
            // string_value = stream.str();
        }
        cout << endl;
        counter ++;
    }
    
    file.close();
}

Cursor Table::query(string q) {
    //Transform the query to lower case
    std::transform(q.begin(), q.end(), q.begin(), ::tolower);
    
    //Store the select arguments.
    //e.g.: SELECT arg1, arg2
    vector<string> select;
    
    //Store the where arguments, values and comparators
    //e.g.: WHERE arg1 = val1, arg2 > val2,
    vector<string> where_args;
    vector<string> where_comparators;
    vector<string> where_vals;
    
    //Helper variables to store the parsing state
    bool parsing_select = false;
    bool parsing_where = false;
    
    //Hold if a word is beeing parsed at the moment. If a space ' ' or a comma ',' is
    //parsed, the variable is set false
    bool parsing_word = false;
    
    bool parsing_where_arg = false;
    bool parsing_where_comparator = false;
    bool parsing_where_val = false;
    
    string string_buffer;
    
    bool ignore_space = false;
    
    for (string::iterator it = q.begin(); it != q.end(); it++) {
        char character = (*it);
        
        if (character == '\'') {
            ignore_space = !ignore_space;
            continue;
            
        } else if (character != ' ' || character != ',') {
            parsing_word = true;
            
        } else {
            if (character == ' ' && ignore_space) {
                parsing_word = true;
            } else {
                parsing_word = false;
            }
        }
        
        // Check for the select word
        if (!parsing_select && !parsing_where) {
            if (character != ' ') {
                string_buffer += character;
            }
            // cout << string_buffer << endl;
            if (string_buffer == "select") {
                parsing_select = true;
                string_buffer.clear();
            } else if (string_buffer == "where") {
                cout << "Changing to WHERE" << endl;
                parsing_where = true;
                parsing_where_arg = true;
                parsing_select = false;                
                string_buffer.clear();
            }
        } else if (character != ' ' || ignore_space) {
            if (parsing_select) {
                //Ignore spaces
                if (character == ',') {
                    //Add the string_buffer to the where_args
                    where_args.push_back(string_buffer);
                    cout << string_buffer << endl;
                    string_buffer.clear();
                } else {
                    //If a word is beeing parsed, add the character to the string buffer,
                    //if not, there are two words separated by a space. In this case, the
                    //select arguments are finished
                    if (parsing_word) {
                        string_buffer += character;
                    } else {
                        parsing_select = false;
                        where_args.push_back(string_buffer);
                        cout << string_buffer << endl;
                        string_buffer.clear();
                    }
                }
                
            } else if (parsing_where) {
                if (character == ',') {
                    if (parsing_where_val) {
                        where_vals.push_back(string_buffer);
                        cout << "Where value = " << string_buffer << endl;
                        string_buffer.clear();
                    }
                    parsing_where_arg = true;
                    parsing_where_comparator = false;
                    parsing_where_val = false;
                } else if (parsing_word) {
                    if (parsing_where_arg) {
                        if (character == '=' || character == '<' || character == '>' || character == '!') {
                            //The argument was parsed, move to the comparator
                            parsing_where_arg = false;
                            parsing_where_comparator = true;
                            where_args.push_back(string_buffer);
                            cout << "Where arg = " << string_buffer << endl;
                            string_buffer.clear();
                        }
                    } else if (parsing_where_comparator) {
                        if (character != '=' || character != '<' || character != '>' || character != '!') {
                            //The comparator was parsed, move to the where value
                            parsing_where_comparator = false;
                            parsing_where_val = true;
                            where_comparators.push_back(string_buffer);
                            cout << "Where comparator = " << string_buffer << endl;
                            string_buffer.clear();
                        }
                    }
                    
                    string_buffer += character;
                }
            }
        } else if (character == ' ') {
            if (!string_buffer.empty()) {
                if (parsing_select) {
                    select.push_back(string_buffer);
                    cout << "F Select = " << string_buffer << endl;
                    string_buffer.clear();
                    parsing_select = false;
                }
            }
        }
    }
    if (parsing_select) {
        select.push_back(string_buffer);
        cout << "Final select = " << string_buffer << endl;
        
    } else if (parsing_where && parsing_where_val) {
        where_vals.push_back(string_buffer);
        cout << "Final where value = " << string_buffer << endl;
    }
    
    return query(select, where_args, where_comparators, where_vals);
}

Cursor Table::query(vector<string> & select, vector<string> & where_args, vector<string> & where_comparators, vector<string> & where_values) {
    //Store the query result
    vector<vector <string> > result;
    
    //TODO: Make the query method
    
    Cursor cursor(scheme, result);
    return cursor;
}

void Table::convertFromCSV(const string & path) {
    string line;
    
    ifstream file;
    file.open(path.c_str());
    
    if (file.is_open()) {
        //Header
        getline(file, line);
        
        //Lines
        while (getline(file, line)) {
            vector<string> words = split(line, ',');
            
            // Insert the line on the database
            insert(words);
        }
        file.close();
    } else {
        cout << "Unable to open file - " << path << endl;
    }
}
#endif //TABLE_H