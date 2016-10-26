#ifndef TABLE_H
#define TABLE_H

#include "util.h"
#include "schema.h"
#include "cursor.h"
#include "queryable.h"
#include "join.h"
#include <fstream>
#include <time.h>
#include <string.h>
#include <algorithm>
#include <utility> //std::pair
#include <stdio.h>


class Table : public Queryable{
private:
    unsigned HEADER_SIZE;

    Schema schema;
    string name;
    string path;
    string header_file_path;
    header_t * header; // _id, registry_position
    
    friend class TableBenchmark;
    
    /**
     * Save a value to the file using the correct type and size. The file
     * will not be opened nor closed inside this method
     */
    void convertAndSave(ofstream *file, string * value, SchemaCol * schema_col);
     
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
     * Import the schema using the Schema standard method
     */
    void importSchema(const string & path);
    
    /**
     * Set the schema to be used
     * @see Schema
     */
    void setSchema(Schema schema);

    Schema getSchema();
    header_t * getHeader();
    
    /*****************************************
     ************* QUERY METHODS *************
     *****************************************/
    
    /**
     * Insert a row at the end of the table. Note that no consistency test
     * is made to ensure uniqueness of any element and the primary key is managed
     * internally (auto increment)
     * @param row - the table row (primary key not included). Note that
     *              the order of elements is important and it's expected
     *              to be the same as the order on the schema
     * @return the _id of the inserted item (used as primary key)
     */
    long long insert(vector<string> row);
    
    /**
     * Get a line from the file, given the registry position.
     * @return a vector containing the _id and the row content
     */
    vector<string> getRow(long long registry_position);
    
    /**
     * Get a row from the file, given the specified _id. This
     * method uses the binary search algorithm
     */
    vector<string> getRowById(long long _id);
    
    
    Join join(string thisCollumn, Table* otherTable, string otherCollumn, JoinType join_type);
    /**
     * Deletes the table and all its associated files
     */
    void drop();
     
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
    
    /**
     * Returns a merge between this table and a table passed as argument.
     * (At the present moment, the only possible parameter for this join is the id)
     * @param otherTable an object that represents the other table
     *        that should be used on the join
     */
     vector<vector<long long> > *mergeJoin(Table otherTable);
};

Table::Table(string name)  {
    this->name = name;
    this->path = name + ".dat";
    this->header_file_path = name + "_h.dat";
    this->header = new header_t();
    loadHeader();
    
    RegistryHeader reg_header;
    Table::HEADER_SIZE = sizeof(reg_header.table_name) + sizeof(reg_header.registry_size) + sizeof(reg_header.time_stamp);
    // cout << "HEADER_SIZE = " << HEADER_SIZE << endl;
    
}

Table::~Table() {
    delete this->header;
}

void Table::importSchema(const string & path) {
    schema.import(path);
}

void Table::setSchema(Schema schema) {
    this->schema = schema;
}

Schema Table::getSchema(){
    return this->schema;
}


header_t * Table::getHeader(){
    return this->header;
}

void Table::loadHeader() {
    ifstream file;
    file.open(header_file_path.c_str(), ios::binary);
    
    while (!file.eof()) {
        HeaderFile header;
        if (!file.read(reinterpret_cast<char *> (&header._id), sizeof(header._id)) ||
            !file.read(reinterpret_cast<char *> (&header.registry_position), sizeof(header.registry_position))) {
                break;
        } else {
            this->header->push_back(pair<decltype(header._id), decltype(header.registry_position) > (header._id, header.registry_position));
        }
    }
    
    file.close();
}

void Table::convertAndSave(ofstream *file, string * string_value, SchemaCol *schema_col) {
    if (schema_col->type == INT32) {
        int value = atoi((*string_value).c_str());
        file->write(reinterpret_cast<char *> (&value), schema_col->getSize());
        // cout << "INT32 " << value << "(" << *string_value << ")" << " | ";
    } else if (schema_col->type == CHAR) {
        char value[schema_col->getSize()];
        strncpy(value, &string_value->c_str()[0], schema_col->getSize());
        file->write(reinterpret_cast<char *> (&value), schema_col->getSize());
        // cout << "CHAR " << value << "(" << *string_value << ")" << " | ";
    } else if (schema_col->type == FLOAT) {
        float value = atof((*string_value).c_str());
        file->write(reinterpret_cast<char *> (&value), schema_col->getSize());
        // cout << "FLOAT " << value << "(" << *string_value << ")" << " | ";
    } else if (schema_col->type == DOUBLE) {
        double value = atof((*string_value).c_str());
        file->write(reinterpret_cast<char *> (&value), schema_col->getSize());
        // cout << "DOUBLE " << value << "(" << *string_value << ")" << " | ";
    } else if (schema_col->type == INT64 || schema_col->type == FOREIGN_KEY) {
        long long value = std::stoll((*string_value).c_str());
        file->write(reinterpret_cast<char *> (&value), schema_col->getSize());
        // cout << "INT64 " << value << "(" << *string_value << ")" << " | ";
    }
}

long long Table::insert(vector<string> row) {
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
    header.registry_size = HEADER_SIZE + schema.getSize();
    time (& header.time_stamp);
    
    file.write(header.table_name, sizeof(header.table_name));
    file.write(reinterpret_cast<char *> (& header.registry_size), sizeof(header.registry_size));
    file.write(reinterpret_cast<char *> (& header.time_stamp), sizeof(header.time_stamp));
    
    // cout << "  | " << header.table_name << " " << header.registry_size << " " << header.time_stamp << " | ";
    
    //Push the _id to the row
    string _id_str;
    
    std::stringstream strstream;
    strstream << header_file._id;
    strstream >> _id_str;
    
    //Insert the _id on the first position so it matches the SchemaCol
    row.insert(row.begin(), _id_str);
    
    //Export the table according to the schema
    vector<SchemaCol>* schema_cols = schema.getCols();
    
    int schema_col_position = 0;
    
    for (vector<string>::iterator row_it = row.begin(); row_it != row.end(); row_it++) {
        //Iterate through the row and save the values
        //TODO: Consider the array size
        convertAndSave(&file, &(*row_it), &schema_cols->at(schema_col_position));
        schema_col_position ++;
    }
    // cout << endl;
    
    file.close();
    
    return header_file._id;
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
    cout << "Printing " << name << " header file" << endl;
    ifstream file;
    file.open(header_file_path.c_str(), ios::binary);
    int counter = 0;
    
    while (!file.eof() && counter != number_of_values) {
        HeaderFile header;
        if (!file.read(reinterpret_cast<char *> (&header._id), sizeof(header._id)) ||
            !file.read(reinterpret_cast<char *> (&header.registry_position), sizeof(header.registry_position))) {
                break;
        } else {
            cout << header._id << " " << header.registry_position << endl;
        }
        counter ++;
    }
    cout << endl;
    
    file.close();
}

void Table::print(int number_of_values) {
    cout << "Printing " << name << " table" << endl;
    int counter = 0;
    while (counter != number_of_values) {
        // cout << "headerSize= "<< header->size()<< endl;
        if (counter == header->size()) {
            break;
        }
        vector<string> row = getRow(header->at(counter).second);
        
        //print the line
        for (vector<string>::iterator it = row.begin(); it != row.end(); it++) {
            cout << (*it) << " | ";
        }
        
        counter ++;
        cout << endl;
    }
    cout << endl;
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
    
    Cursor cursor(schema, result);
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

vector<string> Table::getRow(long long registry_position) {
    ifstream file;
    file.open(path.c_str(), ios::binary);
    
    // Set the file position
    file.seekg(registry_position);
    
    vector<SchemaCol>* schema_cols = schema.getCols();
    vector<string> row;
    
    //Import the header
    RegistryHeader header;
    file.read(header.table_name, sizeof(header.table_name));
    file.read(reinterpret_cast<char *> (& header.registry_size), sizeof(header.registry_size));
    file.read(reinterpret_cast<char *> (& header.time_stamp), sizeof(header.time_stamp));
    
    // cout << "  | " << header.table_name << " " << header.registry_size << " " << header.time_stamp << " | ";

    //Read and convert the values from the file
    for (vector<SchemaCol>::iterator it = schema_cols->begin(); it != schema_cols->end(); it++) {
        SchemaCol & schema_col = *it;
        ostringstream stream;
        
        if (schema_col.type == INT32) {
            int value;
            file.read(reinterpret_cast<char *> (&value), schema_col.getSize());
            // cout << "INT32 " << value << " | ";
            stream << value;
        } else if (schema_col.type == CHAR) {
            char value[schema_col.getSize()];
            file.read(reinterpret_cast<char *> (&value), schema_col.getSize());
            // cout << "CHAR " << value << " | ";
            stream << value;
        } else if (schema_col.type == FLOAT) {
            float value;
            file.read(reinterpret_cast<char *> (&value), schema_col.getSize());
            // cout << "FLOAT " << value << " | ";
            stream << value;
        } else if (schema_col.type == DOUBLE) {
            double value;
            file.read(reinterpret_cast<char *> (&value), schema_col.getSize());
            // cout << "DOUBLE " << value << " | ";
            stream << value;
        }  else if (schema_col.type == INT64 || schema_col.type == FOREIGN_KEY) {
            long long value;
            file.read(reinterpret_cast<char *> (&value), schema_col.getSize());
            // cout << "INT64 " << value << " | ";
            stream << value;
        }
        
        string string_value;
        string_value = stream.str();
        
        // Push the value to the line vector
        row.push_back(string_value);
    }
    // cout << endl;
    file.close();
    
    return row;
}

vector<string> Table::getRowById(long long _id) {
    vector<string> row;
    //Iterate through the Table::header
    //The pair is defined like: (first value = _id, second value = registry_position)
    int idx = distance(header->begin(), lower_bound(header->begin(), header->end(), 
       make_pair(_id, numeric_limits<long long>::min())));
    
    // If the found index is equals to the desired index, the _id was found
    auto pair = header->at(idx);
    if (pair.first == _id) {
        row = getRow(pair.second);
        // print(&row);
    }
    return row;
}

void Table::drop() {
    remove(this->path.c_str());
    remove(this->header_file_path.c_str());
    this->header->clear();
}

Join Table::join(string this_collumn_name, Table* other_table, string other_collumn_name, JoinType join_type) {
    return Join(this, this_collumn_name, other_table, other_collumn_name, join_type);
}

vector<vector<long long>> *Table::mergeJoin(Table other_table){
    header_t *table_a = header;
    header_t *table_b = other_table.header;
    unsigned int n = header->size();
    unsigned int m = other_table.header->size();

    unsigned int i = 0;
    unsigned int j = 0;
    int l, k;

    vector<vector<long long>> *return_val = new vector<vector<long long> >;

    while(i < n and j < m){ 
        cout << table_a->at(i).first << " " << table_b->at(j).first << endl; 
        if(table_a->at(i).first > table_b->at(j).first){
            j++;
        }else if(table_a->at(i).first < table_b->at(j).first){
            i++;
        }else{
            l = i;

            while(l < n and table_a->at(l).first == table_a->at(i).first){
                k = j;
                while(k < m and table_b->at(k).first == table_b->at(j).first){
                    return_val->push_back({table_a->at(l).second, table_b->at(k).second});
                    k++;
                }   
                l++;
            }   

            i = l;
            j = k;
        }   
    }   

    return return_val;
}


#endif //TABLE_H