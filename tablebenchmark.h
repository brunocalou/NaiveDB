#ifndef TABLEBENCHMARK_H
#define TABLEBENCHMARK_H

#include "table.h"

class TableBenchmark {

public:
    
    Table * table;
    
    TableBenchmark(Table * table);
    
    /*****************************************
     *********** BENCHMARK METHODS ***********
     *****************************************/
     
    /**
     * Run all the query methods
     */
    void runBenchmark();
    
    /*****************************************
     ************ QUERY METHODS **************
     *****************************************/
     /**
      * Perform a query where only the _id is compared to the value.
      * The query is made by searching on the table file
      * @return the table row with the selected id
      */
     vector<string> sequentialFileQuery(string _id);
     
     /**
      * Perform a query where only the _id is compared to the value.
      * The query is made by searching on the in-memory index
      * @return the table row with the selected id
      */
     vector<string> sequentialIndexQuery(string _id);
     
     /**
      * Perform a query where only the _id is compared to the value.
      * The query is made by searching on the in-memory index using a B+ tree
      * @return the table row with the selected id
      */
     vector<string> bPlusTreeQuery(string _id);
     
     /**
      * Perform a query where min < _id < max
      * The query is made by searching on the in-memory index using binary search
      * @return the table rows with the selected ids
      */
     vector<string> binaryIndexQuery(string _id);
     
     /*****************************************
      ********** RANGE QUERY METHODS **********
      *****************************************/
     
     /**
      * Perform a query where min < _id < max
      * The query is made by searching on the table file
      * @return the table rows with the selected ids
      */
     vector<vector<string> > sequentialFileRangeQuery(string _id, int min, int max);
     
     /**
      * Perform a query where min < _id < max
      * The query is made by searching on the in-memory index
      * @return the table rows with the selected ids
      */
     vector<vector<string> > sequentialIndexRangeQuery(string _id, int min, int max);
     
     /**
      * Perform a query where min < _id < max
      * The query is made by searching on the in-memory index using a B+ tree
      * @return the table rows with the selected ids
      */
     vector<vector<string> > bPlusTreeRangeQuery(string _id, int min, int max);
     
     /**
      * Perform a query where min < _id < max
      * The query is made by searching on the in-memory index using binary search
      * @return the table rows with the selected ids
      */
     vector<vector<string> > binaryIndexRangeQuery(string _id, int min, int max);
};

TableBenchmark::TableBenchmark(Table * table) {
    this->table = table;
}

void TableBenchmark::runBenchmark() {
    //TODO: Get the time for each method and print on the screen
    sequentialFileQuery("123");
    sequentialIndexQuery("123");
    bPlusTreeQuery("123");
    binaryIndexQuery("123");
}

vector<string> TableBenchmark::sequentialFileQuery(string _id) {
    vector<string> row;
    long long _number_id = std::stoll(_id.c_str());
    
    ifstream file;
    file.open(table->path.c_str(), ios::binary);
    
    cout << "sequential file query" << endl;
    
    vector<SchemeCol>* scheme_cols = table->scheme.getCols();
    
    while (file.good()) {
        //Import the header
        RegistryHeader header;
        file.read(header.table_name, sizeof(header.table_name));
        file.read(reinterpret_cast<char *> (& header.registry_size), sizeof(header.registry_size));
        file.read(reinterpret_cast<char *> (& header.time_stamp), sizeof(header.time_stamp));
        
        //Read the id
        long long row_id;
        file.read(reinterpret_cast<char *> (&row_id), scheme_cols->begin()->getSize());        
        
        if (row_id == _number_id) {
            // Add the _id to the row
            row.push_back(_id);
            cout << "Found " << row_id << endl;
            cout << "  | " << header.table_name << " " << header.registry_size << " " << header.time_stamp << " | ";
        
            //Read and convert the values from the file
            for (vector<SchemeCol>::iterator it = scheme_cols->begin() + 1; it != scheme_cols->end(); it++) {
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
                }  else if (scheme_col.type == INT64) {
                    long long value;
                    file.read(reinterpret_cast<char *> (&value), scheme_col.getSize());
                    cout << "INT64 " << value << " | ";
                    stream << value;
                }
                
                string string_value;
                string_value = stream.str();
                
                //Add the value to the row
                row.push_back(string_value);   
            }
            cout << endl;
            // The id was found, exit the loop
            break;
            
        } else {
            // Go to the next registry
            // The current position is right after the _id
            // | HEADER | ID | REST_OF_THE_BODY | HEADER |
            //               ^
            // Must set the position to the next header
            file.seekg((long long) file.tellg() + header.registry_size - table->HEADER_SIZE - sizeof(_number_id));
        }
    }
    
    file.close();
    
    return row;
}
vector<string> TableBenchmark::sequentialIndexQuery(string _id) {
    vector<string> row;
    
    return row;
}
vector<string> TableBenchmark::bPlusTreeQuery(string _id) {
    vector<string> row;
    
    return row;
}
vector<string> TableBenchmark::binaryIndexQuery(string _id) {
    vector<string> row;
    
    return row;
}

/*****************************************
 ********** RANGE QUERY METHODS **********
 *****************************************/

vector<vector<string> > TableBenchmark::sequentialFileRangeQuery(string _id, int min, int max) {
    vector<vector<string> > rows;
    
    return rows;
}

vector<vector<string> > TableBenchmark::sequentialIndexRangeQuery(string _id, int min, int max) {
    vector<vector<string> > rows;
    
    return rows;
}

vector<vector<string> > TableBenchmark::bPlusTreeRangeQuery(string _id, int min, int max) {
    vector<vector<string> > rows;
    
    return rows;
}

vector<vector<string> > TableBenchmark::binaryIndexRangeQuery(string _id, int min, int max) {
    vector<vector<string> > rows;
    
    return rows;
}
#endif //TABLEBENCHMARK_H