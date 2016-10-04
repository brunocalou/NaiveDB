#ifndef TABLEBENCHMARK_H
#define TABLEBENCHMARK_H

#include "table.h"
#include "BPlusTree/bpt.h"
#include "timer.h"

using bpt::bplus_tree;

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
      * Perform a query where only the _id is compared to the value.
      * The query is made by searching on the in-memory index using binary search
      * @return the table rows with the selected ids
      */
     vector<string> binaryIndexQuery(string _id);
     
     /**
      * Perform a query where only the _id is compared to the value.
      * The query is made by searching on the in-memory hash table
      * @return the table rows with the selected ids
      */
     vector<string> hashTableQuery(string _id);
     
     /*****************************************
      ********** RANGE QUERY METHODS **********
      *****************************************/
     
     /**
      * Perform a query where min < _id < max
      * The query is made by searching on the table file
      * @return the table rows with the selected ids
      */
     vector<vector<string> > sequentialFileRangeQuery(int min, int max);
     
     /**
      * Perform a query where min < _id < max
      * The query is made by searching on the in-memory index
      * @return the table rows with the selected ids
      */
     vector<vector<string> > sequentialIndexRangeQuery(int min, int max);
     
     /**
      * Perform a query where min < _id < max
      * The query is made by searching on the in-memory index using a B+ tree
      * @return the table rows with the selected ids
      */
     vector<vector<string> > bPlusTreeRangeQuery(int min, int max);
     
     /**
      * Perform a query where min < _id < max
      * The query is made by searching on the in-memory index using binary search
      * @return the table rows with the selected ids
      */
     vector<vector<string> > binaryIndexRangeQuery(int min, int max);
     
     /**
      * Perform a query where min < _id < max
      * The query is made by searching on the in-memory hash table using binary search
      * @return the table rows with the selected ids
      */
     vector<vector<string> > hashTableRangeQuery(int min, int max);
};

TableBenchmark::TableBenchmark(Table * table) {
    this->table = table;
}

void TableBenchmark::runBenchmark() {
    //TODO: Get the time for each method and print on the screen
    string _id = "65000";
    int min = 64500;
    int max = 65500;
    sequentialFileQuery(_id);
    sequentialIndexQuery(_id);
    binaryIndexQuery(_id);
    hashTableQuery(_id);
    
    sequentialFileRangeQuery(min, max);
    sequentialIndexRangeQuery(min, max);
    binaryIndexRangeQuery(min, max);
    hashTableRangeQuery(min, max);
    
    bPlusTreeQuery(_id);
    bPlusTreeRangeQuery(min, max);
}

vector<string> TableBenchmark::sequentialFileQuery(string _id) {
    cout << "Sequential file query" << endl;
    Timer timer;
    timer.start();
    
    vector<string> row;
    long long _number_id = std::stoll(_id.c_str());
    
    ifstream file;
    file.open(table->path.c_str(), ios::binary);
    
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
            row = table->getRow((long long) file.tellg() - table->HEADER_SIZE - sizeof(_number_id));
            // The id was found, exit the loop
            cout << "Found " << _id << endl;
            cout << "Time " << timer.getElapsedTime() << " s" << endl;
            // print(&row);
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
    cout << "Sequential index query" << endl;
    Timer timer;
    timer.start();
    
    vector<string> row;
    long long _id_number = std::stoll((_id).c_str());
    //Iterate through the Table::header
    //The pair is defined like: (first value = _id, second value = registry_position)
    for (header_t::iterator it = table->header->begin(); it != table->header->end(); it++) {
        // compare the _id
        // cout << it->second << endl;
        if (_id_number == it->first) {
            cout << "Found " << _id << endl;
            cout << "Time " << timer.getElapsedTime() << " s" << endl;
            row = table->getRow(it->second);
            break;
        }
    }
    // print(&row);
    
    return row;
}
vector<string> TableBenchmark::bPlusTreeQuery(string _id) {
    cout << "B+ tree query" << endl;
    
    vector<string> row;
    //Iterate through the Table::header
    //The pair is defined like: (first value = _id, second value = registry_position)
    
    //Create the b+ tree
    bplus_tree tree("test.db", true);
    
    //Fill the b+ tree
    for (header_t::iterator it = table->header->begin(); it != table->header->end(); it++) {
        std::ostringstream stream;
        stream << it->first;
        const char* key = stream.str().c_str();
        tree.insert(key, it->second);
    }
    cout << "Filled b+ tree" << endl;
    
    Timer timer;
    timer.start();
    
    //Search the b+ tree
    bpt::value_t value;
    tree.search(_id.c_str(), &value);
    if (value != -1) {
        cout << "Found " << _id << endl;
        cout << "Time " << timer.getElapsedTime() << " s" << endl;
        row = table->getRow(value);
        // print(&row);
    }
    return row;
}
vector<string> TableBenchmark::binaryIndexQuery(string _id) {
    cout << "Binary index query" << endl;
    Timer timer;
    timer.start();
    
    vector<string> row;
    long long _id_number = stoll(_id.c_str());
    //Iterate through the Table::header
    //The pair is defined like: (first value = _id, second value = registry_position)
    int idx = distance(table->header->begin(), lower_bound(table->header->begin(),table->header->end(), 
       make_pair(_id_number, numeric_limits<long long>::min())));
    
    // If the found index is equals to the desired index, the _id was found
    auto pair = table->header->at(idx);
    if (pair.first == _id_number) {
        cout << "Found " << _id << endl;
        cout << "Time " << timer.getElapsedTime() << " s" << endl;
        row = table->getRow(pair.second);
        // print(&row);
    }
    return row;
}

/*****************************************
 ********** RANGE QUERY METHODS **********
 *****************************************/

vector<vector<string> > TableBenchmark::sequentialFileRangeQuery(int min, int max) {
    cout << "Sequential file range query" << endl;
    Timer timer;
    timer.start();
    
    vector<vector<string> > rows;
    
    ifstream file;
    file.open(table->path.c_str(), ios::binary);
    
    vector<SchemeCol>* scheme_cols = table->scheme.getCols();
    
    bool found = false;
    
    while (file.good()) {
        //Import the header
        RegistryHeader header;
        file.read(header.table_name, sizeof(header.table_name));
        file.read(reinterpret_cast<char *> (& header.registry_size), sizeof(header.registry_size));
        file.read(reinterpret_cast<char *> (& header.time_stamp), sizeof(header.time_stamp));
        
        //Read the id
        long long row_id;
        file.read(reinterpret_cast<char *> (&row_id), scheme_cols->begin()->getSize());        
        
        if (row_id >= min) {
            found = true;
        }
        
        if (row_id > max) {
            break;
        }
        if (found) {
            rows.push_back(table->getRow((long long) file.tellg() - table->HEADER_SIZE - sizeof(row_id)));
        }
        // Go to the next registry
        // The current position is right after the _id
        // | HEADER | ID | REST_OF_THE_BODY | HEADER |
        //               ^
        // Must set the position to the next header
        file.seekg((long long) file.tellg() + header.registry_size - table->HEADER_SIZE - sizeof(row_id));
    
    }
    
    file.close();
    
    if (rows.size() > 0) {
        cout << "Found" << endl;
        cout << "Time " << timer.getElapsedTime() << " s" << endl;
        // print(&rows);
    }
    return rows;
}

vector<vector<string> > TableBenchmark::sequentialIndexRangeQuery(int min, int max) {
    cout << "Sequential index range query" << endl;
    Timer timer;
    timer.start();
    
    vector<vector<string> > rows;
    bool found = false;
    for (header_t::iterator it = table->header->begin(); it != table->header->end(); it++) {
        // compare the _id
        // cout << it->second << endl;
        if (it->first >= min) {
            found = true;
        }
        
        if (it->first > max) {
            break;
        }
        if (found) {
            // cout << "Found " << _id << endl;
            rows.push_back(table->getRow(it->second));
        }
    }
    
    cout << "Found" << endl;
    cout << "Time " << timer.getElapsedTime() << " s" << endl;
    // print(&rows);
    return rows;
}

vector<vector<string> > TableBenchmark::bPlusTreeRangeQuery(int min, int max) {
    cout << "B+ tree range query" << endl;
    
    vector<vector<string> > rows;
    
    //Create the b+ tree
    bplus_tree tree("test.db", true);
    
    //Fill the b+ tree
    for (header_t::iterator it = table->header->begin(); it != table->header->end(); it++) {
        std::ostringstream stream;
        stream << it->first;
        const char* key = stream.str().c_str();
        tree.insert(key, it->second);
    }
    cout << "Filled b+ tree" << endl;
    
    Timer timer;
    timer.start();
    
    bpt::key_t key_1;
    bpt::key_t key_2;
    // The id is unique, so there can be just (max - min + 1) values
    int size = max - min + 1;
    bpt::value_t values[size];
    
    // Fill the values vector with -1
    for (int i = 0; i < sizeof(values) / sizeof(values[0]); i++) {
        values[i] = -1;
    }
    
    //Convert the min to key_1
    ostringstream stream;
    stream << min;
    key_1 = stream.str().c_str();
    
    //Clear the stream
    stream.str("");
    
    //Convert the max to key_2
    stream << max;
    key_2 = stream.str().c_str();
    
    //Search the b+ tree
    tree.search_range(&key_1, key_2, values, size);
    
    for (int i = 0; i < sizeof(values) / sizeof(values[0]); i++) {
        if (values[i] == -1) {
            break;
        }
        rows.push_back(table->getRow(values[i]));
    }

    if (rows.size() > 0) {
        cout << "Found" << endl;
        cout << "Time " << timer.getElapsedTime() << " s" << endl;
        // print(&rows);
    }
    
    return rows;
}

vector<vector<string> > TableBenchmark::binaryIndexRangeQuery(int min, int max) {
    cout << "Binary index range query" << endl;
    Timer timer;
    timer.start();
    
    vector<vector<string> > rows;
    
    //Iterate through the Table::header
    //The pair is defined like: (first value = _id, second value = registry_position)
    int idx = distance(table->header->begin(), lower_bound(table->header->begin(),table->header->end(), 
       make_pair((long long) min, numeric_limits<long long>::min())));
    bool found = false;
    
    // If the found index is equals to the desired index, the _id was found
    auto pair = table->header->at(idx);
    if (pair.first == min) {
        found = true;
        while (pair.first >= min && pair.first <= max) {
            rows.push_back(table->getRow(pair.second));
            if (idx < table->header->size()) {
                idx++;
                pair = table->header->at(idx);
            }
        }
    }
    
    if (found) {
        cout << "Found" << endl;
        cout << "Time " << timer.getElapsedTime() << " s" << endl;
        // print(&rows);
    }
    return rows;
}

vector<string> TableBenchmark::hashTableQuery(string _id) {
    cout << "Hash table query" << endl;
    
    vector<string> row;
    map<long long, long long> hashtable;
    long long _id_number = atoi(_id.c_str());
    
    
    //Fill the map
    hashtable.insert(table->header->begin(), table->header->end());
    
    Timer timer;
    timer.start();
    
    //Search on the map
    row = table->getRow(hashtable.find(_id_number)->second);

    if (row.size() > 0) {
        cout << "Found" << endl;
        cout << "Time " << timer.getElapsedTime() << " s" << endl;
        // print(&row);
    }
    
    return row;
}

vector<vector<string> > TableBenchmark::hashTableRangeQuery(int min, int max) {
    cout << "Hash table range query" << endl;
    
    vector<vector<string> > rows;
    int current_id = min;
    map<long long, long long> hashtable;
    
    //Fill the map
    hashtable.insert(table->header->begin(), table->header->end());
    
    Timer timer;
    timer.start();
    
    //Search on the map
    for (map<long long, long long>::iterator it = hashtable.begin(); it != hashtable.end(); it++) {
        if (current_id > max) {
            break;
        }
 
        vector<string> row = table->getRow(hashtable.find(current_id)->second);       
        if (row.size() > 0) {
            rows.push_back(row);
        }
        current_id ++;
    }
    
    if (rows.size() > 0) {
        cout << "Found" << endl;
        cout << "Time " << timer.getElapsedTime() << " s" << endl;
        // print(&rows);
    }
    
    return rows;
}
#endif //TABLEBENCHMARK_H