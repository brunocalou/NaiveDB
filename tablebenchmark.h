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