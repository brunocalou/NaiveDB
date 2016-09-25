#ifndef TABLEBENCHMARK_H
#define TABLEBENCHMARK_H

#include "table.h"

class TableBenchmark {
    friend class Table;

public:
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
     
     /*****************************************
      ******** SEQUENCE QUERY METHODS *********
      *****************************************/
     
     /**
      * Perform a query where min < _id < max
      * The query is made by searching on the in-memory index using binary search
      * @return the table rows with the selected ids
      */
     vector<vector<string> > binaryIndexSequenceQuery(string _id, int min, int max);
     
     /**
      * Perform a query where min < _id < max
      * The query is made by searching on the table file
      * @return the table rows with the selected ids
      */
     vector<vector<string> > sequentialFileSequenceQuery(string _id, int min, int max);
     
     /**
      * Perform a query where min < _id < max
      * The query is made by searching on the in-memory index
      * @return the table rows with the selected ids
      */
     vector<vector<string> > sequentialIndexSequenceQuery(string _id, int min, int max);
     
     /**
      * Perform a query where min < _id < max
      * The query is made by searching on the in-memory index using a B+ tree
      * @return the table rows with the selected ids
      */
     vector<vector<string> > bPlusTreeSequenceQuery(string _id, int min, int max);
     
     /**
      * Perform a query where min < _id < max
      * The query is made by searching on the in-memory index using binary search
      * @return the table rows with the selected ids
      */
     vector<vector<string> > binaryIndexSequenceQuery(string _id, int min, int max);
};

voit TableBenchmark::runBenchmark() {
    //TODO: Get the time for each method and print on the screen
    benchmark.sequentialFileQuery('123');
    benchmark.sequentialIndexQuery('123');
    benchmark.bPlusTreeQuery('123');
    benchmark.binaryIndexQuery('123');
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

vector<vector<string> > TableBenchmark::binaryIndexSequenceQuery(string _id, int min, int max) {
    vector<vector<string> > rows;
    
    return rows;
}
vector<vector<string> > TableBenchmark::sequentialFileSequenceQuery(string _id, int min, int max) {
    vector<vector<string> > rows;
    
    return rows;
}
vector<vector<string> > TableBenchmark::sequentialIndexSequenceQuery(string _id, int min, int max) {
    vector<vector<string> > rows;
    
    return rows;
}
vector<vector<string> > TableBenchmark::bPlusTreeSequenceQuery(string _id, int min, int max) {
    vector<vector<string> > rows;
    
    return rows;
}
vector<vector<string> > TableBenchmark::binaryIndexSequenceQuery(string _id, int min, int max) {
    vector<vector<string> > rows;
    
    return rows;
}
#endif //TABLEBENCHMARK_H