#ifndef JOINBENCHMARK_H
#define JOINBENCHMARK_H

#include "table.h"
#include "timer.h"

class JoinBenchmark {

public:
    
    Table * table_1;
    Table * table_2;
    Table * association_table;
    
    JoinBenchmark(Table * table_1, Table * table_2, Table * association_table);
    
    /*****************************************
     *********** BENCHMARK METHODS ***********
     *****************************************/
     
    /**
     * Run all the query methods
     */
    void runBenchmark();
    
    /*****************************************
     ************* JOIN METHODS **************
     *****************************************/
    //  TODO: Add the join methods here
};

JoinBenchmark::JoinBenchmark(Table * table_1, Table * table_2, Table * association_table) {
    this->table_1 = table_1;
    this->table_2 = table_2;
    this->association_table = association_table;
}

void JoinBenchmark::runBenchmark() {
}

#endif //JOINBENCHMARK_H