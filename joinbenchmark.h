#ifndef JOINBENCHMARK_H
#define JOINBENCHMARK_H

#include "table.h"
#include "timer.h"

class JoinBenchmark {

public:
    
    Queryable * this_table;
    Queryable * other_table;
    string this_column_name;
    string other_column_name;
    
    JoinBenchmark(Queryable *this_table, string this_column_name, Queryable* other_table, string other_column_name);
    
    /*****************************************
     *********** BENCHMARK METHODS ***********
     *****************************************/
     
    /**
     * Run all the query methods
     */
    void runBenchmark();
    
private:

    /*****************************************
     ************* JOIN METHODS **************
     *****************************************/
    
    void mergeJoin();
    void hashJoin();
    void nestedLoopJoin();
};

JoinBenchmark::JoinBenchmark(Queryable *this_table, string this_column_name, Queryable* other_table, string other_column_name) {
    this->this_table = this_table;
    this->other_table = other_table;
    this->this_column_name = this_column_name;
    this->other_column_name = other_column_name;
}

void JoinBenchmark::runBenchmark() {
    mergeJoin();
    hashJoin();
    nestedLoopJoin();
}

void JoinBenchmark::mergeJoin() {
    cout << "\nMerge Join" << endl;
    
    Timer timer;
    timer.start();
    Join join(this_table, this_column_name, other_table, other_column_name, MERGE);
    cout << "\tTime: " << timer.getElapsedTime() << " s" << endl;
}

void JoinBenchmark::hashJoin() {
    cout << "\nHash Join" << endl;
    
    Timer timer;
    timer.start();
    Join join(this_table, this_column_name, other_table, other_column_name, HASH);
    cout << "\tTime: " << timer.getElapsedTime() << " s" << endl;
}

void JoinBenchmark::nestedLoopJoin() {
    cout << "\nNested Loop Join" << endl;
    
    Timer timer;
    int counter = 0;
    vector<vector<long long>> * join_result = new vector<vector<long long>>;
    int this_column_position = this_table->getSchema().getColPosition(this_column_name);
    int other_column_position = other_table->getSchema().getColPosition(other_column_name);
    vector<vector<string>> this_rows;
    vector<vector<string>> other_rows;
    
    timer.start();
    
    // Load all the rows
    for (int i = 0; i < this_table->getHeader()->size(); i++) {
        this_rows.push_back(this_table->getRow(this_table->getHeader()->at(counter).second));
    }
    for (int i = 0; i < other_table->getHeader()->size(); i++) {
        other_rows.push_back(other_table->getRow(other_table->getHeader()->at(counter).second));
    }
    
    cout << "\tTime to load: " << timer.getElapsedTime() << " s" << endl;
    
    timer.start();

    while (counter != this_table->getHeader()->size()) { // Iterate over all of this table
        
        vector<string> this_row = this_rows.at(counter);

        // for each Row in this table, search for all matches in the other table
        for(int i=0; i < other_table->getHeader()->size(); i++){
            vector<string> other_row = other_rows.at(i);
        
            if(this_row.at(this_column_position) == other_row.at(other_column_position)){
                //When matched, insert the registries position into the vector to be returned
                vector<long long> join_row;

                //get both registries positions
                join_row.push_back(this_table->getHeader()->at(counter).second);
                join_row.push_back(other_table->getHeader()->at(i).second);
                join_result->push_back(join_row);

                // cout << "insterted " << this_table->getHeader()->at(counter).second << " and " << other_table->getHeader()->at(i).second << endl;
                //For debugging purpose, cout << "insterted "<< this_table->getHeader()->at(counter).second<< " and " << other_table->getHeader()->at(i).second << endl;
            }
        }
        counter ++;
    }
    
    cout << "\tTime to complete: " << timer.getElapsedTime() << " s" << endl;
    
    delete join_result;
}


#endif //JOINBENCHMARK_H