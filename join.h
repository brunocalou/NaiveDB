#ifndef JOIN_H
#define JOIN_H

#include "util.h"
#include "schema.h"
#include "cursor.h"
#include "queryable.h"
#include <fstream>
#include <time.h>
#include <string.h>
#include <algorithm>
#include <utility> //std::pair
#include <stdio.h>

//Possible types of join
enum JoinType { NESTED_INDEX, NESTED, MERGE, HASH };
class Join {
private:

    vector<Queryable*> tables; // Holds the Tables or Joins(TODO) used to perform this join
    vector<vector<long long>> * join_result; // this structure will hold all registries' positions matched from all tables involved.

    /**
    * Performs the Index Nested Loop Join. This method is called inside the constructor
    */
    vector<vector<long long>>* nestedIndexLoopJoin(Queryable *this_table, int this_column_position, vector<long> *this_table_ids, Queryable *other_table, int other_column_position);

    /**
    * Performs the Nested Loop Join. This method is called inside the constructor
    */
    vector<vector<long long>>* nestedLoopJoin(Queryable *this_table, int this_column_name, Queryable* other_table, int other_column_name);

public:

    /**
     *Perform the inner join using the choosen tables with the specified columns
     *the main result is a vector of vector of registry positions
     * eg: Consider the tables "Person" and "Worked" as below:
     * Person id | name                     Worked  id_company | id_person
     *         9 | Jhoe  (position 111)                  77    |     9        (position 555)
     *        10 | Marta (position 222)                  35    |    10        (position 666)
     *                                                   44    |    10        (position 777)
     *
     * In this case, the result would be a vector below:
     * [ [111,555], [222,666],[222,777]]
     *
     *
     * @constructor
     */
    Join(Queryable *this_table, string this_column_name, Queryable* other_table, string other_column_name, JoinType join_type, vector<long> *this_table_ids);

    /**
     * @destructor
     */
    ~Join();


     /**
     * Print the result of the join, converting the row of registries position into
     * the respective registries (for debugging only)
     * @param number_of_values the number of values to print. If set to -1
     *        all the file wil be printed
     */
    void print(int number_of_values);
};
vector<vector<long long>>* Join::nestedIndexLoopJoin(Queryable *this_table, int this_column_position, vector<long> *this_table_ids, Queryable *other_table, int other_column_position){
    this->join_result = new vector<vector<long long>>;

    for(int i=0; i<this_table_ids->size(); i++){
        vector<string> this_row = this_table->getRow(this_table->getHeader()->at(this_table_ids->at(i)).second);

        for(int j=0; j<other_table->getHeader()->size(); j++){ // Iterate over all of other table to search matches
            vector<string> other_row = other_table->getRow(other_table->getHeader()->at(j).second);

            if(this_row.at(this_column_position) == other_row.at(other_column_position)){
                //When matched, insert the registries position into the vector to be returned
                vector<long long> join_row;

                //get both registries positions
                join_row.push_back(this_table->getHeader()->at(this_table_ids->at(i)).second);
                join_row.push_back(other_table->getHeader()->at(j).second);
                this->join_result->push_back(join_row);
                //For debugging purpose, cout << "insterted "<< this_table->getHeader()->at(this_table_ids->at(i)).second<< " and " << other_table->getHeader->at(j).second << endl;
            }
        }
    }
}
vector<vector<long long>>* Join::nestedLoopJoin(Queryable *this_table, int this_column_position, Queryable* other_table, int other_column_position){
    this->join_result = new vector<vector<long long>>;

    for(int i=0; i<this_table->getHeader()->size(); i++){ // Iterate over all of this table

        vector<string> this_row = this_table->getRow(this_table->getHeader()->at(i).second);

        for(int j=0; j<other_table->getHeader()->size(); j++){ // Iterate over all of other table to search matches
            vector<string> other_row = other_table->getRow(other_table->getHeader()->at(j).second);

            if(this_row.at(this_column_position) == other_row.at(other_column_position)){
                //When matched, insert the registries position into the vector to be returned
                vector<long long> join_row;

                //get both registries positions
                join_row.push_back(this_table->getHeader()->at(i).second);
                join_row.push_back(other_table->getHeader()->at(j).second);
                this->join_result->push_back(join_row);
                //For debugging purpose, cout << "insterted "<< this_table->getHeader()->at(i).second<< " and " << other_table->getHeader->at(j).second << endl;
            }
        }
    }
}
Join::Join(Queryable *this_table, string this_column_name, Queryable* other_table, string other_column_name, JoinType join_type, vector<long> *this_table_ids = NULL){
    //saves the tables for future use
    tables.push_back(this_table);
    tables.push_back(other_table);

    //get the order of the choosen columns
    int this_column_position = this_table->getSchema().getColPosition(this_column_name);
    int other_column_position = other_table->getSchema().getColPosition(other_column_name);

    switch(join_type)
    {
    case NESTED_INDEX :
        if(this_table_ids == NULL){
            cout << "Error : this_table_ids = NULL for Nested Index case" << endl;
            break;
        }
        nestedIndexLoopJoin(this_table, this_column_position, this_table_ids, other_table, other_column_position);
        break;
    case NESTED  : nestedLoopJoin(this_table,this_column_position,other_table,other_column_position); break;
    case HASH  : break; // TODO
    case MERGE  : break; // TODO
    }



}

void Join::print(int number_of_values = -1){

    for(int line=0; line<join_result->size();line++){ //iterate over the whole matcheds registries postions
        if(line==number_of_values) break;

        for(int table_order=0; table_order<tables.size(); table_order++){ // iterate over the tables involved in the join
            long long registry_position = join_result->at(line).at(table_order);
            Queryable* table = tables.at(table_order);
            vector<string> row_partial = table->getRow(registry_position);

            for(int column=0; column <table->getSchema().getCols()->size(); column++){ // iterate over the columns of one of the Tables
                cout<<row_partial.at(column)<< " | ";
            }

        }
    cout <<endl;
    }
}
Join::~Join() {
    delete this->join_result;
}

#endif //JOIN_H
