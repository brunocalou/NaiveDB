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
     * Performs the Nested Index Join. This method is called inside the constructor
     */
    void nestedIndexJoin(Queryable *this_table, int this_column_position, Queryable* other_table, int other_column_position);
    
    /**
     * Performs a merge between this table and a table passed as argument
     * @param other_table an object that represents the other table
     *        that should be used on the join
     * @param this_column_position the position of the column belonging
     *        to this table that will be used on the join.
     * @param other_column_position a string with the name of the column belonging
     *        to the other table that will be used on the join.
     */
     void mergeJoin(Queryable *this_table, int this_column_position, Queryable* other_table, int other_column_position);
     
     /**
      * Performs the Hash Join algorithm
      */
     void hashJoin(Queryable *this_table, int this_column_position, Queryable* other_table, int other_column_position);
     
     /**
      * Same as mergeJoin(string this_column_name, Table other_table, string other_column_name), but
      * it uses the in-memory header
      */
    //  vector<vector<long long> > *mergeJoin(Table other_table);
    
public:

    /**
     * Perform the inner join using the choosen tables with the specified columns.
     * The main result is a vector of vector of registry positions
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
    Join(Queryable *this_table, string this_column_name, Queryable* other_table, string other_column_name,  JoinType join_type);
    
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
    void print(int number_of_values = -1);
};

void Join::nestedIndexJoin(Queryable *this_table, int this_column_position, Queryable* other_table, int other_column_position) {
    int counter = 0;

    while (counter != this_table->getHeader()->size()) { // Iterate over all of this table
        
        vector<string> this_row = this_table->getRow(this_table->getHeader()->at(counter).second);

        // for eache Row in this table, search for all matches in the other table
        for(int i=0; i < other_table->getHeader()->size(); i++){
            vector<string> other_row = other_table->getRow(other_table->getHeader()->at(i).second);
        
            if(this_row.at(this_column_position) == other_row.at(other_column_position)){
                //When matched, insert the registries position into the vector to be returned
                vector<long long> join_row;

                //get both registries positions
                join_row.push_back(this_table->getHeader()->at(counter).second);
                join_row.push_back(other_table->getHeader()->at(i).second);
                this->join_result->push_back(join_row);

                // cout << "insterted " << this_table->getHeader()->at(counter).second << " and " << other_table->getHeader()->at(i).second << endl;
                //For debugging purpose, cout << "insterted "<< this_table->getHeader()->at(counter).second<< " and " << other_table->getHeader()->at(i).second << endl;
            }
        }
        counter ++;
    }
}

void Join::hashJoin(Queryable *build_table, int build_table_column_position, Queryable* probe_table, int probe_table_column_position) {
    // Key: column, value: header registry position
    map<string, long long> hash_table;
    
    //Fill the hash table
    header_t* hash_header = build_table->getHeader();
    
    for (header_t::iterator it = hash_header->begin(); it != hash_header->end(); it++) {
        string column_value = build_table->getValue(it->first, build_table_column_position);
        long long registry_position = it->second;
        
        hash_table.insert(pair<string, long long>(column_value, registry_position));
    }
    
    // Iterate over the probe table
    header_t* probe_header = probe_table->getHeader();
    
    for (header_t::iterator it = probe_header->begin(); it != probe_header->end(); it++) {
        string column_value = probe_table->getValue(it->first, probe_table_column_position);
        
        map<string, long long>:: iterator hash_it = hash_table.find(column_value);
        if (hash_it != hash_table.end()) {
            // Found it
            // cout << "Found " << column_value << endl;
            this->join_result->push_back({hash_it->second, it->second});
        }
    }
}

void Join::mergeJoin(Queryable *this_table, int this_column_position, Queryable* other_table, int other_column_position) {
    header_t *table_a = this_table->getHeader();
    header_t *table_b = other_table->getHeader();

    // Sorts the headers
    sort(table_a->begin(), table_a->end(),
        [](const pair<long long, long long> &left, const pair<long long, long long> &right) {
            return left.first < right.first;
        });
    sort(table_b->begin(), table_b->end(),
        [](const pair<long long, long long> &left, const pair<long long, long long> &right) {
            return left.first < right.first;
        });

    unsigned int n = table_a->size();
    unsigned int m = table_b->size();

    unsigned int i = 0;
    unsigned int j = 0;
    int l, k;

    while(i < n and j < m) {
        if(table_a->at(i).first > table_b->at(j).first) {
            j++;
        } else if(table_a->at(i).first < table_b->at(j).first) {
            i++;
        } else {
            l = i;

            while(l < n and table_a->at(l).first == table_a->at(i).first) {
                k = j;
                while(k < m and table_b->at(k).first == table_b->at(j).first) {
                    this->join_result->push_back({table_a->at(l).second, table_b->at(k).second});
                    k++;
                }   
                l++;
            }   

            i = l;
            j = k;
        }   
    }
}

// NOT WORKING
// void Join::mergeJoin(Queryable *this_table, int this_column_position, Queryable* other_table, int other_column_position) {
//     cout << "Start merge join" << endl;
//     vector<pair<string, long long>> *table_a = this_table->getColumn(this_column_position);
//     vector<pair<string, long long>> *table_b = other_table->getColumn(other_column_position);
//     cout << "Start sort on merge join" << endl;
//     sort(table_a->begin(), table_a->end(),
//         [](const pair<string, long long> &left, const pair<string, long long> &right){
//             return left.first.compare(right.first) <= 0;
//         });
//     sort(table_b->begin(), table_b->end(),
//         [](const pair<string, long long> &left, const pair<string, long long> &right){
//             return left.first.compare(right.first) <= 0;
//         });
//     cout << "End sort on merge join" << endl;

//     int n = table_a->size();
//     int m = table_b->size();
//     int i = 0;
//     int j = 0;
//     int l, k;

//     cout << "Start loop on merge join" << endl;
//     while(i < n and j < m){ 
//         if(table_a->at(i).first.compare(table_b->at(j).first) > 0) {
//             j++;
//         } else if(table_a->at(i).first.compare(table_b->at(j).first) < 0) {
//             i++;
//         } else {
//             l = i;

//             while(l < n and table_a->at(l).first.compare(table_a->at(i).first) == 0) {
//                 k = j;
//                 while(k < m and table_b->at(k).first.compare(table_b->at(j).first) == 0) {
//                     this->join_result->push_back({table_a->at(l).second, table_b->at(k).second});
//                     k++;
//                 }   
//                 l++;
//             }   

//             i = l;
//             j = k;
//         }   
//     }   

//     // delete table_a;
//     // delete table_b;

//     cout << this->join_result->size() << endl;
//     cout << "End merge join" << endl;
// }

Join::Join(Queryable *this_table, string this_column_name, Queryable* other_table, string other_column_name, JoinType join_type) {
    this->join_result = new vector<vector<long long>>;

    //saves the tables for future use
    tables.push_back(this_table);
    tables.push_back(other_table);

    //get the order of the choosen columns
    int this_column_position = this_table->getSchema().getColPosition(this_column_name);
    int other_column_position = other_table->getSchema().getColPosition(other_column_name);
    
    switch(join_type) {
        case NESTED_INDEX  : nestedIndexJoin(this_table, this_column_position, other_table, other_column_position); break;
        case NESTED  : break; // TODO
        case HASH  : hashJoin(this_table, this_column_position, other_table, other_column_position); break;
        case MERGE  : mergeJoin(this_table, this_column_position, other_table, other_column_position); break;
    }
}

void Join::print(int number_of_values) {
    
    for(int line=0; line < join_result->size(); line++){ //iterate over the whole matcheds registries postions
        if(line==number_of_values) break;

        for(int table_order=0; table_order<tables.size(); table_order++) { // iterate over the tables involved in the join
            long long registry_position = join_result->at(line).at(table_order);
            Queryable* table = tables.at(table_order);
            vector<string> row_partial = table->getRow(registry_position);

            for(int column=0; column < table->getSchema().getCols()->size(); column++) { // iterate over the columns of one of the Tables
                cout<<row_partial.at(column) << " | ";
            }
     
        }
    cout << endl;
    }
}

Join::~Join() {
    delete this->join_result;
}

#endif //JOIN_H