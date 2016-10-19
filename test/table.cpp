#include "catch.hpp"
#include "../table.h"

TEST_CASE("A table should have a one-to-one relation") {
    GIVEN("Two related tables") {
        Scheme person_scheme;
        person_scheme.addCol("name", CHAR, 255);
        
        Table person_table("person");
        person_table.setScheme(person_scheme);
        
        Scheme contact_scheme;
        contact_scheme.addCol("number", INT64);
        contact_scheme.addCol("person", FOREIGN_KEY);
        
        Table contact_table("contact");
        contact_table.setScheme(contact_scheme);
        
        REQUIRE(person_scheme.getNumberOfCols() == 2);
        REQUIRE(contact_scheme.getNumberOfCols() == 3);
        
        WHEN("A row on the first table relates to a row on the second one") {
            vector<string> person_row;
            person_row.push_back("Person 1");
            
            vector<string> person_2_row;
            person_2_row.push_back("Person 2");
            person_table.insert(person_2_row);
            
            int person_id = person_table.insert(person_row);
            
            vector<string> contact_row;
            contact_row.push_back("123456");
            contact_row.push_back(std::to_string(person_id));
            int contact_id = contact_table.insert(contact_row);
            
            // person_table.print();
            // contact_table.print();
            
            THEN("The foreign key stored must be the same _id as the related row") {
                vector<string> retrieved_contact_row = contact_table.getRowById(contact_id);
                
                REQUIRE(retrieved_contact_row.size() == 3);
                
                for (int i = 0; i < contact_row.size(); i++) {
                    // cout << retrieved_contact_row.at(i + 1) << " == " << contact_row.at(i) << endl; 
                    REQUIRE(retrieved_contact_row.at(i + 1) == contact_row.at(i));
                }
                
                person_table.drop();
                contact_table.drop();
            }
        }
    }
}