#ifndef CURSOR_H
#define CURSOR_H

#include <string>
#include <vector>

#include "schema.h"

using namespace std;

class Cursor {
private:
    vector<vector <string> >::iterator it;
    vector<vector <string> > data;
    Schema schema;
    
public:
    Cursor(Schema schema, vector<vector <string> > data){};
    void moveToFirst(){};
    void moveToNext(){};
    string getString(string column_name){};
    string getString(int column_index){};
    int getColumnIndex(string column_name){};
};

#endif //CURSOR_H