#ifndef CURSOR_H
#define CURSOR_H

#include <string>
#include <vector>

#include "scheme.h"

using namespace std;

class Cursor {
private:
    vector<vector <string> >::iterator it;
    vector<vector <string> > data;
    Scheme scheme;
    
public:
    Cursor(Scheme scheme, vector<vector <string> > data){};
    void moveToFirst(){};
    void moveToNext(){};
    string getString(string column_name){};
    string getString(int column_index){};
    int getColumnIndex(string column_name){};
};

#endif //CURSOR_H