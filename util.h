#ifndef UTIL_H
#define UTIL_H

#include <iostream>
#include <string>
#include <sstream>
#include <vector>

using namespace std;

/**
 * Split a string and put the result into a previously declared vector
 * @see vector<string> split(const string &s, char delim)
 */
void split(const string &s, char delim, vector<string> &elems) {
    stringstream ss;
    ss.str(s);
    string item;
    while (getline(ss, item, delim)) {
        elems.push_back(item);
    }
}

/**
 * Split a string using the delimiter specified.
 * e.g.: split("a:b:c", ':') will return {"a", "b", "c"}
 */
vector<string> split(const string &s, char delim) {
    vector<string> elems;
    split(s, delim, elems);
    return elems;
}

/**
 * Print a 1D vector
 */
template <typename T>
void print(vector<T> row) {
    for (typename vector<T>::iterator it = row.begin(); it != row.end(); it++) {
        cout << (*it) << " | ";
    }
    cout << endl;
}

#endif //UTIL_H