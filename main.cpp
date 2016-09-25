#include "util.h"
#include "Table.h"

using namespace std;

int main() {    
    //Import the csv
    Table table("Aluno");
    table.importScheme("alunos_esquema.txt");
    // table.convertFromCSV("alunos.csv");
    table.print(5);
    table.printHeaderFile(5);
    
    return 0;
}