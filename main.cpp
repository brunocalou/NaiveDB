#include "util.h"
#include "Table.h"

using namespace std;

int main() {    
    //Import the csv
    Table table("Aluno");
    // table.importCSV("alunos.csv");
    table.importScheme("alunos_esquema.txt");
    // table.exportBin();
    // table.importBin();
    table.printHeaderFile(5);
    
    return 0;
}