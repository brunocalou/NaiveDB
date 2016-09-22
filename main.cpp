#include "util.h"
#include "Table.h"

using namespace std;

int main() {    
    //Import the csv
    Table table("Aluno", "alunos_tabela.dat");
    table.importCSV("alunos.csv");
    table.importScheme("alunos_esquema.txt");
    table.exportBin();
    table.importBin();
    
    return 0;
}