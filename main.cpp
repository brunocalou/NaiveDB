// #include "util.h"
// #include "table.h"
#include "tablebenchmark.h"

using namespace std;

int main() {    
    //Import the csv
    Table table("Aluno");
    table.importScheme("alunos_esquema.txt");
    // Create a file with 2GB
    // for (long i = 0; i < 2714; i++)
    //     table.convertFromCSV("alunos.csv");
    // table.print(5);
    // table.printHeaderFile(5);
    // // Cursor cursor = table.query("SELECT *");
    // // table.query("SELECT _id, name, points WHERE _id=123, name = 'bruno alves', points > - 50, points < 100");
    
    TableBenchmark benchmark(&table);
    benchmark.runBenchmark();
    return 0;
}