#include "Candas.h"

void test1()
{
    const char cols[MAX_COL_NUM][MAX_COL_LEN] = {"TYPE", "ANC_ID", "TAG_ID", "DISTANCE"};
    const char dtypes[MAX_COL_NUM] = "CIID";
    int n_row = 5;
    int n_col = 4;

    char types[5] = {'O', 'O', 'O', 'O', 'O'};
    int anchor_ids[5] = {101, 102, 103, 104, 105};
    int tag_ids[5] = {1, 2, 3, 4, 5};
    double distances[5] = {1.1, 2.2, 3.3, 4.4, 5.5};
    void *values[MAX_COL_NUM] = {types, anchor_ids, tag_ids, distances};

    for (int i = 0; i < 100000; i++)
    {
        can_dataframe *df = can_alloc(n_row, n_col, cols, dtypes, values);
        can_print(df, 5);
        can_free(df);
    }
}

int main(int argc, char const *argv[])
{
    const char cols[MAX_COL_NUM][MAX_COL_LEN] = {"ANCHOR", "N", "E", "U", "ANT1", "ANT2"};
    can_dataframe *df = can_read_csv("../test_data/test1", 6, cols, "CDDDII", " ", 1);
    can_print(df, 2);
    can_free(df);
}
