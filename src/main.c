#include "Candas.h"

void test_alloc_and_free()
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

    for (int i = 0; i < 10000; i++)
    {
        can_dataframe *df = can_alloc(n_row, n_col, cols, dtypes, values);
        can_print(df, 5);
        can_free(df);
    }
}

void test_read_and_write_csv()
{
    const char cols[MAX_COL_NUM][MAX_COL_LEN] = {"ANCHOR", "N", "E", "U", "ANT1", "ANT2"};
    can_dataframe *df = can_read_csv("../test_data/test1", 6, cols, "CDDDII", " ", 1);
    can_print(df, 4);
    can_write_csv("../test_data/test1_copy", df, ",");
    printf("written to ""../test_data/test1_copy");
    can_free(df);
}

void test_get_and_set()
{
    const char cols[MAX_COL_NUM][MAX_COL_LEN] = {"ANCHOR", "N", "E", "U", "ANT1", "ANT2"};
    can_dataframe *df = can_read_csv("../test_data/test1", 6, cols, "CDDDII", " ", 1);
    can_print(df, 5);
    char anchor0 = can_get_char(df, 0, "ANCHOR");
    int ant1_2 = can_get_int(df, 2, "ANT1");
    double n3 = can_get_double(df, 3, "N");
    printf("Row 0 of ANCHOR is %c\n", anchor0);
    printf("Row 2 of ANT1 is %d\n", ant1_2);
    printf("Row 3 of N is %lf\n", n3);

    printf("setting value ...\n");
    can_set_char(df, 0, "ANCHOR", 'K');
    can_set_int(df, 2, "ANT1", 88888);
    can_set_double(df, 3, "N", 1e-10);
    can_print(df, 4);

    can_free(df);
}

int main(int argc, char const *argv[])
{
    //test_alloc_and_free();
    //test_read_and_write_csv();
    test_get_and_set();
}
