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
    printf("written to "
           "../test_data/test1_copy");
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

void test_select_col_and_cols()
{
    const char cols[MAX_COL_NUM][MAX_COL_LEN] = {"ANCHOR", "N", "E", "U", "ANT1", "ANT2"};
    can_dataframe *df = can_read_csv("../test_data/test1", 6, cols, "CDDDII", " ", 1);
    can_print(df, 4);

    can_dataframe *ant1 = can_select_col(df, "ANT1");
    can_print(ant1, 4);

    char sel_cols[MAX_COL_NUM][MAX_COL_LEN] = {"ANT1", "U"};
    can_dataframe *ant1_u = can_select_cols(df, 2, sel_cols);
    can_print(ant1_u, 4);

    can_free(df);
    can_free(ant1);
    can_free(ant1_u);
}

void test_select_row_and_rows()
{
    const char cols[MAX_COL_NUM][MAX_COL_LEN] = {"ANCHOR", "N", "E", "U", "ANT1", "ANT2"};
    can_dataframe *df = can_read_csv("../test_data/test1", 6, cols, "CDDDII", " ", 1);
    can_print(df, 4);

    can_dataframe *row0 = can_select_row(df, 0);
    can_print(row0, 1);

    int rows[3] = {3, 2, 1};
    can_dataframe *row321 = can_select_rows(df, 3, rows);
    can_print(row321, 3);

    can_free(df);
    can_free(row0);
    can_free(row321);
}

void test_filter()
{
    const char cols[MAX_COL_NUM][MAX_COL_LEN] = {"ANCHOR", "N", "E", "U", "ANT1", "ANT2"};
    can_dataframe *df = can_read_csv("../test_data/test1", 6, cols, "CDDDII", " ", 1);
    can_print(df, 4);

    can_dataframe *positive_e = can_filter_double(df, "E", 0, 10000);
    can_print(positive_e, 2);

    can_dataframe *ant1_bigger_than_20000 = can_filter_int(df, "ANT1", 20000, 99999);
    can_print(ant1_bigger_than_20000, 2);

    can_dataframe *anchor_A_to_C = can_filter_char(df, "ANCHOR", 'A', 'C');
    can_print(anchor_A_to_C, 3);

    can_free(positive_e);
    can_free(ant1_bigger_than_20000);
    can_free(anchor_A_to_C);
}

void test_concat()
{
    const char cols[MAX_COL_NUM][MAX_COL_LEN] = {"ANCHOR", "N", "E", "U", "ANT1", "ANT2"};
    can_dataframe *df1 = can_read_csv("../test_data/test1", 6, cols, "CDDDII", " ", 1);
    can_print(df1, 4);

    can_dataframe *df2 = can_read_csv("../test_data/test1", 6, cols, "CDDDII", " ", 1);

    can_dataframe *df3 = can_concat_row(df1, df2);
    can_print(df3, 8);

    can_dataframe *df4 = can_concat_col(df3, df3);
    can_print(df4, 8);

    can_free(df1);
    can_free(df2);
    can_free(df3);
}

void test_merge()
{
    const char cols[MAX_COL_NUM][MAX_COL_LEN] = {"ANCHOR", "N", "E", "U", "ANT1", "ANT2"};
    can_dataframe *df1 = can_read_csv("../test_data/test1", 6, cols, "CDDDII", " ", 1);
    can_print(df1, 4);

    const char cols2[MAX_COL_NUM][MAX_COL_LEN] = {"ANT1", "LENGTH"};
    int ant1[7] = {10001, 10002, 19354, 20000, 19333, 36647, 88888};
    double length[7] = {1.1, 1.2, 1.4, 2.0, 1.3, 3.7, 8.8};
    void* values[MAX_COL_NUM] = {ant1, length};
    can_dataframe *df2 = can_alloc(7, 2, cols2, "ID", values);
    can_print(df2, 7);

    can_dataframe *df3 = can_merge_left(df1, df2, "ANT1");
    can_print(df3, 4);

    can_free(df1);
    can_free(df2);
    can_free(df3);
}

int main(int argc, char const *argv[])
{
    // test_alloc_and_free();
    // test_read_and_write_csv();
    // test_get_and_set();
    // test_select_col_and_cols();
    // test_select_row_and_rows();
    // test_filter();
    // test_concat();
    test_merge();
}
