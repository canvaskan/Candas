/**
 * @file Candas.h
 * @author Haoyu Kan
 * @brief a Pandas-like Dataframe written in pure C (Header-only)
 * @version 0.2
 * @date 2022-11-18
 *
 * @copyright Copyright (c) 2022
 *
 * - only one new struct: can_dataframe
 *  * row | col1[int] |  col2[double] |  col3[char]
 *  --------------------------------------------
 *     0  | 1         |   1.1         |   'A'
 *     1  | 8         |   2.4         |   'B'
 *     .  | .         |    .          |    .
 * - only use C standard library
 * - currently support int/double/char data type,
 *   specified by first character 'I'/'D'/'C'
 * - most functions (except get pointer) are deep copy,
 *   which means use can_free for every can_dataframe
 *
 */

#ifndef CANDAS_H_
#define CANDAS_H_

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#define MAX_COL_NUM 16
#define MAX_COL_LEN 32
#define MAX_LINE_LEN 1024

#define MISS_INT -999
#define MISS_DOUBLE -999.999
#define MISS_CHAR ' '

/**
 * @brief Pandas-like DataFrame, currently only support int/double/char (I/D/C) type
 * row | col1[int] |  col2[double] |  col3[char]
 * --------------------------------------------
 *  0  | 1         |   1.1         |   'A'
 *  1  | 8         |   2.4         |   'B'
 *  .  | .         |    .          |    .
 */
typedef struct
{
    int n_row;
    int n_col;
    char dtypes[MAX_COL_NUM];
    char cols[MAX_COL_NUM][MAX_COL_LEN];
    void *values[MAX_COL_NUM];
} can_dataframe;

// BASIC USAGE ====================================================================================
can_dataframe *can_alloc(int n_row, int n_col, const char cols[MAX_COL_NUM][MAX_COL_LEN], const char dtypes[MAX_COL_NUM], void *values[MAX_COL_NUM]);
void can_free(can_dataframe *df);

can_dataframe *can_read_csv(const char file[MAX_LINE_LEN], int n_col, const char cols[MAX_COL_NUM][MAX_COL_LEN], const char dtypes[MAX_COL_NUM], const char *delim, int skip_row);
void can_write_csv(const char file[MAX_LINE_LEN], const can_dataframe *df, const char *delim);
void can_print(const can_dataframe *df, int n_row);

int can_get_int(const can_dataframe *df, int row, char col[MAX_COL_LEN]);
double can_get_double(const can_dataframe *df, int row, char col[MAX_COL_LEN]);
char can_get_char(const can_dataframe *df, int row, char col[MAX_COL_LEN]);

void can_set_int(can_dataframe *df, int row, char col[MAX_COL_LEN], int value);
void can_set_double(can_dataframe *df, int row, char col[MAX_COL_LEN], double value);
void can_set_char(can_dataframe *df, int row, char col[MAX_COL_LEN], char value);

int *can_get_int_pointer(can_dataframe *df, char col[MAX_COL_LEN]);
double *can_get_double_pointer(can_dataframe *df, char col[MAX_COL_LEN]);
char *can_get_char_pointer(can_dataframe *df, char col[MAX_COL_LEN]);

// Higher Manipulation ============================================================================
can_dataframe *can_select_col(const can_dataframe *df, char col[MAX_COL_LEN]);
can_dataframe *can_select_cols(const can_dataframe *df, int n_col, char cols[MAX_COL_NUM][MAX_COL_LEN]);
can_dataframe *can_select_row(const can_dataframe *df, int row);
can_dataframe *can_select_rows(const can_dataframe *df, int n_row, int *rows);

can_dataframe *can_filter_double(const can_dataframe *df, char col[MAX_COL_LEN], double min, double max);
can_dataframe *can_filter_int(const can_dataframe *df, char col[MAX_COL_LEN], int min, int max);
can_dataframe *can_filter_char(const can_dataframe *df, char col[MAX_COL_LEN], char min, char max);

can_dataframe *can_concat_row(const can_dataframe *df1, const can_dataframe *df2);
can_dataframe *can_concat_col(const can_dataframe *df1, const can_dataframe *df2);

can_dataframe *can_merge_left(const can_dataframe *df1, const can_dataframe *df2, char key_col[MAX_COL_LEN]);

can_dataframe *can_sort(const can_dataframe *df, char key_col[MAX_COL_LEN]);

// TODO
can_dataframe *can_unique(const can_dataframe *df, char key_col[MAX_COL_LEN]);

// BELOW IS IMPLEMENTATION ========================================================================

/// @brief init and alloc memory for can_dataframe
/// @param n_row  I number of rows (can reserve empty rows)
/// @param n_col  I number of cols (must be exact)
/// @param cols   I column names
/// @param dtypes I data types, e.g. "IDDC" means 4 cols with int, double, double, char
/// @param values I init values of columns (deep copy, only n_row), set NULL (not {NULL}) to do only malloc
/// @return
can_dataframe *can_alloc(int n_row, int n_col, const char cols[MAX_COL_NUM][MAX_COL_LEN], const char dtypes[MAX_COL_NUM], void *values[MAX_COL_NUM])
{
    can_dataframe *df = (can_dataframe *)calloc(1, sizeof(can_dataframe));
    if (df == NULL)
    {
        fprintf(stderr, "ERORR: can_alloc cannot alloc memory\n");
        exit(EXIT_FAILURE);
    }

    if (n_row < 0)
    {
        fprintf(stderr, "ERROR: can_alloc n_row < 0\n");
        exit(EXIT_FAILURE);
    }
    df->n_row = n_row;

    if (n_col <= 0)
    {
        fprintf(stderr, "ERROR: can_alloc n_col <= 0\n");
        exit(EXIT_FAILURE);
    }
    df->n_col = n_col;

    for (int j = 0; j < n_col; j++)
    {
        if (strlen(cols[j]) >= MAX_COL_LEN)
        {
            fprintf(stderr, "WARNING: can_alloc col name %s exceed MAX_COL_LEN = %d, col name will be cut\n", cols[j], MAX_COL_LEN);
        }
        strncpy(df->cols[j], cols[j], MAX_COL_LEN);

        if (dtypes[j] == 'I' || dtypes[j] == 'D' || dtypes[j] == 'C')
        {
            df->dtypes[j] = dtypes[j];
        }
        else
        {
            fprintf(stderr, "ERROR: dtype must be 'I'(int) or 'D'(double) or 'C'(char)\n");
            exit(EXIT_FAILURE);
        }

        if (dtypes[j] == 'I')
        {
            df->values[j] = (int *)malloc(sizeof(int) * n_row);
            if (df->values[j] == NULL)
            {
                fprintf(stderr, "ERORR: can_alloc cannot alloc memory\n");
                exit(EXIT_FAILURE);
            }
            if (values != NULL)
            {
                memcpy(df->values[j], values[j], sizeof(int) * n_row);
            }
        }
        else if (dtypes[j] == 'D')
        {
            df->values[j] = (double *)malloc(sizeof(double) * n_row);
            if (df->values[j] == NULL)
            {
                fprintf(stderr, "ERORR: can_alloc cannot alloc memory\n");
                exit(EXIT_FAILURE);
            }
            if (values != NULL)
            {
                memcpy(df->values[j], values[j], sizeof(double) * n_row);
            }
        }
        else if (dtypes[j] == 'C')
        {
            df->values[j] = (char *)malloc(sizeof(char) * n_row);
            if (df->values[j] == NULL)
            {
                fprintf(stderr, "ERORR: can_alloc cannot alloc memory\n");
                exit(EXIT_FAILURE);
            }
            if (values != NULL)
            {
                memcpy(df->values[j], values[j], sizeof(char) * n_row);
            }
        }
    }
    return df;
}

/// @brief free dataframe, free the values in it and set n_row = 0
/// @param df IO dataframe to be freed
void can_free(can_dataframe *df)
{
    for (int j = 0; j < df->n_col; j++)
    {
        free(df->values[j]);
    }
    df->n_row = 0;
}

/// @brief read csv file to dataframe
/// @param file     I csv filepath
/// @param n_col    I number of columns
/// @param cols     I column names
/// @param dtypes   I column data types
/// @param delim    I delimiters (internally use strtok)
/// @param skip_row I number of header row to skip
/// @return           dataframe
can_dataframe *can_read_csv(const char file[MAX_LINE_LEN], int n_col, const char cols[MAX_COL_NUM][MAX_COL_LEN], const char dtypes[MAX_COL_NUM], const char *delim, int skip_row)
{
    FILE *fp = fopen(file, "r");
    if (!fp)
    {
        fprintf(stderr, "can_read_csv cannot open file %s", file);
        exit(EXIT_FAILURE);
    }
    char line[MAX_LINE_LEN] = "";

    // count row
    int n_row = 0;
    for (int i = 0; i < skip_row; i++)
    {
        fgets(line, MAX_LINE_LEN, fp);
    }
    while (fgets(line, MAX_LINE_LEN, fp))
    {
        n_row++;
    }

    rewind(fp);

    // init dataframe
    can_dataframe *df = can_alloc(n_row, n_col, cols, dtypes, NULL);

    for (int i = 0; i < skip_row; i++)
    {
        fgets(line, MAX_LINE_LEN, fp);
    }

    for (int i = 0; i < df->n_row; i++)
    {
        fgets(line, MAX_LINE_LEN, fp);
        char *pch = strtok(line, delim);
        if (*pch == '\n') // empty line
        {
            fprintf(stderr, "WARNING: can_read_csv detect empty line at line %d\n", i + 1 + skip_row);
            continue;
        }
        for (int j = 0; j < df->n_col; j++)
        {
            if (df->dtypes[j] == 'I')
            {
                if (pch == NULL)
                {
                    ((int *)(df->values[j]))[i] = MISS_INT;
                }
                else
                {
                    ((int *)(df->values[j]))[i] = atoi(pch);
                }
            }
            else if (df->dtypes[j] == 'D')
            {
                if (pch == NULL)
                {
                    ((double *)(df->values[j]))[i] = MISS_DOUBLE;
                }
                else
                {
                    ((double *)(df->values[j]))[i] = atof(pch);
                }
            }
            else if (df->dtypes[j] == 'C')
            {
                if (pch == NULL)
                {
                    ((char *)(df->values[j]))[i] = MISS_CHAR;
                }
                else
                {
                    ((char *)(df->values[j]))[i] = *pch;
                }
            }

            pch = strtok(NULL, delim);

            if (j == df->n_col - 1 && pch != NULL)
            {
                fprintf(stderr, "WARNING: can_read_csv encounter strange line at line %d\n", i + 1 + skip_row);
            }
        }
    }

    fclose(fp);
    return df;
}

void can_write_csv(const char file[MAX_LINE_LEN], const can_dataframe *df, const char *delim)
{
    FILE *fp = fopen(file, "w");
    if (!fp)
    {
        fprintf(stderr, "can_read_csv cannot open file %s", file);
        exit(EXIT_FAILURE);
    }

    fprintf(fp, "Dataframe (%d, %d) , dtypes: %s\n", df->n_row, df->n_col, df->dtypes);

    for (int j = 0; j < df->n_col; j++)
    {
        fprintf(fp, "%s%s", df->cols[j], delim);
    }
    fprintf(fp, "\n");

    for (int i = 0; i < df->n_row; i++)
    {
        for (int j = 0; j < df->n_col; j++)
        {
            if (df->dtypes[j] == 'I')
            {
                fprintf(fp, "% d%s", ((int *)(df->values[j]))[i], delim);
            }
            else if (df->dtypes[j] == 'D')
            {
                fprintf(fp, "% e%s", ((double *)(df->values[j]))[i], delim);
            }
            else if (df->dtypes[j] == 'C')
            {
                fprintf(fp, "%c%s", ((char *)(df->values[j]))[i], delim);
            }
        }
        fprintf(fp, "\n");
    }

    fclose(fp);
}

/// @brief print n_row of df on screen
/// @param df     I dataframe to be shown
/// @param n_row  I n row
void can_print(const can_dataframe *df, int n_row)
{
    if (n_row > df->n_row)
    {
        fprintf(stderr, "WARNING: can_print n_row > df->n_row, will be cut\n");
        n_row = df->n_row;
    }

    printf("Dataframe (%d, %d) , dtypes: %s\n", df->n_row, df->n_col, df->dtypes);

    for (int j = 0; j < df->n_col; j++)
    {
        printf("%s\t", df->cols[j]);
    }
    printf("\n");

    for (int i = 0; i < n_row; i++)
    {
        for (int j = 0; j < df->n_col; j++)
        {
            if (df->dtypes[j] == 'I')
            {
                printf("% d\t", ((int *)(df->values[j]))[i]);
            }
            else if (df->dtypes[j] == 'D')
            {
                printf("% e\t", ((double *)(df->values[j]))[i]);
            }
            else if (df->dtypes[j] == 'C')
            {
                printf("%c\t", ((char *)(df->values[j]))[i]);
            }
        }
        printf("\n");
    }
}

/// @brief get int type value by row and col name
/// @param df  I dataframe
/// @param row I row number(start from 0)
/// @param col I col name
/// @return integer type value
int can_get_int(const can_dataframe *df, int row, char col[MAX_COL_LEN])
{
    if (row >= df->n_row)
    {
        fprintf(stderr, "ERORR: can_get_int row=%d >= df->n_row\n", row);
        exit(EXIT_FAILURE);
    }
    else if (row < 0)
    {
        fprintf(stderr, "ERORR: can_get_int invalid row=%d < 0\n", row);
        exit(EXIT_FAILURE);
    }

    int found_col = -1;
    for (int j = 0; j < df->n_col; j++)
    {
        if (strcmp(col, df->cols[j]) == 0)
        {
            found_col = j;
            break;
        }
    }
    if (found_col == -1)
    {
        fprintf(stderr, "ERORR: can_get_int cannot found col=%s\n", col);
    }
    else if (df->dtypes[found_col] != 'I')
    {
        fprintf(stderr, "ERORR: can_get_int found col=%s but it is not integer type\n", col);
        exit(EXIT_FAILURE);
    }

    int res = ((int *)df->values[found_col])[row];
    return res;
}

/// @brief get double type value by row and col name
/// @param df  I dataframe
/// @param row I row number(start from 0)
/// @param col I col name
/// @return double type value
double can_get_double(const can_dataframe *df, int row, char col[MAX_COL_LEN])
{
    if (row >= df->n_row)
    {
        fprintf(stderr, "ERORR: can_get_double row=%d >= df->n_row\n", row);
        exit(EXIT_FAILURE);
    }
    else if (row < 0)
    {
        fprintf(stderr, "ERORR: can_get_double invalid row=%d < 0\n", row);
        exit(EXIT_FAILURE);
    }

    int found_col = -1;
    for (int j = 0; j < df->n_col; j++)
    {
        if (strcmp(col, df->cols[j]) == 0)
        {
            found_col = j;
            break;
        }
    }
    if (found_col == -1)
    {
        fprintf(stderr, "ERORR: can_get_double cannot found col=%s\n", col);
    }
    else if (df->dtypes[found_col] != 'D')
    {
        fprintf(stderr, "ERORR: can_get_double found col=%s but it is not double type\n", col);
        exit(EXIT_FAILURE);
    }

    double res = ((double *)df->values[found_col])[row];
    return res;
}

/// @brief get char type value by row and col name
/// @param df  I dataframe
/// @param row I row number(start from 0)
/// @param col I col name
/// @return char type value
char can_get_char(const can_dataframe *df, int row, char col[MAX_COL_LEN])
{
    if (row >= df->n_row)
    {
        fprintf(stderr, "ERORR: can_get_char row=%d >= df->n_row\n", row);
        exit(EXIT_FAILURE);
    }
    else if (row < 0)
    {
        fprintf(stderr, "ERORR: can_get_char invalid row=%d < 0\n", row);
        exit(EXIT_FAILURE);
    }

    int found_col = -1;
    for (int j = 0; j < df->n_col; j++)
    {
        if (strcmp(col, df->cols[j]) == 0)
        {
            found_col = j;
            break;
        }
    }
    if (found_col == -1)
    {
        fprintf(stderr, "ERORR: can_get_char cannot found col=%s\n", col);
    }
    else if (df->dtypes[found_col] != 'C')
    {
        fprintf(stderr, "ERORR: can_get_char found col=%s but it is not char type\n", col);
        exit(EXIT_FAILURE);
    }

    char res = ((char *)df->values[found_col])[row];
    return res;
}

/// @brief set int type value of df
/// @param df    IO dataframe
/// @param row   I number of row
/// @param col   I column name
/// @param value I given integer type value
void can_set_int(can_dataframe *df, int row, char col[MAX_COL_LEN], int value)
{
    if (row >= df->n_row)
    {
        fprintf(stderr, "ERORR: can_set_int row=%d >= df->n_row\n", row);
        exit(EXIT_FAILURE);
    }
    else if (row < 0)
    {
        fprintf(stderr, "ERORR: can_set_int invalid row=%d < 0\n", row);
        exit(EXIT_FAILURE);
    }

    int found_col = -1;
    for (int j = 0; j < df->n_col; j++)
    {
        if (strcmp(col, df->cols[j]) == 0)
        {
            found_col = j;
            break;
        }
    }
    if (found_col == -1)
    {
        fprintf(stderr, "ERORR: can_set_int cannot found col=%s\n", col);
        exit(EXIT_FAILURE);
    }
    else if (df->dtypes[found_col] != 'I')
    {
        fprintf(stderr, "ERORR: can_set_int found col=%s but it is not char type\n", col);
        exit(EXIT_FAILURE);
    }
    ((int *)df->values[found_col])[row] = value;
}

/// @brief set double type value of df
/// @param df    IO dataframe
/// @param row   I number of row
/// @param col   I column name
/// @param value I given double type value
void can_set_double(can_dataframe *df, int row, char col[MAX_COL_LEN], double value)
{
    if (row >= df->n_row)
    {
        fprintf(stderr, "ERORR: can_set_double row=%d >= df->n_row\n", row);
        exit(EXIT_FAILURE);
    }
    else if (row < 0)
    {
        fprintf(stderr, "ERORR: can_set_double invalid row=%d < 0\n", row);
        exit(EXIT_FAILURE);
    }

    int found_col = -1;
    for (int j = 0; j < df->n_col; j++)
    {
        if (strcmp(col, df->cols[j]) == 0)
        {
            found_col = j;
            break;
        }
    }
    if (found_col == -1)
    {
        fprintf(stderr, "ERORR: can_set_double cannot found col=%s\n", col);
        exit(EXIT_FAILURE);
    }
    else if (df->dtypes[found_col] != 'D')
    {
        fprintf(stderr, "ERORR: can_set_double found col=%s but it is not char type\n", col);
        exit(EXIT_FAILURE);
    }
    ((double *)df->values[found_col])[row] = value;
}

/// @brief set char type value of df
/// @param df    IO dataframe
/// @param row   I number of row
/// @param col   I column name
/// @param value I given char type value
void can_set_char(can_dataframe *df, int row, char col[MAX_COL_LEN], char value)
{
    if (row >= df->n_row)
    {
        fprintf(stderr, "ERORR: can_set_char row=%d >= df->n_row\n", row);
        exit(EXIT_FAILURE);
    }
    else if (row < 0)
    {
        fprintf(stderr, "ERORR: can_set_char invalid row=%d < 0\n", row);
        exit(EXIT_FAILURE);
    }

    int found_col = -1;
    for (int j = 0; j < df->n_col; j++)
    {
        if (strcmp(col, df->cols[j]) == 0)
        {
            found_col = j;
            break;
        }
    }
    if (found_col == -1)
    {
        fprintf(stderr, "ERORR: can_set_char cannot found col=%s\n", col);
        exit(EXIT_FAILURE);
    }
    else if (df->dtypes[found_col] != 'C')
    {
        fprintf(stderr, "ERORR: can_set_char found col=%s but it is not char type\n", col);
        exit(EXIT_FAILURE);
    }
    ((char *)df->values[found_col])[row] = value;
}

/// @brief return the pointer to df's col of integer type
/// @param df  I dataframe
/// @param col I column name
/// @return integer pointer (shallow copy)
int *can_get_int_pointer(can_dataframe *df, char col[MAX_COL_LEN])
{
    // check if col exist, and data type is int
    int found_col = -1;
    for (int j = 0; j < df->n_col; j++)
    {
        if (strcmp(col, df->cols[j]) == 0)
        {
            found_col = j;
            break;
        }
    }
    if (found_col == -1)
    {
        fprintf(stderr, "ERORR: can_get_int_pointer cannot found col=%s\n", col);
        exit(EXIT_FAILURE);
    }
    else if (df->dtypes[found_col] != 'I')
    {
        fprintf(stderr, "ERORR: can_get_int_pointer found col=%s, but it is not integer type\n", col);
        exit(EXIT_FAILURE);
    }
    return df->values[found_col];
}

/// @brief return the pointer to df's col of double type
/// @param df  I dataframe
/// @param col I column name
/// @return double pointer (shallow copy)
double *can_get_double_pointer(can_dataframe *df, char col[MAX_COL_LEN])
{
    // check if col exist, and data type is double
    int found_col = -1;
    for (int j = 0; j < df->n_col; j++)
    {
        if (strcmp(col, df->cols[j]) == 0)
        {
            found_col = j;
            break;
        }
    }
    if (found_col == -1)
    {
        fprintf(stderr, "ERORR: can_get_double_pointer cannot found col=%s\n", col);
        exit(EXIT_FAILURE);
    }
    else if (df->dtypes[found_col] != 'D')
    {
        fprintf(stderr, "ERORR: can_get_double_pointer found col=%s, but it is not double type\n", col);
        exit(EXIT_FAILURE);
    }
    return df->values[found_col];
}

/// @brief return the pointer to df's col of char type
/// @param df  I dataframe
/// @param col I column name
/// @return char pointer (shallow copy)
char *can_get_char_pointer(can_dataframe *df, char col[MAX_COL_LEN])
{
    // check if col exist, and data type is char
    int found_col = -1;
    for (int j = 0; j < df->n_col; j++)
    {
        if (strcmp(col, df->cols[j]) == 0)
        {
            found_col = j;
            break;
        }
    }
    if (found_col == -1)
    {
        fprintf(stderr, "ERORR: can_get_char_pointer cannot found col=%s\n", col);
        exit(EXIT_FAILURE);
    }
    else if (df->dtypes[found_col] != 'C')
    {
        fprintf(stderr, "ERORR: can_get_char_pointer found col=%s, but it is not char type\n", col);
        exit(EXIT_FAILURE);
    }
    return df->values[found_col];
}

/// @brief select one column by name from dataframe
/// @param df  I dataframe
/// @param col I column name
/// @return sub dataframe (deep copy)
can_dataframe *can_select_col(const can_dataframe *df, char col[MAX_COL_LEN])
{
    int found_col = -1;
    for (int j = 0; j < df->n_col; j++)
    {
        if (strcmp(col, df->cols[j]) == 0)
        {
            found_col = j;
            break;
        }
    }
    if (found_col == -1)
    {
        fprintf(stderr, "ERORR: can_select_col cannot found col=%s\n", col);
        exit(EXIT_FAILURE);
    }

    char cols[MAX_COL_NUM][MAX_COL_LEN] = {""};
    strncpy(cols[0], col, MAX_COL_LEN);
    char dtypes[MAX_COL_NUM] = "";
    dtypes[0] = df->dtypes[found_col];

    void *values[MAX_COL_NUM] = {df->values[found_col]};
    can_dataframe *res = can_alloc(df->n_row, 1, cols, dtypes, values);
    return res;
}

/// @brief select multiple columns from a dataframe
/// @param df    I dataframe
/// @param n_col I number of columns selected
/// @param cols  I column names
/// @return sub dataframe (deep copy)
can_dataframe *can_select_cols(const can_dataframe *df, int n_col, char cols[MAX_COL_NUM][MAX_COL_LEN])
{
    char dtypes[MAX_COL_NUM] = "";
    void *values[MAX_COL_NUM] = {NULL};

    for (int k = 0; k < n_col; k++)
    {
        char *col = cols[k];
        int found_col = -1;
        for (int j = 0; j < df->n_col; j++)
        {
            if (strcmp(col, df->cols[j]) == 0)
            {
                found_col = j;
                break;
            }
        }
        if (found_col == -1)
        {
            fprintf(stderr, "ERORR: can_select_cols cannot found col=%s\n", col);
            exit(EXIT_FAILURE);
        }
        dtypes[k] = df->dtypes[found_col];
        values[k] = df->values[found_col];
    }
    can_dataframe *res = can_alloc(df->n_row, n_col, cols, dtypes, values);
    return res;
}

/// @brief select one row from dataframe
/// @param df  I dataframe
/// @param row I number of row
/// @return sub dataframe (deep copy)
can_dataframe *can_select_row(const can_dataframe *df, int row)
{
    if (row >= df->n_row)
    {
        fprintf(stderr, "ERORR: can_select_row row=%d >= df->n_row\n", row);
        exit(EXIT_FAILURE);
    }
    else if (row < 0)
    {
        fprintf(stderr, "ERORR: can_select_row invalid row=%d < 0\n", row);
        exit(EXIT_FAILURE);
    }

    void *values[MAX_COL_NUM] = {NULL};
    for (int j = 0; j < df->n_col; j++)
    {
        values[j] = df->values[j] + row;
    }

    can_dataframe *res = can_alloc(1, df->n_col, df->cols, df->dtypes, values);
    return res;
}

/// @brief select multiple rows (not necessarily in ascending order) from a dataframe
/// @param df    I dataframe
/// @param n_row I number of rows
/// @param rows  I rows number
/// @return sub dataframe (deep copy)
can_dataframe *can_select_rows(const can_dataframe *df, int n_row, int *rows)
{
    for (int k = 0; k < n_row; k++)
    {
        int row = rows[k];
        if (row >= df->n_row)
        {
            fprintf(stderr, "ERORR: can_select_rows row=%d >= df->n_row\n", row);
            exit(EXIT_FAILURE);
        }
        else if (row < 0)
        {
            fprintf(stderr, "ERORR: can_select_rows invalid row=%d < 0\n", row);
            exit(EXIT_FAILURE);
        }
    }

    // assign capacity, values here will be replaced
    can_dataframe *res = can_alloc(n_row, df->n_col, df->cols, df->dtypes, (void **)df->values); // (void**) here do nothing but prevent [-Wdiscarded-qualifiers] warning

    // replace values
    // for selected rows (not all rows)
    for (int i = 0; i < n_row; i++)
    {
        // for all column
        for (int j = 0; j < res->n_col; j++)
        {
            if (df->dtypes[j] == 'I')
            {
                int v = can_get_int(df, rows[i], (char *)df->cols[j]); // (char*) here do nothing but prevent [-Wdiscarded-qualifiers] warning
                can_set_int(res, i, res->cols[j], v);
            }
            else if (df->dtypes[j] == 'D')
            {
                double v = can_get_double(df, rows[i], (char *)df->cols[j]); // (char*) here do nothing but prevent [-Wdiscarded-qualifiers] warning
                can_set_double(res, i, res->cols[j], v);
            }
            else if (df->dtypes[j] == 'C')
            {
                char v = can_get_char(df, rows[i], (char *)df->cols[j]); // (char*) here do nothing but prevent [-Wdiscarded-qualifiers] warning
                can_set_char(res, i, res->cols[j], v);
            }
        }
    }
    return res;
}

/// @brief filter rows that have value of col between min and max
/// @param df  I dataframe
/// @param col I col name that have double date type
/// @param min I min value
/// @param max I max value
/// @return     filtered dataframe
can_dataframe *can_filter_double(const can_dataframe *df, char col[MAX_COL_LEN], double min, double max)
{
    int found_col = -1;
    for (int j = 0; j < df->n_col; j++)
    {
        if (strcmp(col, df->cols[j]) == 0)
        {
            found_col = j;
            break;
        }
    }
    if (found_col == -1)
    {
        fprintf(stderr, "ERORR: can_filter_double cannot found col=%s\n", col);
        exit(EXIT_FAILURE);
    }
    else if (df->dtypes[found_col] != 'D')
    {
        fprintf(stderr, "ERORR: can_filter_double found col=%s but it is not double type\n", col);
        exit(EXIT_FAILURE);
    }

    // count how many valid value
    double *vs = (double *)df->values[found_col];
    int n_row = 0;
    for (int i = 0; i < df->n_row; i++)
    {
        if (vs[i] >= min && vs[i] <= max)
        {
            n_row++;
        }
    }

    // assign capacity, values here will be replaced
    can_dataframe *res = can_alloc(n_row, df->n_col, df->cols, df->dtypes, (void **)df->values); // (void**) here do nothing but prevent [-Wdiscarded-qualifiers] warning

    // replace values
    // for all rows that satisfy condition
    int row_i = 0;
    for (int i = 0; i < df->n_row; i++)
    {
        // if satisfy condition
        double compare = can_get_double(df, i, col);
        if (compare >= min && compare <= max)
        {
            // for all column copy value
            for (int j = 0; j < res->n_col; j++)
            {
                if (df->dtypes[j] == 'I')
                {
                    int v = can_get_int(df, i, (char *)df->cols[j]); // (char*) here do nothing but prevent [-Wdiscarded-qualifiers] warning
                    can_set_int(res, row_i, res->cols[j], v);
                }
                else if (df->dtypes[j] == 'D')
                {
                    double v = can_get_double(df, i, (char *)df->cols[j]); // (char*) here do nothing but prevent [-Wdiscarded-qualifiers] warning
                    can_set_double(res, row_i, res->cols[j], v);
                }
                else if (df->dtypes[j] == 'C')
                {
                    char v = can_get_char(df, i, (char *)df->cols[j]); // (char*) here do nothing but prevent [-Wdiscarded-qualifiers] warning
                    can_set_char(res, row_i, res->cols[j], v);
                }
            }
            row_i++;
        }
    }
    return res;
}

/// @brief filter rows that have value of col between min and max
/// @param df  I dataframe
/// @param col I col name that have int date type
/// @param min I min value
/// @param max I max value
/// @return     filtered dataframe
can_dataframe *can_filter_int(const can_dataframe *df, char col[MAX_COL_LEN], int min, int max)
{
    int found_col = -1;
    for (int j = 0; j < df->n_col; j++)
    {
        if (strcmp(col, df->cols[j]) == 0)
        {
            found_col = j;
            break;
        }
    }
    if (found_col == -1)
    {
        fprintf(stderr, "ERORR: can_filter_int cannot found col=%s\n", col);
        exit(EXIT_FAILURE);
    }
    else if (df->dtypes[found_col] != 'I')
    {
        fprintf(stderr, "ERORR: can_filter_int found col=%s but it is not int type\n", col);
        exit(EXIT_FAILURE);
    }

    // count how many valid value
    int *vs = (int *)df->values[found_col];
    int n_row = 0;
    for (int i = 0; i < df->n_row; i++)
    {
        if (vs[i] >= min && vs[i] <= max)
        {
            n_row++;
        }
    }

    // assign capacity, values here will be replaced
    can_dataframe *res = can_alloc(n_row, df->n_col, df->cols, df->dtypes, (void **)df->values); // (void**) here do nothing but prevent [-Wdiscarded-qualifiers] warning

    // replace values
    // for all rows that satisfy condition
    int row_i = 0;
    for (int i = 0; i < df->n_row; i++)
    {
        // if satisfy condition
        int compare = can_get_int(df, i, col);
        if (compare >= min && compare <= max)
        {
            // for all column copy value
            for (int j = 0; j < res->n_col; j++)
            {
                if (df->dtypes[j] == 'I')
                {
                    int v = can_get_int(df, i, (char *)df->cols[j]); // (char*) here do nothing but prevent [-Wdiscarded-qualifiers] warning
                    can_set_int(res, row_i, res->cols[j], v);
                }
                else if (df->dtypes[j] == 'D')
                {
                    double v = can_get_double(df, i, (char *)df->cols[j]); // (char*) here do nothing but prevent [-Wdiscarded-qualifiers] warning
                    can_set_double(res, row_i, res->cols[j], v);
                }
                else if (df->dtypes[j] == 'C')
                {
                    char v = can_get_char(df, i, (char *)df->cols[j]); // (char*) here do nothing but prevent [-Wdiscarded-qualifiers] warning
                    can_set_char(res, row_i, res->cols[j], v);
                }
            }
            row_i++;
        }
    }
    return res;
}

/// @brief filter rows that have value of col between min and max
/// @param df  I dataframe
/// @param col I col name that have char date type
/// @param min I min value
/// @param max I max value
/// @return     filtered dataframe
can_dataframe *can_filter_char(const can_dataframe *df, char col[MAX_COL_LEN], char min, char max)
{
    int found_col = -1;
    for (int j = 0; j < df->n_col; j++)
    {
        if (strcmp(col, df->cols[j]) == 0)
        {
            found_col = j;
            break;
        }
    }
    if (found_col == -1)
    {
        fprintf(stderr, "ERORR: can_filter_char cannot found col=%s\n", col);
        exit(EXIT_FAILURE);
    }
    else if (df->dtypes[found_col] != 'C')
    {
        fprintf(stderr, "ERORR: can_filter_char found col=%s but it is not char type\n", col);
        exit(EXIT_FAILURE);
    }

    // count how many valid value
    char *vs = (char *)df->values[found_col];
    int n_row = 0;
    for (int i = 0; i < df->n_row; i++)
    {
        if (vs[i] >= min && vs[i] <= max)
        {
            n_row++;
        }
    }

    // assign capacity, values here will be replaced
    can_dataframe *res = can_alloc(n_row, df->n_col, df->cols, df->dtypes, (void **)df->values); // (void**) here do nothing but prevent [-Wdiscarded-qualifiers] warning

    // replace values
    // for all rows that satisfy condition
    int row_i = 0;
    for (int i = 0; i < df->n_row; i++)
    {
        // if satisfy condition
        char compare = can_get_char(df, i, col);
        if (compare >= min && compare <= max)
        {
            // for all column copy value
            for (int j = 0; j < res->n_col; j++)
            {
                if (df->dtypes[j] == 'I')
                {
                    int v = can_get_int(df, i, (char *)df->cols[j]); // (char*) here do nothing but prevent [-Wdiscarded-qualifiers] warning
                    can_set_int(res, row_i, res->cols[j], v);
                }
                else if (df->dtypes[j] == 'D')
                {
                    double v = can_get_double(df, i, (char *)df->cols[j]); // (char*) here do nothing but prevent [-Wdiscarded-qualifiers] warning
                    can_set_double(res, row_i, res->cols[j], v);
                }
                else if (df->dtypes[j] == 'C')
                {
                    char v = can_get_char(df, i, (char *)df->cols[j]); // (char*) here do nothing but prevent [-Wdiscarded-qualifiers] warning
                    can_set_char(res, row_i, res->cols[j], v);
                }
            }
            row_i++;
        }
    }
    return res;
}

/// @brief concatenate two dataframe by row
/// @param df1 I dataframe 1
/// @param df2 I dataframe 2
/// @return concatenated dataframe
can_dataframe *can_concat_row(const can_dataframe *df1, const can_dataframe *df2)
{
    // check if have same cols
    if (df1->n_col != df2->n_col)
    {
        fprintf(stderr, "ERROR: can_concat_row df1->n_col != df2->n_col\n");
        exit(EXIT_FAILURE);
    }
    for (int j = 0; j < df1->n_col; j++)
    {
        if (strcmp(df1->cols[j], df2->cols[j]) != 0)
        {
            fprintf(stderr, "ERROR: can_concat_row column of df1 %s have different name with df2\n", df1->cols[j]);
            exit(EXIT_FAILURE);
        }

        if (df1->dtypes[j] != df2->dtypes[j])
        {
            fprintf(stderr, "ERROR: can_concat_row column of df1 %s have different data type with df2\n", df1->cols[j]);
            exit(EXIT_FAILURE);
        }
    }

    can_dataframe *res = can_alloc(df1->n_row + df2->n_row, df1->n_col, df1->cols, df1->dtypes, (void **)df1->values); // (void**) here do nothing but prevent [-Wdiscarded-qualifiers] warning

    // copy value of df2
    int row_i = df1->n_row;
    for (int i = 0; i < df2->n_row; i++)
    {
        // for all column copy value
        for (int j = 0; j < res->n_col; j++)
        {
            if (df2->dtypes[j] == 'I')
            {
                int v = can_get_int(df2, i, (char *)df2->cols[j]); // (char*) here do nothing but prevent [-Wdiscarded-qualifiers] warning
                can_set_int(res, row_i, res->cols[j], v);
            }
            else if (df2->dtypes[j] == 'D')
            {
                double v = can_get_double(df2, i, (char *)df2->cols[j]); // (char*) here do nothing but prevent [-Wdiscarded-qualifiers] warning
                can_set_double(res, row_i, res->cols[j], v);
            }
            else if (df2->dtypes[j] == 'C')
            {
                char v = can_get_char(df2, i, (char *)df2->cols[j]); // (char*) here do nothing but prevent [-Wdiscarded-qualifiers] warning
                can_set_char(res, row_i, res->cols[j], v);
            }
        }
        row_i++;
    }

    return res;
}

/// @brief concatenate two dataframe by col
/// @param df1 I dataframe 1
/// @param df2 I dataframe 2
/// @return concatenated dataframe
can_dataframe *can_concat_col(const can_dataframe *df1, const can_dataframe *df2)
{
    // check they have same row
    if (df1->n_row != df2->n_row)
    {
        fprintf(stderr, "ERORR: can_concat_col df1->n_row = %d != df2->n_row = %d\n", df1->n_row, df2->n_row);
        exit(EXIT_FAILURE);
    }

    // check if their col add up exceed MAX_COL_NUM
    if (df1->n_col + df2->n_col > MAX_COL_NUM)
    {
        fprintf(stderr, "ERORR: can_concat_col df1->n_col + df2->n_col = %d > MAX_COL_NUM = %d\n", df1->n_col + df2->n_col, MAX_COL_NUM);
        exit(EXIT_FAILURE);
    }

    char cols[MAX_COL_NUM][MAX_COL_LEN] = {""};
    char dtypes[MAX_COL_NUM] = "";
    void *values[MAX_COL_NUM] = {NULL};
    for (int j = 0; j < df1->n_col; j++)
    {
        strncpy(cols[j], df1->cols[j], MAX_COL_LEN);
        dtypes[j] = df1->dtypes[j];
        values[j] = df1->values[j];
    }
    for (int j = df1->n_col; j < df1->n_col + df2->n_col; j++)
    {
        strncpy(cols[j], df2->cols[j - df1->n_col], MAX_COL_LEN);
        dtypes[j] = df2->dtypes[j - df1->n_col];
        values[j] = df2->values[j - df1->n_col];
    }
    can_dataframe *res = can_alloc(df1->n_row, df1->n_col + df2->n_col, cols, dtypes, values);
    return res;
}

/// @brief merge two dataframe, keep left dataframe unchange,
/// (df2's key col should be unique, if not unique, first found will be matched)
/// (should be very careful when key col is double type because of "0.1+0.2!=0.3" problem of float numbers)
/// @param df1     I left dataframe
/// @param df2     I right dataframe
/// @param key_col I the key column name
/// @return merged dataframe (deep copy)
can_dataframe *can_merge_left(const can_dataframe *df1, const can_dataframe *df2, char key_col[MAX_COL_LEN])
{
    // check if their col add up exceed MAX_COL_NUM (-1 because only keep left key col)
    if (df1->n_col + df2->n_col - 1 > MAX_COL_NUM)
    {
        fprintf(stderr, "ERORR: can_merge_left df1->n_col + df2->n_col - 1 = %d > MAX_COL_NUM = %d\n", df1->n_col + df2->n_col, MAX_COL_NUM);
        exit(EXIT_FAILURE);
    }

    // check they both have key col
    int found_col1 = -1;
    for (int j = 0; j < df1->n_col; j++)
    {
        if (strcmp(key_col, df1->cols[j]) == 0)
        {
            found_col1 = j;
            break;
        }
    }
    if (found_col1 == -1)
    {
        fprintf(stderr, "ERORR: can_merge_left df1 do not have key_col %s\n", key_col);
        exit(EXIT_FAILURE);
    }
    int found_col2 = -1;
    for (int j = 0; j < df2->n_col; j++)
    {
        if (strcmp(key_col, df2->cols[j]) == 0)
        {
            found_col2 = j;
            break;
        }
    }
    if (found_col2 == -1)
    {
        fprintf(stderr, "ERORR: can_merge_left df1 have key_col %s, but df2 do not have key_col\n", key_col);
        exit(EXIT_FAILURE);
    }

    // check if their key col are of same type
    if (df1->dtypes[found_col1] != df2->dtypes[found_col2])
    {
        fprintf(stderr, "ERORR: can_merge_left df1, df2 have same key col but different type %c, %c\n", df1->dtypes[found_col1], df2->dtypes[found_col2]);
        exit(EXIT_FAILURE);
    }

    char cols[MAX_COL_NUM][MAX_COL_LEN] = {""};
    char dtypes[MAX_COL_NUM] = "";
    void *values[MAX_COL_NUM] = {NULL};
    for (int j = 0; j < df1->n_col; j++)
    {
        strncpy(cols[j], df1->cols[j], MAX_COL_LEN);
        dtypes[j] = df1->dtypes[j];
        values[j] = df1->values[j];
    }
    int col_i = df1->n_col;
    for (int j = df1->n_col; j < df1->n_col + df2->n_col; j++)
    {
        if (strcmp(key_col, df2->cols[j - df1->n_col]) == 0)
        {
            continue;
        }
        strncpy(cols[col_i], df2->cols[j - df1->n_col], MAX_COL_LEN);
        dtypes[col_i] = df2->dtypes[j - df1->n_col];
        values[col_i] = df2->values[j - df1->n_col]; // invalid values, just placeholder
        col_i++;
    }
    can_dataframe *res = can_alloc(df1->n_row, df1->n_col + df2->n_col - 1, cols, dtypes, values);

    // match key col and fill value
    for (int i1 = 0; i1 < df1->n_row; i1++)
    {
        char key_col_dtype = df1->dtypes[found_col1];
        if (key_col_dtype == 'I')
        {
            int v1 = can_get_int(df1, i1, key_col);
            for (int i2 = 0; i2 < df2->n_row; i2++)
            {
                int v2 = can_get_int(df2, i2, key_col);
                if (v1 == v2) // matched
                {
                    // for every col in df2 except key col (found_col2)
                    for (int j2 = 0; j2 < df2->n_col; j2++)
                    {
                        if (j2 == found_col2)
                        {
                            continue;
                        }
                        if (df2->dtypes[j2] == 'I')
                        {
                            int v = can_get_int(df2, i2, (char *)df2->cols[j2]); // (char*) here do nothing but prevent [-Wdiscarded-qualifiers] warning
                            can_set_int(res, i1, (char *)df2->cols[j2], v);
                        }
                        else if (df2->dtypes[j2] == 'D')
                        {
                            double v = can_get_double(df2, i2, (char *)df2->cols[j2]); // (char*) here do nothing but prevent [-Wdiscarded-qualifiers] warning
                            can_set_double(res, i1, (char *)df2->cols[j2], v);
                        }
                        else if (df2->dtypes[j2] == 'C')
                        {
                            char v = can_get_char(df2, i2, (char *)df2->cols[j2]); // (char*) here do nothing but prevent [-Wdiscarded-qualifiers] warning
                            can_set_char(res, i1, (char *)df2->cols[j2], v);
                        }
                    }
                }
            }
        }

        else if (key_col_dtype == 'D')
        {
            double v1 = can_get_double(df1, i1, key_col);
            for (int i2 = 0; i2 < df2->n_row; i2++)
            {
                double v2 = can_get_double(df2, i2, key_col);
                if (v1 == v2) // matched
                {
                    // for every col in df2 except key col (found_col2)
                    for (int j2 = 0; j2 < df2->n_col; j2++)
                    {
                        if (j2 == found_col2)
                        {
                            continue;
                        }
                        if (df2->dtypes[j2] == 'I')
                        {
                            int v = can_get_int(df2, i2, (char *)df2->cols[j2]); // (char*) here do nothing but prevent [-Wdiscarded-qualifiers] warning
                            can_set_int(res, i1, (char *)df2->cols[j2], v);
                        }
                        else if (df2->dtypes[j2] == 'D')
                        {
                            double v = can_get_double(df2, i2, (char *)df2->cols[j2]); // (char*) here do nothing but prevent [-Wdiscarded-qualifiers] warning
                            can_set_double(res, i1, (char *)df2->cols[j2], v);
                        }
                        else if (df2->dtypes[j2] == 'C')
                        {
                            char v = can_get_char(df2, i2, (char *)df2->cols[j2]); // (char*) here do nothing but prevent [-Wdiscarded-qualifiers] warning
                            can_set_char(res, i1, (char *)df2->cols[j2], v);
                        }
                    }
                }
            }
        }

        else if (key_col_dtype == 'C')
        {
            char v1 = can_get_char(df1, i1, key_col);
            for (int i2 = 0; i2 < df2->n_row; i2++)
            {
                char v2 = can_get_char(df2, i2, key_col);
                if (v1 == v2) // matched
                {
                    // for every col in df2 except key col (found_col2)
                    for (int j2 = 0; j2 < df2->n_col; j2++)
                    {
                        if (j2 == found_col2)
                        {
                            continue;
                        }
                        if (df2->dtypes[j2] == 'I')
                        {
                            int v = can_get_int(df2, i2, (char *)df2->cols[j2]); // (char*) here do nothing but prevent [-Wdiscarded-qualifiers] warning
                            can_set_int(res, i1, (char *)df2->cols[j2], v);
                        }
                        else if (df2->dtypes[j2] == 'D')
                        {
                            double v = can_get_double(df2, i2, (char *)df2->cols[j2]); // (char*) here do nothing but prevent [-Wdiscarded-qualifiers] warning
                            can_set_double(res, i1, (char *)df2->cols[j2], v);
                        }
                        else if (df2->dtypes[j2] == 'C')
                        {
                            char v = can_get_char(df2, i2, (char *)df2->cols[j2]); // (char*) here do nothing but prevent [-Wdiscarded-qualifiers] warning
                            can_set_char(res, i1, (char *)df2->cols[j2], v);
                        }
                    }
                }
            }
        }
    }

    return res;
}

/// @brief helper function for can_sort
/// @param a
/// @param b
/// @return
static int can_compare_int(const void *a, const void *b)
{
    return (*(int *)a - *(int *)b);
}
/// @brief helper function for can_sort
/// @param a
/// @param b
/// @return
static int can_compare_double(const void *a, const void *b)
{
    double diff = (*(double *)a - *(double *)b);
    if (diff == 0.0)
    {
        return 0;
    }
    else
    {
        return diff > 0.0 ? 1 : -1;
    }
}
/// @brief helper function for can_sort
/// @param a
/// @param b
/// @return
static int can_compare_char(const void *a, const void *b)
{
    return (*(char *)a - *(char *)b);
}

/// @brief sort dataframe in ascending order
/// @param df      I dataframe
/// @param key_col I key column name
/// @return sorted dataframe
can_dataframe *can_sort(const can_dataframe *df, char key_col[MAX_COL_LEN])
{
    int found_col = -1;
    for (int j = 0; j < df->n_col; j++)
    {
        if (strcmp(key_col, df->cols[j]) == 0)
        {
            found_col = j;
            break;
        }
    }
    if (found_col == -1)
    {
        fprintf(stderr, "ERROR: can_sort df do not have key col %s\n", key_col);
        exit(EXIT_FAILURE);
    }

    // make same structure without data
    can_dataframe *res = can_alloc(df->n_row, df->n_col, df->cols, df->dtypes, NULL);
    // find the sorted order
    int *order = malloc(sizeof(int) * df->n_row);

    char key_col_type = df->dtypes[found_col];
    if (key_col_type == 'I')
    {
        // make a copy of key col, and sort it by qsort
        int *vp = malloc(sizeof(int) * df->n_row);
        int order_i = 0;
        memcpy(vp, can_get_int_pointer((can_dataframe *)df, key_col), sizeof(int) * df->n_row);
        qsort(vp, df->n_row, sizeof(int), can_compare_int);

        // use the sorted copy as reference to get sorted order
        for (int i = 0; i < res->n_row; i++)
        {
            int v = vp[i];
            // for each row in df, search equal value
            for (int row_i = 0; row_i < df->n_row; row_i++)
            {
                // if multiple rows have same value on key col, make sure they won't repeat (each row can only appear once)
                if (v == can_get_int(df, row_i, key_col))
                {
                    int found = 0;
                    for (int k = 0; k < order_i; k++)
                    {
                        if (row_i == order[k])
                        {
                            found = 1;
                            break;
                        }
                    }
                    // if not in order list, add it
                    if (found == 0)
                    {
                        order[order_i] = row_i;
                        order_i++;
                    }
                }
            }
        }
        free(vp);
    }
    else if (key_col_type == 'D')
    {
        // make a copy of key col, and sort it by qsort
        double *vp = malloc(sizeof(double) * df->n_row);
        int order_i = 0;
        memcpy(vp, can_get_double_pointer((can_dataframe *)df, key_col), sizeof(double) * df->n_row);
        qsort(vp, df->n_row, sizeof(double), can_compare_double);

        // use the sorted copy as reference to get sorted order
        for (int i = 0; i < res->n_row; i++)
        {
            double v = vp[i];
            // for each row in df, search equal value
            for (int row_i = 0; row_i < df->n_row; row_i++)
            {
                // if multiple rows have same value on key col, make sure they won't repeat (each row can only appear once)
                if (v == can_get_double(df, row_i, key_col))
                {
                    int found = 0;
                    for (int k = 0; k < order_i; k++)
                    {
                        if (row_i == order[k])
                        {
                            found = 1;
                            break;
                        }
                    }
                    // if not in order list, add it
                    if (found == 0)
                    {
                        order[order_i] = row_i;
                        order_i++;
                    }
                }
            }
        }
        free(vp);
    }
    else if (key_col_type == 'D')
    {
        // make a copy of key col, and sort it by qsort
        char *vp = malloc(sizeof(char) * df->n_row);
        int order_i = 0;
        memcpy(vp, can_get_char_pointer((can_dataframe *)df, key_col), sizeof(char) * df->n_row);
        qsort(vp, df->n_row, sizeof(char), can_compare_char);

        // use the sorted copy as reference to get sorted order
        for (int i = 0; i < res->n_row; i++)
        {
            char v = vp[i];
            // for each row in df, search equal value
            for (int row_i = 0; row_i < df->n_row; row_i++)
            {
                // if multiple rows have same value on key col, make sure they won't repeat (each row can only appear once)
                if (v == can_get_char(df, row_i, key_col))
                {
                    int found = 0;
                    for (int k = 0; k < order_i; k++)
                    {
                        if (row_i == order[k])
                        {
                            found = 1;
                            break;
                        }
                    }
                    // if not in order list, add it
                    if (found == 0)
                    {
                        order[order_i] = row_i;
                        order_i++;
                    }
                }
            }
        }
        free(vp);
    }

    // now set the value of res, according to the sorted order
    for(int i = 0;i<res->n_row;i++)
    {
        for(int j=0;j<res->n_col;j++)
        {
            char col_type = res->dtypes[j];
            if(col_type=='I')
            {
                can_set_int(res, i, res->cols[j], can_get_int(df, order[i], res->cols[j]));
            }
            else if(col_type=='D')
            {
                can_set_double(res, i, res->cols[j], can_get_double(df, order[i], res->cols[j]));
            }
            else if(col_type=='C')
            {
                can_set_char(res, i, res->cols[j], can_get_char(df, order[i], res->cols[j]));
            }
        }
    }

    free(order);
    return res;
}

#endif