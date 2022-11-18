/**
 * @file Candas.h
 * @author Haoyu Kan
 * @brief a Pandas-like Dataframe written in pure C
 * @version 0.1
 * @date 2022-11-18
 *
 * @copyright Copyright (c) 2022
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
#define MISS_CHAR '/'

/**
 * @brief Pandas-like DataFrame, currently only support int/double/char type
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

// TODO
can_dataframe *can_select_col(const can_dataframe *df, char col[MAX_COL_LEN]);
can_dataframe *can_select_cols(const can_dataframe *df, int n_col, char cols[MAX_COL_NUM][MAX_COL_LEN]);
can_dataframe *can_select_row(const can_dataframe *df, int row);
can_dataframe *can_select_rows(const can_dataframe *df, int n_row, int *rows);

can_dataframe *can_filter_double(const can_dataframe *df, char col[MAX_COL_LEN], double min, double max);
can_dataframe *can_filter_int(const can_dataframe *df, char col[MAX_COL_LEN], int min, int max);
can_dataframe *can_filter_char(const can_dataframe *df, char col[MAX_COL_LEN], char c);

can_dataframe *can_concat_row(const can_dataframe *df1, const can_dataframe *df2);
can_dataframe *can_concat_col(const can_dataframe *df1, const can_dataframe *df2);

can_dataframe *can_merge_inner(const can_dataframe *df1, const can_dataframe *df2, char key);
can_dataframe *can_merge_outer(const can_dataframe *df1, const can_dataframe *df2, char key);
can_dataframe *can_merge_left(const can_dataframe *df1, const can_dataframe *df2, char key);

// ================================================================================================

/// @brief init and alloc memory for can_dataframe
/// @param n_row  I number of rows (can reserve empty rows)
/// @param n_col  I number of cols (must be exact)
/// @param cols   I column names
/// @param dtypes I data types, e.g. "IDDC" means 4 cols with int, double, double, char
/// @param values I init values of columns (deep copy, only n_row), set NULL to do nothing
/// @return
can_dataframe *can_alloc(int n_row, int n_col, const char cols[MAX_COL_NUM][MAX_COL_LEN], const char dtypes[MAX_COL_NUM], void *values[MAX_COL_NUM])
{
    can_dataframe *df = (can_dataframe *)malloc(sizeof(can_dataframe));
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
    if(!fp)
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
        if(*pch == '\n') // empty line
        {
            printf("WARNING: can_read_csv detect empty line at line %d\n", i + 1 + skip_row);
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
    FILE* fp = fopen(file, "w");
    if(!fp)
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
    }
    else if (df->dtypes[found_col] != 'C')
    {
        fprintf(stderr, "ERORR: can_set_char found col=%s but it is not char type\n", col);
        exit(EXIT_FAILURE);
    }
    ((char *)df->values[found_col])[row] = value;
}

#endif