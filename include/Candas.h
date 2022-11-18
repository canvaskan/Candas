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
 * @brief Pandas-like indexed DataFrame, currently only support int/double/char type
 *  col1[int]   col2[double]   col3[char]
 *     1         1.1            'A'
 *     8         2.4            'B'
 *     .          .              .
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
void can_print(const can_dataframe *df, int n_row);

// TODO
can_dataframe *can_select_col(const can_dataframe *df, char col);
can_dataframe *can_select_row(const can_dataframe *df, int i1, int i2);
can_dataframe *can_filter_double(const can_dataframe *df, char col, double min, double max);
can_dataframe *can_filter_int(const can_dataframe *df, char col, int min, int max);
can_dataframe *can_filter_char(const can_dataframe *df, char col, char c);
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
    can_dataframe *df = malloc(sizeof(can_dataframe));
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
            df->values[j] = malloc(sizeof(int) * n_row);
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
            df->values[j] = malloc(sizeof(double) * n_row);
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
            df->values[j] = malloc(sizeof(char) * n_row);
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
                printf("% lf\t", ((double *)(df->values[j]))[i]);
            }
            else if (df->dtypes[j] == 'C')
            {
                printf("%c\t", ((char *)(df->values[j]))[i]);
            }
        }
        printf("\n");
    }
}

#endif