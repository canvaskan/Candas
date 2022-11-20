# Candas
A Pandas-like dataframe written in pure C


@brief a Pandas-like Dataframe written in pure C (Header-only)

@version 0.2

@date 2022-11-18

@copyright Copyright (c) 2022


- only one new struct: can_dataframe
  
| row_number | ID   | DISTANCE | TAG  | ... |
|------------|------|----------|------|-----|
| /          | int  | double   | char | ... |
| 0          | 1001 | 1.1      | 'A'  | ... |
| 1          | 1008 | 2.4      | 'B'  | ... |
| 2          | 1033 | 9.9      | 'C'  | ... |
| ...        | ...  | ...      | ...  | ... |

- only use C standard library
- currently support int/double/char data type,
  specified by first character 'I'/'D'/'C'
- support read & write csv, filter by value, concatenate by row & col, merge(left), sort by value
- most functions (except get pointer) are deep copy,
  which means use can_free for every can_dataframe
- examples in main.c
