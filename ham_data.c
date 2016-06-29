/*
 * Copyright (C) 2016 kcotugno
 * All rights reserved
 *
 * Distributed under the terms of the BSD 2 Clause software license. See the
 * accompanying LICENSE file or http://www.opensource.org/licenses/BSD-2-Clause.
 *
 * libhamdata: ham_data.c
 *
 * Author: kcotugno
 * Date: 6/28/2016
 */

#include <ctype.h>
#include <stdio.h>

#include "libhamdata.h"

int main (int argc, char **argv) {
    int save_in_ram = HAM_BOOL_YES;
    int optional = HAM_BOOL_NO;
    char *path;

    /* A directory needs to be specified */
    if(argc < 2)
        path = "";
    else
        path = argv[1];

    /* Check to save in ram and include optional files */
    if(argc > 2)
        if(isdigit(argv[2][0]) == '0')
            save_in_ram = HAM_BOOL_NO;

        if(argc > 3)
            if(isdigit(argv[3][0]) == '1')
                optional = HAM_BOOL_YES;


    ham_fcc_database *fccdb;

    if(ham_fcc_init_directory(&fccdb, path, optional)) {
        printf("Error: failed to open files...\n\n"
                "First argument is the directory where the FCC file reside (defualt: current)\n"
                "Second argument of 0 will not use a faster memory database (default: 1)...\n"
                "Third argument of 1 will include optional files (default: 0)...\n");

        return 1;
    }

    ham_fcc_to_sqlite(fccdb, save_in_ram);
    ham_fcc_terminate(&fccdb);
    return 0;
}
