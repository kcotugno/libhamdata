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

#include <stdio.h>

#include "libhamdata.h"

int main (int argc, char **argv) {
    int save_in_ram = HAM_BOOL_YES;
    int optional = HAM_BOOL_NO;

    /* Check to include optional files and save in ram */
    if(argc > 1)
        if(argv[1][0] == '1')
            optional = HAM_BOOL_YES;

    ham_fcc_database *fccdb;

    if(ham_fcc_database_init(&fccdb, optional)) {
        printf("Error: failed to open files...\n\n"
                "Please place the FCC database files in the folder from which you run the program\n"
                "First argument of 1 will include optional files (default: 0)...\n");

        return 1;
    }

    if(ham_fcc_to_sqlite(fccdb))
        printf("Conversion failed\n");

    ham_fcc_terminate(&fccdb);
    return 0;
}
