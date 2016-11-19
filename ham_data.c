/*
 * Copyright (C) 2016 Kevin Cotugno
 * All rights reserved
 *
 * Distributed under the terms of the MIT software license. See the
 * accompanying LICENSE file or http://www.opensource.org/licenses/MIT.
 *
 * libhamdata: ham_data.c
 *
 * Author: kcotugno
 * Date: 6/28/2016
 */

#include <stdio.h>

#include "libhamdata.h"

int main (int argc, char **argv) {
    ham_fcc_database *fccdb;

    char *filename = NULL;
    char *directory = NULL;

    if(argc > 1) {
        filename = argv[1];
    }

    if(argc > 2) {
        directory = argv[2];
    }

    if(ham_fcc_database_init(&fccdb, directory)) {
        printf("Error: failed to open files...\n\n"
               "Options paramaters:\n"
               "1: name of output file.\n"
               "2: directory of FCC files.\n");

        return 1;
    }
    int error = ham_fcc_to_sqlite(fccdb, filename);

    if(error)
        printf("Conversion failed: %d\n", error);

    ham_fcc_terminate(fccdb);
    return 0;
}
