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

    if(ham_fcc_database_init(&fccdb)) {
        printf("Error: failed to open files...\n\n"
                "Please place the FCC database files in the folder from which you run the program\n");

        return 1;
    }

    if(ham_fcc_to_sqlite(fccdb))
        printf("Conversion failed\n");

    ham_fcc_terminate(fccdb);
    return 0;
}
