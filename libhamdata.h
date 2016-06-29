/*
 * Copyright (C) 2016 kcotugno
 * All rights reserved
 *
 * Distributed under the terms of the BSD 2 Clause software license. See the
 * accompanying LICENSE file or http://www.opensource.org/licenses/BSD-2-Clause.
 *
 * libhamdata: libhamdata.h
 *
 * Author: kcotugno
 * Date: 6/12/2016
 */

#ifndef _LIBHAMDATA_H_
#define _LIBHAMDATA_H_

#if defined(_WIN32)
    #define OS_WIN
#else /* TODO make more specific OS identification */
    #define OS_GENERIC
#endif

#if defined(OS_WIN)
    #if defined(LIBHAMDATA_EXPORTS)
        #define LIBHAMDATA_API __declspec(dllexport)
    #else
        #define LIBHAMDATA_API __declspec(dllimport)
    #endif
#else
    #define LIBHAMDATA_API extern
#endif

#define INT64 int64_t

/* Return code */
#define HAM_OK 0

#define HAM_ERROR_GENERIC 100
#define HAM_ERROR_MALLOC_FAIL 101
#define HAM_ERROR_OPEN_FILE 102
#define HAM_ERROR_DIR_TOO_LONG 103

#define HAM_ERROR_SQLITE_CREATE_FILE 201
#define HAM_ERROR_SQLITE_OPEN_DATABASE_CONNECTION 202
#define HAM_ERROR_SQLITE_CREATE_TABLES 203
#define HAM_ERROR_SQLITE_INSERT_FAIL 204
#define HAM_ERROR_SQLITE_BACKUP_FAIL 205

/* Generic bool */
#define HAM_BOOL_NO 0
#define HAM_BOOL_YES 1

/*
 * Max length the directory can be. This will be subtracted by 7 in the test so we know the file
 * name can be appended.
 */
#define HAM_MAX_DIR_LENGTH 2048

/* FCC Database structure */
typedef struct ham_fcc_database ham_fcc_database;

/*
 * Initializer and terminator.
 *
 * This should return HAM_OK. If a files fails to open, HAM_ERROR_OPEN_FILE is returned. Further,
 * no ham_fcc_database is allocated. In this case, do not call ham_fcc_terminate!
 */
LIBHAMDATA_API int ham_fcc_init_directory(ham_fcc_database **database, char *directory,
                            const int include_optional);
LIBHAMDATA_API int ham_fcc_terminate(ham_fcc_database **database);

/* Conversion functions */
LIBHAMDATA_API int ham_fcc_to_sqlite(const ham_fcc_database *fcc_database, const int save_in_ram);

#endif /* _LIBHANDATA_H_ */
