/*
 * Copyright (C) 2016 kcotugno
 * All rights reserved
 *
 * Distributed under the terms of the BSD 2 Clause software license. See the
 * accompanying LICENSE file or http://www.opensource.org/licenses/BSD-2-Clause.
 *
 * libhamdata: libhamdata.c
 *
 * Author: kcotugno
 * Date: 6/12/2016
 */

#include "libhamdata.h"
#include "sqlite3.h"

#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>

#if defined(OS_WIN)
    #define HAM_FILESYSTEM_SLASH "\\"
#elif defined(OS_GENERIC)
    #define HAM_FILESYSTEM_SLASH "/"
#endif

/* FCC row delimiter */
#define HAM_DELIMITER "|"

#define HAM_NULL_CHAR '\0'

#define HAM_BUFFER_SIZE 4096

#define HAM_FILESYSTEM_SLASH_LENGTH 1
#define HAM_FCC_FILE_NAME_LENGTH 6
#define HAM_SLASH_FCC_NAME_LENGTH 7

#define HAM_FILE_CLOSED 0
#define HAM_FILE_OPEN 1

/* FCC file identifiers */
#define HAM_FCC_FILE_AM 1
#define HAM_FCC_FILE_EN 2
#define HAM_FCC_FILE_HD 3
#define HAM_FCC_FILE_HS 4
#define HAM_FCC_FILE_CO 5
#define HAM_FCC_FILE_LA 6
#define HAM_FCC_FILE_SC 7
#define HAM_FCC_FILE_SF 8

/* FCC file field count */
#define HAM_FCC_AM_FIELDS 18
#define HAM_FCC_EN_FIELDS 27
#define HAM_FCC_HD_FIELDS 50
#define HAM_FCC_HS_FIELDS 6
#define HAM_FCC_CO_FIELDS 8
#define HAM_FCC_LA_FIELDS 8
#define HAM_FCC_SC_FIELDS 9
#define HAM_FCC_SF_FIELDS 11

/* Default SQLite file name */
#define HAM_SQLITE_FILE_NAME "fcchamdatabase.s3db"

const static char *HAM_SQLITE_TABLE_FCC_AM = "CREATE TABLE fcc_am(record_type TEXT NOT NULL,"
                                                "unique_system_identifier INTEGER NOT NULL,"
                                                "uls_file_num TEXT,"
                                                "ebf_number TEXT,"
                                                "callsign TEXT,"
                                                "operator_class TEXT,"
                                                "group_code TEXT,"
                                                "region_code INTEGER,"
                                                "trustee_callsign TEXT,"
                                                "trustee_indicator TEXT,"
                                                "physician_certification TEXT,"
                                                "ve_signature TEXT,"
                                                "systematic_callsign_change TEXT,"
                                                "vanity_callsign_change TEXT,"
                                                "vanity_relationship TEXT,"
                                                "previous_callsign TEXT,"
                                                "previous_operator_class TEXT,"
                                                "trustee_name TEXT);";

const static char *HAM_SQLITE_TABLE_FCC_EN = "CREATE TABLE fcc_en(record_type TEXT NOT NULL,"
                                                "unique_system_identifier INTEGER NOT NULL,"
                                                "uls_file_number TEXT,"
                                                "ebf_number TEXT,"
                                                "call_sign TEXT,"
                                                "entity_type TEXT,"
                                                "licensee_id TEXT,"
                                                "entity_name TEXT,"
                                                "first_name TEXT,"
                                                "mi TEXT,"
                                                "last_name TEXT,"
                                                "suffix TEXT,"
                                                "phone TEXT,"
                                                "fax TEXT,"
                                                "email TEXT,"
                                                "street_address TEXT,"
                                                "city TEXT,"
                                                "state TEXT,"
                                                "zip_code TEXT,"
                                                "po_box TEXT,"
                                                "attention_line TEXT,"
                                                "sgin TEXT,"
                                                "frn TEXT,"
                                                "applicant_type_code TEXT,"
                                                "applicant_type_other TEXT,"
                                                "status_code TEXT,"
                                                "status_date DATETIME);";

const static char *HAM_SQLITE_TABLE_FCC_HD = "CREATE TABLE fcc_hd(record_type TEXT NOT NULL,"
                                                "unique_system_identifier INTEGER NOT NULL,"
                                                "uls_file_number TEXT,"
                                                "ebf_number TEXT,"
                                                "call_sign TEXT,"
                                                "license_status TEXT,"
                                                "radio_service_code TEXT,"
                                                "grant_date TEXT,"
                                                "expired_date TEXT,"
                                                "cancellation_date TEXT,"
                                                "eligibility_rule_num TEXT,"
                                                "applicant_type_code_reserved TEXT,"
                                                "alien TEXT,"
                                                "alien_government TEXT,"
                                                "alien_corporation TEXT,"
                                                "alien_officer TEXT,"
                                                "alien_control TEXT,"
                                                "revoked TEXT,"
                                                "convicted TEXT,"
                                                "adjudged TEXT,"
                                                "involved_reserved TEXT,"
                                                "common_carrier TEXT,"
                                                "non_common_carrier TEXT,"
                                                "private_comm TEXT,"
                                                "fixed TEXT,"
                                                "mobile TEXT,"
                                                "radiolocation TEXT,"
                                                "satellite TEXT,"
                                                "developmental_or_sta TEXT,"
                                                "interconnected_service TEXT,"
                                                "certifier_first_name TEXT,"
                                                "certifier_mi TEXT,"
                                                "certifier_last_name TEXT,"
                                                "certifier_suffix TEXT,"
                                                "certifier_title TEXT,"
                                                "gender TEXT,"
                                                "african_american TEXT,"
                                                "native_american TEXT,"
                                                "hawaiian TEXT,"
                                                "asian TEXT,"
                                                "white TEXT,"
                                                "ethnicity TEXT,"
                                                "effective_date TEXT,"
                                                "last_action_date TEXT,"
                                                "auction_id INTEGER,"
                                                "reg_stat_broad_serv TEXT,"
                                                "band_manager TEXT,"
                                                "type_serv_broad_serv TEXT,"
                                                "alien_ruling TEXT,"
                                                "licensee_name_change TEXT);";

const static char *HAM_SQLITE_TABLE_FCC_HS = "CREATE TABLE fcc_hs(record_type TEXT NOT NULL,"
                                                "unique_system_identifier INTEGER NOT NULL,"
                                                "uls_file_number TEXT,"
                                                "callsign TEXT,"
                                                "log_date TEXT,"
                                                "code TEXT);";


// Optional FCC tables
const static char *HAM_SQLITE_TABLE_FCC_CO = "CREATE TABLE fcc_co(record_type TEXT NOT NULL,"
                                                "unique_system_identifier INTEGER NOT NULL,"
                                                "uls_file_num TEXT,"
                                                "callsign TEXT,"
                                                "comment_date TEXT,"
                                                "description TEXT,"
                                                "status_code TEXT,"
                                                "status_date DATETIME);";

const static char *HAM_SQLITE_TABLE_FCC_LA = "CREATE TABLE fcc_la(record_type TEXT NOT NULL,"
                                                "unique_system_identifier INTEGER NOT NULL,"
                                                "callsign TEXT,"
                                                "attachment_code TEXT,"
                                                "attachment_desc TEXT,"
                                                "attachment_date TEXT,"
                                                "attachment_filename TEXT,"
                                                "action_performed TEXT);";

const static char *HAM_SQLITE_TABLE_FCC_SC = "CREATE TABLE fcc_sc(record_type TEXT,"
                                                "unique_system_identifier INTEGER NOT NULL,"
                                                "uls_file_number TEXT,"
                                                "ebf_number TEXT,"
                                                "callsign TEXT,"
                                                "special_condition_type TEXT,"
                                                "special_condition_code INTEGER,"
                                                "status_code TEXT,"
                                                "status_date DATETIME);";

const static char *HAM_SQLITE_TABLE_FCC_SF = "CREATE TABLE fcc_sf(record_type TEXT,"
                                                "unique_system_identifier INTEGER,"
                                                "uls_file_number TEXT,"
                                                "ebf_number TEXT,"
                                                "callsign TEXT,"
                                                "lic_freeform_cond_type TEXT,"
                                                "unique_lic_freeform_id INTEGER,"
                                                "sequence_number INTEGER,"
                                                "lic_freeform_condition TEXT,"
                                                "status_code TEXT,"
                                                "status_date DATETIME);";

/* SQLite insert statements */
const static char *HAM_SQLITE_INSERT_FCC_AM = "INSERT INTO fcc_am (record_type,"
                                                "unique_system_identifier,"
                                                "uls_file_num,"
                                                "ebf_number,"
                                                "callsign,"
                                                "operator_class,"
                                                "group_code,"
                                                "region_code,"
                                                "trustee_callsign,"
                                                "trustee_indicator,"
                                                "physician_certification,"
                                                "ve_signature,"
                                                "systematic_callsign_change,"
                                                "vanity_callsign_change,"
                                                "vanity_relationship,"
                                                "previous_callsign,"
                                                "previous_operator_class,"
                                                "trustee_name)";

const static char *HAM_SQLITE_INSERT_FCC_EN = "INSERT INTO fcc_en(record_type,"
                                                "unique_system_identifier,"
                                                "uls_file_number,"
                                                "ebf_number,"
                                                "call_sign,"
                                                "entity_type,"
                                                "licensee_id,"
                                                "entity_name,"
                                                "first_name,"
                                                "mi,"
                                                "last_name,"
                                                "suffix,"
                                                "phone,"
                                                "fax,"
                                                "email,"
                                                "street_address,"
                                                "city,"
                                                "state,"
                                                "zip_code,"
                                                "po_box,"
                                                "attention_line,"
                                                "sgin,"
                                                "frn,"
                                                "applicant_type_code,"
                                                "applicant_type_other,"
                                                "status_code,"
                                                "status_date)";

const static char *HAM_SQLITE_INSERT_FCC_HD = "INSERT INTO fcc_hd(record_type,"
                                                "unique_system_identifier,"
                                                "uls_file_number,"
                                                "ebf_number,"
                                                "call_sign,"
                                                "license_status,"
                                                "radio_service_code,"
                                                "grant_date,"
                                                "expired_date,"
                                                "cancellation_date,"
                                                "eligibility_rule_num,"
                                                "applicant_type_code_reserved,"
                                                "alien,"
                                                "alien_government,"
                                                "alien_corporation,"
                                                "alien_officer,"
                                                "alien_control,"
                                                "revoked,"
                                                "convicted,"
                                                "adjudged,"
                                                "involved_reserved,"
                                                "common_carrier,"
                                                "non_common_carrier,"
                                                "private_comm,"
                                                "fixed,"
                                                "mobile,"
                                                "radiolocation,"
                                                "satellite,"
                                                "developmental_or_sta,"
                                                "interconnected_service,"
                                                "certifier_first_name,"
                                                "certifier_mi,"
                                                "certifier_last_name,"
                                                "certifier_suffix,"
                                                "certifier_title,"
                                                "gender,"
                                                "african_american,"
                                                "native_american,"
                                                "hawaiian,"
                                                "asian,"
                                                "white,"
                                                "ethnicity,"
                                                "effective_date,"
                                                "last_action_date,"
                                                "auction_id,"
                                                "reg_stat_broad_serv,"
                                                "band_manager,"
                                                "type_serv_broad_serv,"
                                                "alien_ruling,"
                                                "licensee_name_change)";

const static char *HAM_SQLITE_INSERT_FCC_HS = "INSERT INTO fcc_hs(record_type,"
                                                "unique_system_identifier,"
                                                "uls_file_number,"
                                                "callsign,"
                                                "log_date,"
                                                "code)";


// Optional FCC file sqlite inserts
const static char *HAM_SQLITE_INSERT_FCC_CO = "INSERT INTO fcc_co(record_type,"
                                                "unique_system_identifier,"
                                                "uls_file_num,"
                                                "callsign,"
                                                "comment_date,"
                                                "description,"
                                                "status_code,"
                                                "status_date)";

const static char *HAM_SQLITE_INSERT_FCC_LA = "INSERT INTO fcc_la(record_type,"
                                                "unique_system_identifier,"
                                                "callsign,"
                                                "attachment_code,"
                                                "attachment_desc,"
                                                "attachment_date,"
                                                "attachment_filename,"
                                                "action_performed)";

const static char *HAM_SQLITE_INSERT_FCC_SC = "INSERT INTO fcc_sc(record_type,"
                                                "unique_system_identifier,"
                                                "uls_file_number,"
                                                "ebf_number,"
                                                "callsign,"
                                                "special_condition_type,"
                                                "special_condition_code,"
                                                "status_code,"
                                                "status_date)";

const static char *HAM_SQLITE_INSERT_FCC_SF = "INSERT INTO fcc_sf(record_type,"
                                                "unique_system_identifier,"
                                                "uls_file_number,"
                                                "ebf_number,"
                                                "callsign,"
                                                "lic_freeform_cond_type,"
                                                "unique_lic_freeform_id,"
                                                "sequence_number,"
                                                "lic_freeform_condition,"
                                                "status_code,"
                                                "status_date)";

const static char *HAM_SQLITE_INSERT_VALUES = "VALUES(";
const static char HAM_SQLITE_INSERT_VALUES_CLOSE = ')';

/* Internal function prototypes */
char *ham_cat_dir_name(char *path, const char *directory, const char *name);
int ham_alloc_string_array(char ***array, const int num_fields, const int num_char);
int ham_free_string_array(char ***array, const int num_fields, const int num_char);
int ham_parse_line_with_delimiter(char **fields, const char *line, const int num_fields,
                                const char *delimiter);
INT64 ham_get_lines_in_file(FILE *file);

/* Internal FCC file function prototypes */
int ham_fcc_files_exist(char *directory, const int include_optional);
void ham_fcc_close_all(ham_fcc_database *database);

/* Internal SQLite function prototypes */
int ham_sqlite_create_file(const char *path);
int ham_sqlite_open_database_connection(sqlite3 **db, char *path);
int ham_sqlite_save_ram_database(sqlite3 *ram_db, sqlite3 *backup_db);
int ham_sqlite_create_tables(sqlite3 *db, const ham_fcc_database *fcc_database, char *errmsg);
int ham_sqlite_fcc_convert_file(sqlite3 *db, char *errmsg, FILE *data, const int fcc_file);
int ham_sqlite_insert_fields(sqlite3 *db, char *errmsg, char **fields, const int num_fields,
                            const int fcc_file);
int ham_sqlite_strip_sql_ctr(char *buffer);

/* FCC database structure */
typedef struct ham_fcc_database {
    char *directory;

    /* Necessary files */
    FILE *am;
    FILE *en;
    FILE *hd;
    FILE *hs;

    /* Optional files */
    FILE *co;
    FILE *la;
    FILE *sc;
    FILE *sf;

    int include_optional;

    int am_open;
    int en_open;
    int hd_open;
    int hs_open;
    int co_open;
    int la_open;
    int sc_open;
    int sf_open;

    /* Holds the number of lines in the files */
    INT64 am_length;
    INT64 en_length;
    INT64 hd_length;
    INT64 hs_length;
    INT64 co_length;
    INT64 la_length;
    INT64 sc_length;
    INT64 sf_length;
} ham_fcc_database;

/* Join the fcc database directory and file name */
char *ham_cat_dir_name(char *path, const char *directory, const char *name) {
    if(strlen(directory) == 0) {
        strncpy(path, name, strlen(name));
        return path;
    }
    strncat(path, directory, strlen(directory));
    strncat(path, HAM_FILESYSTEM_SLASH, HAM_FILESYSTEM_SLASH_LENGTH);
    strncat(path, name, HAM_FCC_FILE_NAME_LENGTH);

    return path;
}

/*
 * Handles the memory allocation of the array of char arrays for holding the fields in the FCC
 * files.
 */
int ham_alloc_string_array(char ***array, const int num_fields, const int num_char) {
    /* Allocate the pointer to pointers */
    (*array) = malloc(sizeof(char*) * num_fields);
    if((*array) == NULL)
        return HAM_ERROR_MALLOC_FAIL;
    memset((*array), HAM_NULL_CHAR, sizeof(char*) * num_fields);

    for(int i = 0; i < num_fields; i++) {
        (*array)[i] = malloc(sizeof(char) * num_char);

        if((*array)[i] == NULL) {
            /* Clean up if an allocation fails */
            /* This will not free the array if it's the first */
            for(int j = 0; j < i; j++)
                free((*array)[j]);

            free((*array));

            return HAM_ERROR_MALLOC_FAIL;
        }

        /* If we don't set the memory to null, shit goes down later. */
        memset((*array)[i], HAM_NULL_CHAR, sizeof(char) * HAM_BUFFER_SIZE);
    }

    return HAM_OK;
}

/* Free the array of char arrays we allocated for holding the data fields */
int ham_free_string_array(char ***array, const int num_fields, const num_char) {
    for(int i = 0; i < num_fields; i++)
        free((*array)[i]);

    free((*array));

    return HAM_OK;
}

int ham_parse_line_with_delimiter(char **fields, const char *line, const int num_fields,
                                const char *delimiter) {
    const char *begin, *end;
    char buffer[HAM_BUFFER_SIZE], buffer2[HAM_BUFFER_SIZE];
    int field = 0, ctr = 0;

    /* The fields must be reset or the unwanted data will be appended to many fields */
    for(int i = 0; i < num_fields; i++)
        memset(fields[i], HAM_NULL_CHAR, HAM_BUFFER_SIZE);

    begin = line;

    while (!ctr) {
        memset(buffer, HAM_NULL_CHAR, sizeof(char) * HAM_BUFFER_SIZE);

        end = strpbrk(begin, delimiter);

        if(end == NULL) {
            strncpy(buffer, begin, strlen(begin));

            ctr++;
        } else {
            strncpy(buffer, begin, (end - begin));
            begin = end + 1;
        }

        /*
         * We are adding the "'FIELD'," formatting for sqlite so we don't have to loop through all
         * the data again later. If down the road some other format is added with which we don't
         * want it pre-formatted, we can change.
         */
        if(strlen(buffer) != 0) {
            memset(buffer2, HAM_NULL_CHAR, sizeof(char) * HAM_BUFFER_SIZE);
            ham_sqlite_strip_sql_ctr(buffer);

            memset(buffer2, '\'', 1);
            strncpy(&buffer2[1], buffer, strlen(buffer));
            strncat(buffer2, "\',", strlen("\',"));
            strncpy(fields[field], buffer2, strlen(buffer2));
        }
        else
            strncpy(fields[field], "null,", strlen("null,"));

        /* Again, we pre-formatted for sqlite */
        if (field == (num_fields - 1))
            memset(strrchr(fields[field], ','), HAM_SQLITE_INSERT_VALUES_CLOSE, 1);

/*         printf("Begin: %p\n", begin);
        printf("End: %p\n", end);
        printf("Token: %s\n", fields[field]);
        printf("Length: %zd\n\n", strlen(buffer)); */
        field++;
    }

    return HAM_OK;
}

int ham_fcc_files_exist(char *directory, const int include_optional) {
    /* TODO: Implement */
    return HAM_OK;
}

/* Returns the number of lines in a file. If there's an error, -1 is returned. */
INT64 ham_get_lines_in_file(FILE *file) {
    if(file == NULL)
        return -1;

    INT64 lines = 0;
    char buffer[HAM_BUFFER_SIZE];

    /* Set file position to the beginning, possibly losing the position of the caller */
    rewind(file);

    do {
        fgets(buffer, HAM_BUFFER_SIZE, file);
        lines++;
    } while(!feof(file));

    rewind(file);
    return lines;
}

void ham_fcc_close_all(ham_fcc_database *database) {

    if(database->am_open == HAM_FILE_OPEN) {
        fclose(database->am);
        database->am_open = HAM_FILE_CLOSED;
    }

    if(database->en_open == HAM_FILE_OPEN) {
        fclose(database->en);
        database->en_open = HAM_FILE_CLOSED;
    }

    if(database->hd_open == HAM_FILE_OPEN) {
        fclose(database->hd);
        database->hd_open = HAM_FILE_CLOSED;
    }

    if(database->hs_open == HAM_FILE_OPEN) {
        fclose(database->hs);
        database->hs_open = HAM_FILE_CLOSED;
    }

    if(database->co_open == HAM_FILE_OPEN) {
        fclose(database->co);
        database->co_open = HAM_FILE_CLOSED;
    }

    if(database->la_open == HAM_FILE_OPEN) {
        fclose(database->la);
        database->la_open = HAM_FILE_CLOSED;
    }

    if(database->sc_open == HAM_FILE_OPEN) {
        fclose(database->sc);
        database->sc_open = HAM_FILE_CLOSED;
    }

    if(database->sf_open == HAM_FILE_OPEN) {
        fclose(database->sf);
        database->sf_open = HAM_FILE_CLOSED;
    }

}

LIBHAMDATA_API int ham_fcc_init_directory(ham_fcc_database **database, char *directory,
                            const int include_optional) {
    if (strlen(directory) > (HAM_MAX_DIR_LENGTH - HAM_SLASH_FCC_NAME_LENGTH))
        return HAM_ERROR_DIR_TOO_LONG;

    char path[HAM_MAX_DIR_LENGTH] = "";

    /* For now we'll assume all the files exist */
    /* TODO check files exist */
    *database = malloc(sizeof(ham_fcc_database));
    if(*database == NULL)
        return HAM_ERROR_MALLOC_FAIL;

    /* Default to not include optional files */
    (*database)->include_optional = include_optional;

    /* Initialize the open indicators to closed */
    (*database)->am_open = HAM_FILE_CLOSED;
    (*database)->en_open = HAM_FILE_CLOSED;
    (*database)->hd_open = HAM_FILE_CLOSED;
    (*database)->hs_open = HAM_FILE_CLOSED;
    (*database)->co_open = HAM_FILE_CLOSED;
    (*database)->la_open = HAM_FILE_CLOSED;
    (*database)->sc_open = HAM_FILE_CLOSED;
    (*database)->sf_open = HAM_FILE_CLOSED;

    /* Open all FCC files */
    (*database)->am = fopen(ham_cat_dir_name(path, directory, "am.dat"), "r");
    assert((*database)->am != NULL);
    if((*database)->am == NULL) {
        ham_fcc_terminate(database);
        return HAM_ERROR_OPEN_FILE;
    }
    (*database)->am_open = HAM_FILE_OPEN;
    (*database)->am_length = ham_get_lines_in_file((*database)->am);

    (*database)->en = fopen(ham_cat_dir_name(path, directory, "en.dat"), "r");
    assert((*database)->en != NULL);
    if((*database)->en == NULL) {
        ham_fcc_terminate(database);
        return HAM_ERROR_OPEN_FILE;
    }
    (*database)->en_open = HAM_FILE_OPEN;
    (*database)->en_length = ham_get_lines_in_file((*database)->en);

    (*database)->hd = fopen(ham_cat_dir_name(path, directory, "hd.dat"), "r");
    assert((*database)->hd != NULL);
    if((*database)->hd == NULL) {
        ham_fcc_terminate(database);
        return HAM_ERROR_OPEN_FILE;
    }
    (*database)->hd_open = HAM_FILE_OPEN;
    (*database)->hd_length = ham_get_lines_in_file((*database)->hd);

    (*database)->hs = fopen(ham_cat_dir_name(path, directory, "hs.dat"), "r");
    assert((*database)->hs != NULL);
    if((*database)->hs == NULL) {
        ham_fcc_terminate(database);
        return HAM_ERROR_OPEN_FILE;
    }
    (*database)->hs_open = HAM_FILE_OPEN;
    (*database)->hs_length = ham_get_lines_in_file((*database)->hs);

    /* Optional files */
    if(include_optional) {

        (*database)->co = fopen(ham_cat_dir_name(path, directory, "co.dat"), "r");
        if((*database)->co == NULL) {
            ham_fcc_terminate(database);
            return HAM_ERROR_OPEN_FILE;
        }
        (*database)->co_open = HAM_FILE_OPEN;
        (*database)->co_length = ham_get_lines_in_file((*database)->co);

        (*database)->la = fopen(ham_cat_dir_name(path, directory, "la.dat"), "r");
        if((*database)->la == NULL) {
            ham_fcc_terminate(database);
            return HAM_ERROR_OPEN_FILE;
        }
        (*database)->la_open = HAM_FILE_OPEN;
        (*database)->la_length = ham_get_lines_in_file((*database)->la);

        (*database)->sc = fopen(ham_cat_dir_name(path, directory, "sc.dat"), "r");
        if((*database)->sc == NULL) {
            ham_fcc_terminate(database);
            return HAM_ERROR_OPEN_FILE;
        }
        (*database)->sc_open = HAM_FILE_OPEN;
        (*database)->sc_length = ham_get_lines_in_file((*database)->sc);

        (*database)->sf = fopen(ham_cat_dir_name(path, directory, "sf.dat"), "r");
        if((*database)->sf == NULL) {
            ham_fcc_terminate(database);
            return HAM_ERROR_OPEN_FILE;
        }
        (*database)->sf_open = HAM_FILE_OPEN;
        (*database)->sf_length = ham_get_lines_in_file((*database)->sf);
    }


    /* printf("%I64d\n%I64d\n%I64d\n%I64d\n", (*database)->am_length,(*database)->en_length,
                (*database)->hd_length,(*database)->hs_length); */
    return HAM_OK;
}

LIBHAMDATA_API int ham_fcc_terminate(ham_fcc_database **database) {
    /* Can be safely called if already freed. */
    if(*database == NULL)
        return HAM_OK;

    ham_fcc_close_all(*database);

    free(*database);
    *database = NULL;
    return HAM_OK;
}

/*
 * Convert the FCC's text database to SQLite.
 *
 * If HAM_SQLITE_FILE_NAME already exists, it will be overwritten.
 */
LIBHAMDATA_API int ham_fcc_to_sqlite(const ham_fcc_database *fcc_database, const int save_in_ram) {

    /*
     * These variable will be used throughout the conversion * process, being passed to multiple
     * sub-functions. If save_in_ram is false, just ram_db is used.
     */
    sqlite3 *ram_db, *db;
    /* char *errmsg = NULL; */

    char *path;
    if(save_in_ram)
        path = ":memory:";
    else
        path = HAM_SQLITE_FILE_NAME;

    /* Conversion preparations */
    if(ham_sqlite_create_file(HAM_SQLITE_FILE_NAME))
        return HAM_ERROR_SQLITE_CREATE_FILE;

    if(ham_sqlite_open_database_connection(&ram_db, path))
        return HAM_ERROR_SQLITE_OPEN_DATABASE_CONNECTION;

    if(ham_sqlite_create_tables(ram_db, fcc_database, NULL))
        return HAM_ERROR_SQLITE_CREATE_TABLES;

    /* Perform the conversion */
    ham_sqlite_fcc_convert_file(ram_db, NULL, fcc_database->am, HAM_FCC_FILE_AM);
    ham_sqlite_fcc_convert_file(ram_db, NULL, fcc_database->en, HAM_FCC_FILE_EN);
    ham_sqlite_fcc_convert_file(ram_db, NULL, fcc_database->hd, HAM_FCC_FILE_HD);
    ham_sqlite_fcc_convert_file(ram_db, NULL, fcc_database->hs, HAM_FCC_FILE_HS);

    if(fcc_database->include_optional == HAM_BOOL_YES) {
    ham_sqlite_fcc_convert_file(ram_db, NULL, fcc_database->co, HAM_FCC_FILE_CO);
    ham_sqlite_fcc_convert_file(ram_db, NULL, fcc_database->la, HAM_FCC_FILE_LA);
    ham_sqlite_fcc_convert_file(ram_db, NULL, fcc_database->sc, HAM_FCC_FILE_SC);
    ham_sqlite_fcc_convert_file(ram_db, NULL, fcc_database->sf, HAM_FCC_FILE_SF);
    }

    if(save_in_ram) {
        if(ham_sqlite_open_database_connection(&db, HAM_SQLITE_FILE_NAME))
            return HAM_ERROR_SQLITE_OPEN_DATABASE_CONNECTION;

        if(ham_sqlite_save_ram_database(ram_db, db))
            return HAM_ERROR_SQLITE_BACKUP_FAIL;
    }

    return HAM_OK;
}


/* Creates the HAM_SQLITE_FILE_NAME. If it already exists, reset it. */
int ham_sqlite_create_file(const char *path) {
    FILE *f = fopen(path, "w");

    if(f == NULL)
        return HAM_ERROR_SQLITE_CREATE_FILE;

    fclose(f);

    return HAM_OK;
}

int ham_sqlite_open_database_connection(sqlite3 **db, char *path) {
    if(sqlite3_open(path, db)) {
        fprintf(stderr, "Unable to open database: %s\n", sqlite3_errmsg(*db));
        sqlite3_close(*db);
        return HAM_ERROR_SQLITE_OPEN_DATABASE_CONNECTION;
    }

    return HAM_OK;
}

int ham_sqlite_save_ram_database(sqlite3 *ram_db, sqlite3 *backup_db) {
    sqlite3_backup *backup;
    backup = sqlite3_backup_init(backup_db, "main", ram_db, "main");
    if(backup) {
        if(sqlite3_backup_step(backup, -1) != SQLITE_DONE) {
            fprintf(stderr, "Error: failed to backup database\n");
            return HAM_ERROR_SQLITE_BACKUP_FAIL;
        }

        sqlite3_backup_finish(backup);
    }

    return HAM_OK;
}

int ham_sqlite_create_tables(sqlite3 *db, const ham_fcc_database *fcc_database, char *errmsg) {
    if(sqlite3_exec(db, HAM_SQLITE_TABLE_FCC_AM, NULL, NULL, &errmsg))
        return HAM_ERROR_SQLITE_CREATE_TABLES;

    if(sqlite3_exec(db, HAM_SQLITE_TABLE_FCC_EN, NULL, NULL, &errmsg))
        return HAM_ERROR_SQLITE_CREATE_TABLES;

    if(sqlite3_exec(db, HAM_SQLITE_TABLE_FCC_HD, NULL, NULL, &errmsg))
        return HAM_ERROR_SQLITE_CREATE_TABLES;

    if(sqlite3_exec(db, HAM_SQLITE_TABLE_FCC_HS, NULL, NULL, &errmsg))
        return HAM_ERROR_SQLITE_CREATE_TABLES;

    /* Include optional files */
    if(fcc_database->include_optional) {
        if(sqlite3_exec(db, HAM_SQLITE_TABLE_FCC_CO, NULL, NULL, &errmsg))
        return HAM_ERROR_SQLITE_CREATE_TABLES;

        if(sqlite3_exec(db, HAM_SQLITE_TABLE_FCC_LA, NULL, NULL, &errmsg))
            return HAM_ERROR_SQLITE_CREATE_TABLES;

        if(sqlite3_exec(db, HAM_SQLITE_TABLE_FCC_SC, NULL, NULL, &errmsg))
            return HAM_ERROR_SQLITE_CREATE_TABLES;

        if(sqlite3_exec(db, HAM_SQLITE_TABLE_FCC_SF, NULL, NULL, &errmsg))
            return HAM_ERROR_SQLITE_CREATE_TABLES;
    }

    return HAM_OK;
}

int ham_sqlite_fcc_convert_file(sqlite3 *db, char *errmsg, FILE *data, const int fcc_file) {
    char buffer[HAM_BUFFER_SIZE] = "";
    int error = HAM_OK;
    int num_fields = 0;
    char **fields;

    switch (fcc_file) {
        case HAM_FCC_FILE_AM:
            error = ham_alloc_string_array(&fields, HAM_FCC_AM_FIELDS, HAM_BUFFER_SIZE);
            if(error != HAM_OK)
                return HAM_ERROR_MALLOC_FAIL;

            num_fields = HAM_FCC_AM_FIELDS;
            break;

        case HAM_FCC_FILE_EN:
            error = ham_alloc_string_array(&fields, HAM_FCC_EN_FIELDS, HAM_BUFFER_SIZE);
            if(error != HAM_OK)
                return HAM_ERROR_MALLOC_FAIL;

            num_fields = HAM_FCC_EN_FIELDS;
            break;

        case HAM_FCC_FILE_HD:
            error = ham_alloc_string_array(&fields, HAM_FCC_HD_FIELDS, HAM_BUFFER_SIZE);
            if(error != HAM_OK)
                return HAM_ERROR_MALLOC_FAIL;

            num_fields = HAM_FCC_HD_FIELDS;
            break;

        case HAM_FCC_FILE_HS:
            error = ham_alloc_string_array(&fields, HAM_FCC_HS_FIELDS, HAM_BUFFER_SIZE);
            if(error != HAM_OK)
                return HAM_ERROR_MALLOC_FAIL;

            num_fields = HAM_FCC_HS_FIELDS;
            break;

        case HAM_FCC_FILE_CO:
            error = ham_alloc_string_array(&fields, HAM_FCC_CO_FIELDS, HAM_BUFFER_SIZE);
            if(error != HAM_OK)
                return HAM_ERROR_MALLOC_FAIL;

            num_fields = HAM_FCC_CO_FIELDS;
            break;

        case HAM_FCC_FILE_LA:
            error = ham_alloc_string_array(&fields, HAM_FCC_LA_FIELDS, HAM_BUFFER_SIZE);
            if(error != HAM_OK)
                return HAM_ERROR_MALLOC_FAIL;

            num_fields = HAM_FCC_LA_FIELDS;
            break;

        case HAM_FCC_FILE_SC:
            error = ham_alloc_string_array(&fields, HAM_FCC_SC_FIELDS, HAM_BUFFER_SIZE);

            if(error != HAM_OK)
                return HAM_ERROR_MALLOC_FAIL;
            num_fields = HAM_FCC_SC_FIELDS;
            break;

        case HAM_FCC_FILE_SF:
            error = ham_alloc_string_array(&fields, HAM_FCC_SF_FIELDS, HAM_BUFFER_SIZE);
            if(error != HAM_OK)
                return HAM_ERROR_MALLOC_FAIL;

            num_fields = HAM_FCC_SF_FIELDS;
            break;

        default:
            return HAM_ERROR_GENERIC;
    }

    memset(buffer, HAM_NULL_CHAR, HAM_BUFFER_SIZE);

    while(fgets(buffer, HAM_BUFFER_SIZE, data) != NULL) {

        /*
         * fgets includes the new line at the end of the buffer; we need to replace it with a null
         * char.
         */
        memset(strpbrk(buffer, "\n"), HAM_NULL_CHAR, 1);

        error = ham_parse_line_with_delimiter((char **)fields, buffer, num_fields, HAM_DELIMITER);
        if(error != HAM_OK) {
            ham_free_string_array(&fields, num_fields, HAM_BUFFER_SIZE);
            return HAM_ERROR_GENERIC;
        }

        ham_sqlite_insert_fields(db, errmsg, fields, num_fields, fcc_file);

        memset(buffer, HAM_NULL_CHAR, HAM_BUFFER_SIZE);
    }

    ham_free_string_array(&fields, num_fields, HAM_BUFFER_SIZE);

    return error;
}
int ham_sqlite_insert_fields(sqlite3 *db, char *errmsg, char **fields, const int num_fields,
                            const int fcc_file) {
    char sql[HAM_BUFFER_SIZE];

    switch (fcc_file) {
        case HAM_FCC_FILE_AM:
            strncpy(sql, HAM_SQLITE_INSERT_FCC_AM, strlen(HAM_SQLITE_INSERT_FCC_AM));
            break;

        case HAM_FCC_FILE_EN:
            strncpy(sql, HAM_SQLITE_INSERT_FCC_EN, strlen(HAM_SQLITE_INSERT_FCC_EN));
            break;

        case HAM_FCC_FILE_HD:
            strncpy(sql, HAM_SQLITE_INSERT_FCC_HD, strlen(HAM_SQLITE_INSERT_FCC_HD));
            break;

        case HAM_FCC_FILE_HS:
            strncpy(sql, HAM_SQLITE_INSERT_FCC_HS, strlen(HAM_SQLITE_INSERT_FCC_HS));
            break;

        case HAM_FCC_FILE_CO:
            strncpy(sql, HAM_SQLITE_INSERT_FCC_CO, strlen(HAM_SQLITE_INSERT_FCC_CO));
            break;

        case HAM_FCC_FILE_LA:
            strncpy(sql, HAM_SQLITE_INSERT_FCC_LA, strlen(HAM_SQLITE_INSERT_FCC_LA));
            break;

        case HAM_FCC_FILE_SC:
            strncpy(sql, HAM_SQLITE_INSERT_FCC_SC, strlen(HAM_SQLITE_INSERT_FCC_SC));
            break;

        case HAM_FCC_FILE_SF:
            strncpy(sql, HAM_SQLITE_INSERT_FCC_SF, strlen(HAM_SQLITE_INSERT_FCC_SF));
            break;

        default:
            return HAM_ERROR_GENERIC;
    }

    strncat(sql, HAM_SQLITE_INSERT_VALUES, strlen(HAM_SQLITE_INSERT_VALUES));

    for(int i = 0; i < num_fields; i++)
        strncat(sql, fields[i], strlen(fields[i]));

    if(sqlite3_exec(db, sql, NULL, NULL, &errmsg)) {
        fprintf(stderr, "Unable to insert data: %s\n", sqlite3_errmsg(db));
        fprintf(stderr, "%s\n\n", sql);
        return HAM_ERROR_SQLITE_INSERT_FAIL;
    }

    return HAM_OK;
}

int ham_sqlite_strip_sql_ctr(char *data) {
    if(!strlen(data) || strchr(data, '\'') == NULL)
        return HAM_OK;

    char buffer[HAM_BUFFER_SIZE];
    char *begin, *end;

    memset(buffer, HAM_NULL_CHAR, HAM_BUFFER_SIZE);

    begin = data;
    end = strchr(begin, '\'');
    while(end != NULL) {
        strncat(buffer, begin, ((end -  begin) + 1));
        strncat(buffer, "\'", 1);
        begin = end + 1;
        end = strchr(begin, '\'');
    }
    strncat(buffer, begin, HAM_BUFFER_SIZE - ((end - begin) + 1 ));
    /* printf("%s\n", buffer); */

    strncpy(data, buffer, strlen(buffer));
    return HAM_OK;
}
