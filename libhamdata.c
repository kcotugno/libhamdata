/*
 * Copyright (C) 2016 Kevin Cotugno
 * All rights reserved
 *
 * Distributed under the terms of the MIT software license. See the
 * accompanying LICENSE file or http://www.opensource.org/licenses/MIT.
 *
 * libhamdata: libhamdata.c
 *
 * Author: kcotugno
 * Date: 6/12/2016
 */

#include "libhamdata.h"
#include "sqlite3.h"

#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

/* FCC row delimiter */
#define HAM_DELIMITER "|"

#define HAM_NULL_CHAR '\0'

#define HAM_BUFFER_SIZE 4096

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
#define HAM_SQLITE_FILE_NAME "fcchamdatabase.sqlite3"

/* If you have the FCC file index, you can retrive the name from this array. */
const static char *FCC_FILE_NAMES[9] = {"Unused", "AM.dat", "EN.dat", "HD.dat", "HS.dat", "CO.dat",
                                        "LA.dat", "SC.dat", "SF.dat"};

const static char *HAM_SQLITE_TABLE_FCC_AM = "CREATE TABLE IF NOT EXISTS amateurs ("
                                                "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                                                "record_type TEXT NOT NULL,"
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
                                                "trustee_name TEXT,"
                                                "created_at DATETIME,"
                                                "updated_at DATETIME);";

const static char *HAM_SQLITE_TABLE_FCC_EN = "CREATE TABLE IF NOT EXISTS entities ("
                                                "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                                                "record_type TEXT NOT NULL,"
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
                                                "status_date DATETIME,"
                                                "created_at DATETIME,"
                                                "updated_at DATETIME);";

const static char *HAM_SQLITE_TABLE_FCC_HD = "CREATE TABLE IF NOT EXISTS headers ("
                                                "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                                                "record_type TEXT NOT NULL,"
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
                                                "licensee_name_change TEXT,"
                                                "created_at DATETIME,"
                                                "updated_at DATETIME);";

const static char *HAM_SQLITE_TABLE_FCC_HS = "CREATE TABLE IF NOT EXISTS histories ("
                                                "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                                                "record_type TEXT NOT NULL,"
                                                "unique_system_identifier INTEGER NOT NULL,"
                                                "uls_file_number TEXT,"
                                                "callsign TEXT,"
                                                "log_date TEXT,"
                                                "code TEXT,"
                                                "created_at DATETIME,"
                                                "updated_at DATETIME);";


const static char *HAM_SQLITE_TABLE_FCC_CO = "CREATE TABLE IF NOT EXISTS comments ("
                                                "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                                                "record_type TEXT NOT NULL,"
                                                "unique_system_identifier INTEGER NOT NULL,"
                                                "uls_file_num TEXT,"
                                                "callsign TEXT,"
                                                "comment_date TEXT,"
                                                "description TEXT,"
                                                "status_code TEXT,"
                                                "status_date DATETIME,"
                                                "created_at DATETIME,"
                                                "updated_at DATETIME);";

const static char *HAM_SQLITE_TABLE_FCC_LA = "CREATE TABLE IF NOT EXISTS license_attachments ("
                                                "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                                                "record_type TEXT NOT NULL,"
                                                "unique_system_identifier INTEGER NOT NULL,"
                                                "callsign TEXT,"
                                                "attachment_code TEXT,"
                                                "attachment_desc TEXT,"
                                                "attachment_date TEXT,"
                                                "attachment_filename TEXT,"
                                                "action_performed TEXT,"
                                                "created_at DATETIME,"
                                                "updated_at DATETIME);";

const static char *HAM_SQLITE_TABLE_FCC_SC = "CREATE TABLE IF NOT EXISTS special_conditions ("
                                                "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                                                "record_type TEXT,"
                                                "unique_system_identifier INTEGER NOT NULL,"
                                                "uls_file_number TEXT,"
                                                "ebf_number TEXT,"
                                                "callsign TEXT,"
                                                "special_condition_type TEXT,"
                                                "special_condition_code INTEGER,"
                                                "status_code TEXT,"
                                                "status_date DATETIME,"
                                                "created_at DATETIME,"
                                                "updated_at DATETIME);";

const static char *HAM_SQLITE_TABLE_FCC_SF = "CREATE TABLE IF NOT EXISTS license_free_form_special_conditions ("
                                                "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                                                "record_type TEXT,"
                                                "unique_system_identifier INTEGER,"
                                                "uls_file_number TEXT,"
                                                "ebf_number TEXT,"
                                                "callsign TEXT,"
                                                "lic_freeform_cond_type TEXT,"
                                                "unique_lic_freeform_id INTEGER,"
                                                "sequence_number INTEGER,"
                                                "lic_freeform_condition TEXT,"
                                                "status_code TEXT,"
                                                "status_date DATETIME,"
                                                "created_at DATETIME,"
                                                "updated_at DATETIME);";

/* SQLite insert statements */
const static char *HAM_SQLITE_INSERT_FCC_AM = "INSERT INTO amateurs"
                                                "(record_type,"
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
                                                "trustee_name,"
                                                "created_at,"
                                                "updated_at)"
                                                "VALUES (@rec_type,"
                                                "@unique_system_identifier,"
                                                "@uls_file_num,"
                                                "@ebf_number,"
                                                "@callsign,"
                                                "@operator_class,"
                                                "@group_code,"
                                                "@region_code,"
                                                "@trustee_callsign,"
                                                "@trustee_indicator,"
                                                "@physician_certification,"
                                                "@ve_signature,"
                                                "@systematic_callsign_change,"
                                                "@vanity_callsign_change,"
                                                "@vanity_relationship,"
                                                "@previous_callsign,"
                                                "@previous_operator_class,"
                                                "@trustee_name,"
                                                "@created_at,"
                                                "@updated_at)";

const static char *HAM_SQLITE_INSERT_FCC_EN = "INSERT INTO entities"
                                                "(record_type,"
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
                                                "status_date,"
                                                "created_at,"
                                                "updated_at)"
                                                "VALUES (@record_type,"
                                                "@unique_system_identifier,"
                                                "@uls_file_number,"
                                                "@ebf_number,"
                                                "@call_sign,"
                                                "@entity_type,"
                                                "@licensee_id,"
                                                "@entity_name,"
                                                "@first_name,"
                                                "@mi,"
                                                "@last_name,"
                                                "@suffix,"
                                                "@phone,"
                                                "@fax,"
                                                "@email,"
                                                "@street_address,"
                                                "@city,"
                                                "@state,"
                                                "@zip_code,"
                                                "@po_box,"
                                                "@attention_line,"
                                                "@sgin,"
                                                "@frn,"
                                                "@applicant_type_code,"
                                                "@applicant_type_other,"
                                                "@status_code,"
                                                "@status_date,"
                                                "@created_at,"
                                                "@updated_at)";

const static char *HAM_SQLITE_INSERT_FCC_HD = "INSERT INTO headers"
                                                "(record_type,"
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
                                                "licensee_name_change,"
                                                "created_at,"
                                                "updated_at)"
                                                "VALUES (@record_type,"
                                                "@unique_system_identifier,"
                                                "@uls_file_number,"
                                                "@ebf_number,"
                                                "@call_sign,"
                                                "@license_status,"
                                                "@grant_date,"
                                                "@radio_service_code,"
                                                "@expired_date,"
                                                "@cancellation_date,"
                                                "@eligibility_rule_num,"
                                                "@applicant_type_code_reserved,"
                                                "@alien,"
                                                "@alien_government,"
                                                "@alien_corporation,"
                                                "@alien_officer,"
                                                "@revoked,"
                                                "@alien_control,"
                                                "@convicted,"
                                                "@adjudged,"
                                                "@involved_reserved,"
                                                "@common_carrier,"
                                                "@non_common_carrier,"
                                                "@private_comm,"
                                                "@fixed,"
                                                "@mobile,"
                                                "@radiolocation,"
                                                "@satellite,"
                                                "@developmental_or_sta,"
                                                "@interconnected_service,"
                                                "@certifier_first_name,"
                                                "@certifier_mi,"
                                                "@certifier_last_name,"
                                                "@certifier_suffix,"
                                                "@certifier_title,"
                                                "@gender,"
                                                "@african_american,"
                                                "@native_american,"
                                                "@hawaiian,"
                                                "@asian,"
                                                "@white,"
                                                "@ethnicity,"
                                                "@effective_date,"
                                                "@last_action_date,"
                                                "@auction_id,"
                                                "@reg_stat_broad_serv,"
                                                "@band_manager,"
                                                "@type_serv_broad_serv,"
                                                "@alien_ruling,"
                                                "@licensee_name_change,"
                                                "@created_at,"
                                                "@updated_at)";

const static char *HAM_SQLITE_INSERT_FCC_HS = "INSERT INTO histories"
                                                "(record_type,"
                                                "unique_system_identifier,"
                                                "uls_file_number,"
                                                "callsign,"
                                                "log_date,"
                                                "code,"
                                                "created_at,"
                                                "updated_at)"
                                                "VALUES (@record_type,"
                                                "@unique_system_identifier,"
                                                "@uls_file_number,"
                                                "@callsign,"
                                                "@log_date,"
                                                "@code,"
                                                "@created_at,"
                                                "@updated_at)";

const static char *HAM_SQLITE_INSERT_FCC_CO = "INSERT INTO comments"
                                                "(record_type,"
                                                "unique_system_identifier,"
                                                "uls_file_num,"
                                                "callsign,"
                                                "comment_date,"
                                                "description,"
                                                "status_code,"
                                                "status_date,"
                                                "created_at,"
                                                "updated_at)"
                                                "VALUES (@record_type,"
                                                "@unique_system_identifier,"
                                                "@uls_file_num,"
                                                "@callsign,"
                                                "@description,"
                                                "@comment_date,"
                                                "@status_code,"
                                                "@status_date,"
                                                "@created_at,"
                                                "@updated_at)";

const static char *HAM_SQLITE_INSERT_FCC_LA = "INSERT INTO license_attachments"
                                                "(record_type,"
                                                "unique_system_identifier,"
                                                "callsign,"
                                                "attachment_code,"
                                                "attachment_desc,"
                                                "attachment_date,"
                                                "attachment_filename,"
                                                "action_performed,"
                                                "created_at,"
                                                "updated_at)"
                                                "VALUES (@record_type,"
                                                "@unique_system_identifier,"
                                                "@callsign,"
                                                "@attachment_code,"
                                                "@attachment_desc,"
                                                "@attachment_date,"
                                                "@attachment_filename,"
                                                "@action_performed,"
                                                "@created_at,"
                                                "@updated_at)";

const static char *HAM_SQLITE_INSERT_FCC_SC = "INSERT INTO special_conditions"
                                                "(record_type,"
                                                "unique_system_identifier,"
                                                "uls_file_number,"
                                                "ebf_number,"
                                                "callsign,"
                                                "special_condition_type,"
                                                "special_condition_code,"
                                                "status_code,"
                                                "status_date,"
                                                "created_at,"
                                                "updated_at)"
                                                "VALUES (@record_type,"
                                                "@unique_system_identifier,"
                                                "@uls_file_number,"
                                                "@ebf_number,"
                                                "@callsign,"
                                                "@special_condition_type,"
                                                "@special_condition_code,"
                                                "@status_code,"
                                                "@status_date,"
                                                "@created_at,"
                                                "@updated_at)";

const static char *HAM_SQLITE_INSERT_FCC_SF = "INSERT INTO license_free_form_special_conditions"
                                                "(record_type,"
                                                "unique_system_identifier,"
                                                "uls_file_number,"
                                                "ebf_number,"
                                                "callsign,"
                                                "lic_freeform_cond_type,"
                                                "unique_lic_freeform_id,"
                                                "sequence_number,"
                                                "lic_freeform_condition,"
                                                "status_code,"
                                                "status_date,"
                                                "created_at,"
                                                "updated_at)"
                                                "VALUES (@record_type,"
                                                "@unique_system_identifier,"
                                                "@uls_file_number,"
                                                "@ebf_number,"
                                                "@callsign,"
                                                "@lic_freeform_cond_type,"
                                                "@unique_lic_freeform_id,"
                                                "@sequence_number,"
                                                "@lic_freeform_condition,"
                                                "@status_code,"
                                                "@status_date,"
                                                "@created_at,"
                                                "@updated_at)";

/* FCC database structure */
struct ham_fcc_database {
    char *directory;

    /* FCC database files */
    FILE *am;
    FILE *en;
    FILE *hd;
    FILE *hs;
    FILE *co;
    FILE *la;
    FILE *sc;
    FILE *sf;

    int am_open;
    int en_open;
    int hd_open;
    int hs_open;
    int co_open;
    int la_open;
    int sc_open;
    int sf_open;

    /* Holds the number of lines in the files */
    ham_fcc_lengths *fcc_lengths;
};

/* FCC database file lengths */
struct ham_fcc_lengths {
    INT64 am_length;
    INT64 en_length;
    INT64 hd_length;
    INT64 hs_length;
    INT64 co_length;
    INT64 la_length;
    INT64 sc_length;
    INT64 sf_length;
};

typedef struct ham_fcc_sqlite {
    sqlite3 *database;
    char *sql_errmsg;

    sqlite3_stmt *am_stmt;
    sqlite3_stmt *en_stmt;
    sqlite3_stmt *hd_stmt;
    sqlite3_stmt *hs_stmt;
    sqlite3_stmt *co_stmt;
    sqlite3_stmt *la_stmt;
    sqlite3_stmt *sc_stmt;
    sqlite3_stmt *sf_stmt;

    unsigned int am_line;
    unsigned int en_line;
    unsigned int hd_line;
    unsigned int hs_line;
    unsigned int co_line;
    unsigned int la_line;
    unsigned int sc_line;
    unsigned int sf_line;

    char time[80];

    unsigned int sql_insert_calls;
} ham_fcc_sqlite;

/* Internal function prototypes */
int ham_alloc_string_array(char ***array, const int num_fields, const int num_char);
int ham_free_string_array(char ***array, const int num_fields, const int num_char);
int ham_parse_line_with_delimiter(char **fields, const char *line, const int num_fields,
                                    const char *delimiter);
INT64 ham_get_lines_in_file(FILE *file);

/* Internal FCC file function prototypes */
int ham_fcc_files_exist(char *directory);
void ham_fcc_close_all(ham_fcc_database *database);

/* Internal SQLite function prototypes */
int ham_sqlite_init(ham_fcc_sqlite **fcc_sqlite);
int ham_sqlite_terminate(ham_fcc_sqlite *fcc_sqlite);
int ham_sqlite_sql_prepare_stmt(ham_fcc_sqlite *fcc_sqlite);
int ham_sqlite_sql_finalize_stmt(ham_fcc_sqlite *fcc_sqlite);
int ham_sqlite_reset_file(const char *filename);
int ham_sqlite_open_database_connection(sqlite3 **db, const char *filename);
int ham_sqlite_create_tables(ham_fcc_sqlite *fcc_sqlite);
int ham_sqlite_fcc_convert_file(ham_fcc_sqlite *fcc_sqlite, FILE *data, const int fcc_file);
int ham_sqlite_insert_fields(ham_fcc_sqlite *fcc_sqlite, char **fields, const int num_fields,
                                sqlite3_stmt *sql_stmt, const int fcc_file, const int currentline);

int ham_sqlite_init_time(ham_fcc_sqlite *fcc_sqlite) {
    time_t rawtime;
    struct tm *timeinfo;

    time(&rawtime);
    timeinfo = gmtime(&rawtime);

    strftime(fcc_sqlite->time, 80, "%Y-%m-%d %H-%M-%S", timeinfo);

    return HAM_OK;
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
int ham_free_string_array(char ***array, const int num_fields, const int num_char) {
    for(int i = 0; i < num_fields; i++)
        free((*array)[i]);

    free((*array));

    return HAM_OK;
}

int ham_parse_line_with_delimiter(char **fields, const char *line, const int num_fields,
                                const char *delimiter) {
    const char *begin, *end;
    char buffer[HAM_BUFFER_SIZE];
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

        if(strlen(buffer) == 0)
            fields[field][0] = '\0';
        else
            strncpy(fields[field], buffer, strlen(buffer));

        /*printf("Begin: %p\n", begin);
        printf("End: %p\n", end);
        printf("Token: %s\n", fields[field]);
        printf("Length: %zd\n\n", strlen(buffer));*/
        field++;
    }

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
    if(database->am_open == HAM_BOOL_YES) {
        fclose(database->am);
        database->am_open = HAM_BOOL_NO;
    }

    if(database->en_open == HAM_BOOL_YES) {
        fclose(database->en);
        database->en_open = HAM_BOOL_NO;
    }

    if(database->hd_open == HAM_BOOL_YES) {
        fclose(database->hd);
        database->hd_open = HAM_BOOL_NO;
    }

    if(database->hs_open == HAM_BOOL_YES) {
        fclose(database->hs);
        database->hs_open = HAM_BOOL_NO;
    }

    if(database->co_open == HAM_BOOL_YES) {
        fclose(database->co);
        database->co_open = HAM_BOOL_NO;
    }

    if(database->la_open == HAM_BOOL_YES) {
        fclose(database->la);
        database->la_open = HAM_BOOL_NO;
    }

    if(database->sc_open == HAM_BOOL_YES) {
        fclose(database->sc);
        database->sc_open = HAM_BOOL_NO;
    }

    if(database->sf_open == HAM_BOOL_YES) {
        fclose(database->sf);
        database->sf_open = HAM_BOOL_NO;
    }
}

LIBHAMDATA_API int ham_fcc_database_init(ham_fcc_database **database) {
    int filesopen = 0;

    (*database) = malloc(sizeof(ham_fcc_database));
    if((*database) == NULL)
        return HAM_ERROR_MALLOC_FAIL;

    (*database)->fcc_lengths = malloc(sizeof(ham_fcc_lengths));
    if((*database)->fcc_lengths == NULL) {
        free(*database);
        return HAM_ERROR_MALLOC_FAIL;
    }

    /* Initialize the open indicators to closed */
    (*database)->am_open = HAM_BOOL_NO;
    (*database)->en_open = HAM_BOOL_NO;
    (*database)->hd_open = HAM_BOOL_NO;
    (*database)->hs_open = HAM_BOOL_NO;
    (*database)->co_open = HAM_BOOL_NO;
    (*database)->la_open = HAM_BOOL_NO;
    (*database)->sc_open = HAM_BOOL_NO;
    (*database)->sf_open = HAM_BOOL_NO;

    /* Open all FCC files */
    (*database)->am = fopen(FCC_FILE_NAMES[HAM_FCC_FILE_AM], "r");
    if((*database)->am != NULL) {
        (*database)->am_open = HAM_BOOL_YES;
        (*database)->fcc_lengths->am_length = ham_get_lines_in_file((*database)->am);
        filesopen++;
    }

    (*database)->en = fopen(FCC_FILE_NAMES[HAM_FCC_FILE_EN], "r");
    if((*database)->en != NULL) {
        (*database)->en_open = HAM_BOOL_YES;
        (*database)->fcc_lengths->en_length = ham_get_lines_in_file((*database)->en);
        filesopen++;
    }


    (*database)->hd = fopen(FCC_FILE_NAMES[HAM_FCC_FILE_HD], "r");
    if((*database)->hd != NULL) {
        (*database)->hd_open = HAM_BOOL_YES;
        (*database)->fcc_lengths->hd_length = ham_get_lines_in_file((*database)->hd);
        filesopen++;
    }

    (*database)->hs = fopen(FCC_FILE_NAMES[HAM_FCC_FILE_HS], "r");
    if((*database)->hs != NULL) {
        (*database)->hs_open = HAM_BOOL_YES;
        (*database)->fcc_lengths->hs_length = ham_get_lines_in_file((*database)->hs);
        filesopen++;
    }

    (*database)->co = fopen(FCC_FILE_NAMES[HAM_FCC_FILE_CO], "r");
    if((*database)->co != NULL) {
        (*database)->co_open = HAM_BOOL_YES;
        (*database)->fcc_lengths->co_length = ham_get_lines_in_file((*database)->co);
        filesopen++;
    }

    (*database)->la = fopen(FCC_FILE_NAMES[HAM_FCC_FILE_LA], "r");
    if((*database)->la != NULL) {
        (*database)->la_open = HAM_BOOL_YES;
        (*database)->fcc_lengths->la_length = ham_get_lines_in_file((*database)->la);
        filesopen++;
    }

    (*database)->sc = fopen(FCC_FILE_NAMES[HAM_FCC_FILE_SC], "r");
    if((*database)->sc != NULL) {
        (*database)->sc_open = HAM_BOOL_YES;
        (*database)->fcc_lengths->sc_length = ham_get_lines_in_file((*database)->sc);
        filesopen++;
    }

    (*database)->sf = fopen(FCC_FILE_NAMES[HAM_FCC_FILE_SF], "r");
    if((*database)->sf != NULL) {
        (*database)->sf_open = HAM_BOOL_YES;
        (*database)->fcc_lengths->sf_length = ham_get_lines_in_file((*database)->sf);
        filesopen++;
    }

    if(filesopen < 1) {
        ham_fcc_terminate(*database);

        return HAM_ERROR_OPEN_FILE;
    }

    return HAM_OK;
}

LIBHAMDATA_API int ham_fcc_terminate(ham_fcc_database *database) {

    /* Can be safely called if already freed. */
    if(database == NULL)
        return HAM_OK;

    ham_fcc_close_all(database);

    free(database->fcc_lengths);
    free(database);

    return HAM_OK;
}

/*
 * Convert the FCC's text database to SQLite.
 *
 * If HAM_SQLITE_FILE_NAME already exists, it will be overwritten.
 */
LIBHAMDATA_API int ham_fcc_to_sqlite(const ham_fcc_database *fcc_database) {

    /*
     * This variable contains all the data need for the conversion process. It is passed to multiple
     * sub-functions.
     */
    ham_fcc_sqlite *fcc_sqlite;

    /* Conversion preparations */

    if(ham_sqlite_init(&fcc_sqlite))
        return HAM_ERROR_SQLITE_INIT;

    if(ham_sqlite_create_tables(fcc_sqlite))
        return HAM_ERROR_SQLITE_CREATE_TABLES;

    if(ham_sqlite_sql_prepare_stmt(fcc_sqlite))
        return HAM_ERROR_SQLITE_PREPARE_STMT;

    /* Perform the conversion */
    if(fcc_database->am_open == HAM_BOOL_YES)
        ham_sqlite_fcc_convert_file(fcc_sqlite, fcc_database->am, HAM_FCC_FILE_AM);

    if(fcc_database->en_open == HAM_BOOL_YES)
        ham_sqlite_fcc_convert_file(fcc_sqlite, fcc_database->en, HAM_FCC_FILE_EN);

    if(fcc_database->hd_open == HAM_BOOL_YES)
        ham_sqlite_fcc_convert_file(fcc_sqlite, fcc_database->hd, HAM_FCC_FILE_HD);

    if(fcc_database->hs_open == HAM_BOOL_YES)
        ham_sqlite_fcc_convert_file(fcc_sqlite, fcc_database->hs, HAM_FCC_FILE_HS);

    if(fcc_database->co_open == HAM_BOOL_YES)
        ham_sqlite_fcc_convert_file(fcc_sqlite, fcc_database->co, HAM_FCC_FILE_CO);

    if(fcc_database->la_open == HAM_BOOL_YES)
        ham_sqlite_fcc_convert_file(fcc_sqlite, fcc_database->la, HAM_FCC_FILE_LA);

    if(fcc_database->sc_open == HAM_BOOL_YES)
        ham_sqlite_fcc_convert_file(fcc_sqlite, fcc_database->sc, HAM_FCC_FILE_SC);

    if(fcc_database->sf_open == HAM_BOOL_YES)
        ham_sqlite_fcc_convert_file(fcc_sqlite, fcc_database->sf, HAM_FCC_FILE_SF);

    printf("Records inserted: %u\n", fcc_sqlite->sql_insert_calls);

    /* Clean up */
    ham_sqlite_sql_finalize_stmt(fcc_sqlite);
    ham_sqlite_terminate(fcc_sqlite);

    return HAM_OK;
}

int ham_sqlite_init(ham_fcc_sqlite **fcc_sqlite) {
    (*fcc_sqlite) = malloc(sizeof(ham_fcc_sqlite));
    if((*fcc_sqlite) == NULL)
        return HAM_ERROR_SQLITE_INIT;

    if(ham_sqlite_open_database_connection(&(*fcc_sqlite)->database, HAM_SQLITE_FILE_NAME)) {
        free((*fcc_sqlite));
        (*fcc_sqlite) = NULL;

        return HAM_ERROR_SQLITE_OPEN_DATABASE_CONNECTION;
    }

    if(sqlite3_exec((*fcc_sqlite)->database, "PRAGMA quick_check", NULL, NULL, NULL)) {
        printf("Error: database file corrupt! Reseting the file...\n");
        sqlite3_close((*fcc_sqlite)->database);

        if(ham_sqlite_reset_file(HAM_SQLITE_FILE_NAME)) {
            free((*fcc_sqlite));
            (*fcc_sqlite) = NULL;

            return HAM_ERROR_SQLITE_RESET_FILE;
        }

        ham_sqlite_open_database_connection(&(*fcc_sqlite)->database, HAM_SQLITE_FILE_NAME);
    }

    (*fcc_sqlite)->sql_insert_calls = 0;
    (*fcc_sqlite)->am_line = 0;
    (*fcc_sqlite)->en_line = 0;
    (*fcc_sqlite)->hd_line = 0;
    (*fcc_sqlite)->hs_line = 0;
    (*fcc_sqlite)->co_line = 0;
    (*fcc_sqlite)->la_line = 0;
    (*fcc_sqlite)->sc_line = 0;
    (*fcc_sqlite)->sf_line = 0;

    ham_sqlite_init_time(*fcc_sqlite);

    sqlite3_exec((*fcc_sqlite)->database, "PRAGMA syncronous = OFF", NULL, NULL, NULL);
    sqlite3_exec((*fcc_sqlite)->database, "PRAGMA journal_mode = MEMORY", NULL, NULL, NULL);

    sqlite3_exec((*fcc_sqlite)->database, "BEGIN TRANSACTION", NULL, NULL, NULL);

    return HAM_OK;
}

int ham_sqlite_terminate(ham_fcc_sqlite *fcc_sqlite) {

    /* Can be safely called if already freed. */
    if(fcc_sqlite == NULL)
        return HAM_OK;

    sqlite3_exec(fcc_sqlite->database, "END TRANSACTION", NULL, NULL, NULL);

    sqlite3_close(fcc_sqlite->database);

    free(fcc_sqlite);
    return HAM_OK;
}

int ham_sqlite_sql_prepare_stmt(ham_fcc_sqlite *fcc_sqlite) {

    if(sqlite3_prepare_v2(fcc_sqlite->database, HAM_SQLITE_INSERT_FCC_AM, -1,
                            &fcc_sqlite->am_stmt, NULL))
        return HAM_ERROR_SQLITE_PREPARE_STMT;

    if(sqlite3_prepare_v2(fcc_sqlite->database, HAM_SQLITE_INSERT_FCC_EN, -1,
                            &fcc_sqlite->en_stmt, NULL))
        return HAM_ERROR_SQLITE_PREPARE_STMT;

    if(sqlite3_prepare_v2(fcc_sqlite->database, HAM_SQLITE_INSERT_FCC_HD, -1,
                            &fcc_sqlite->hd_stmt, NULL))
        return HAM_ERROR_SQLITE_PREPARE_STMT;

    if(sqlite3_prepare_v2(fcc_sqlite->database, HAM_SQLITE_INSERT_FCC_HS, -1,
                            &fcc_sqlite->hs_stmt, NULL))
        return HAM_ERROR_SQLITE_PREPARE_STMT;

    if(sqlite3_prepare_v2(fcc_sqlite->database, HAM_SQLITE_INSERT_FCC_CO, -1,
                            &fcc_sqlite->co_stmt, NULL))
        return HAM_ERROR_SQLITE_PREPARE_STMT;

    if(sqlite3_prepare_v2(fcc_sqlite->database, HAM_SQLITE_INSERT_FCC_LA, -1,
                            &fcc_sqlite->la_stmt, NULL))
        return HAM_ERROR_SQLITE_PREPARE_STMT;

    if(sqlite3_prepare_v2(fcc_sqlite->database, HAM_SQLITE_INSERT_FCC_SC, -1,
                            &fcc_sqlite->sc_stmt, NULL))
        return HAM_ERROR_SQLITE_PREPARE_STMT;

    if(sqlite3_prepare_v2(fcc_sqlite->database, HAM_SQLITE_INSERT_FCC_SF, -1,
                            &fcc_sqlite->sf_stmt, NULL))
        return HAM_ERROR_SQLITE_PREPARE_STMT;

    return HAM_OK;
}

int ham_sqlite_sql_finalize_stmt(ham_fcc_sqlite *fcc_sqlite) {
    if(fcc_sqlite->am_stmt != NULL) {
        sqlite3_finalize(fcc_sqlite->am_stmt);
        fcc_sqlite->am_stmt = NULL;
    }

    if(fcc_sqlite->en_stmt != NULL) {
        sqlite3_finalize(fcc_sqlite->en_stmt);
        fcc_sqlite->en_stmt = NULL;
    }

    if(fcc_sqlite->hd_stmt != NULL) {
        sqlite3_finalize(fcc_sqlite->hd_stmt);
        fcc_sqlite->hd_stmt = NULL;
    }

    if(fcc_sqlite->hs_stmt != NULL) {
        sqlite3_finalize(fcc_sqlite->hs_stmt);
        fcc_sqlite->hs_stmt = NULL;
    }

    if(fcc_sqlite->co_stmt != NULL) {
        sqlite3_finalize(fcc_sqlite->co_stmt);
        fcc_sqlite->co_stmt = NULL;
    }

    if(fcc_sqlite->la_stmt != NULL) {
        sqlite3_finalize(fcc_sqlite->la_stmt);
        fcc_sqlite->la_stmt = NULL;
    }

    if(fcc_sqlite->sc_stmt != NULL) {
        sqlite3_finalize(fcc_sqlite->sc_stmt);
        fcc_sqlite->sc_stmt = NULL;
    }

    if(fcc_sqlite->sf_stmt != NULL) {
        sqlite3_finalize(fcc_sqlite->sf_stmt);
        fcc_sqlite->sf_stmt = NULL;
    }

    return HAM_OK;
}


/* Creates the HAM_SQLITE_FILE_NAME. If it already exists, reset it. */
int ham_sqlite_reset_file(const char *filename) {
    FILE *f = fopen(filename, "w");

    if(f == NULL)
        return HAM_ERROR_SQLITE_RESET_FILE;

    fclose(f);

    return HAM_OK;
}

int ham_sqlite_open_database_connection(sqlite3 **database, const char *filename) {
    if(sqlite3_open(filename, database)) {
        fprintf(stderr, "Error: unable to open database: %s\n", sqlite3_errmsg(*database));

        sqlite3_close(*database);
        return HAM_ERROR_SQLITE_OPEN_DATABASE_CONNECTION;
    }

    return HAM_OK;
}

int ham_sqlite_create_tables(ham_fcc_sqlite *fcc_sqlite) {
    if(sqlite3_exec(fcc_sqlite->database, HAM_SQLITE_TABLE_FCC_AM, NULL, NULL, NULL))
        return HAM_ERROR_SQLITE_CREATE_TABLES;

    if(sqlite3_exec(fcc_sqlite->database, HAM_SQLITE_TABLE_FCC_EN, NULL, NULL, NULL))
        return HAM_ERROR_SQLITE_CREATE_TABLES;

    if(sqlite3_exec(fcc_sqlite->database, HAM_SQLITE_TABLE_FCC_HD, NULL, NULL, NULL))
        return HAM_ERROR_SQLITE_CREATE_TABLES;

    if(sqlite3_exec(fcc_sqlite->database, HAM_SQLITE_TABLE_FCC_HS, NULL, NULL, NULL))
        return HAM_ERROR_SQLITE_CREATE_TABLES;

    if(sqlite3_exec(fcc_sqlite->database, HAM_SQLITE_TABLE_FCC_CO, NULL, NULL, NULL))
        return HAM_ERROR_SQLITE_CREATE_TABLES;

    if(sqlite3_exec(fcc_sqlite->database, HAM_SQLITE_TABLE_FCC_LA, NULL, NULL, NULL))
        return HAM_ERROR_SQLITE_CREATE_TABLES;

    if(sqlite3_exec(fcc_sqlite->database, HAM_SQLITE_TABLE_FCC_SC, NULL, NULL, NULL))
        return HAM_ERROR_SQLITE_CREATE_TABLES;

    if(sqlite3_exec(fcc_sqlite->database, HAM_SQLITE_TABLE_FCC_SF, NULL, NULL, NULL))
        return HAM_ERROR_SQLITE_CREATE_TABLES;

    return HAM_OK;
}

int ham_sqlite_fcc_convert_file(ham_fcc_sqlite *fcc_sqlite, FILE *data, const int fcc_file) {
    char buffer[HAM_BUFFER_SIZE];
    int error = HAM_OK;
    int num_fields = 0;
    sqlite3_stmt *sql_stmt = NULL;
    char **fields;

    unsigned int *currentline;

    switch (fcc_file) {
        case HAM_FCC_FILE_AM:
            error = ham_alloc_string_array(&fields, HAM_FCC_AM_FIELDS, HAM_BUFFER_SIZE);
            if(error != HAM_OK)
                return HAM_ERROR_MALLOC_FAIL;

            num_fields = HAM_FCC_AM_FIELDS;
            sql_stmt = fcc_sqlite->am_stmt;
            currentline = &fcc_sqlite->am_line;
            break;

        case HAM_FCC_FILE_EN:
            error = ham_alloc_string_array(&fields, HAM_FCC_EN_FIELDS, HAM_BUFFER_SIZE);
            if(error != HAM_OK)
                return HAM_ERROR_MALLOC_FAIL;

            num_fields = HAM_FCC_EN_FIELDS;
            sql_stmt = fcc_sqlite->en_stmt;
            currentline = &fcc_sqlite->en_line;
            break;

        case HAM_FCC_FILE_HD:
            error = ham_alloc_string_array(&fields, HAM_FCC_HD_FIELDS, HAM_BUFFER_SIZE);
            if(error != HAM_OK)
                return HAM_ERROR_MALLOC_FAIL;

            num_fields = HAM_FCC_HD_FIELDS;
            sql_stmt = fcc_sqlite->hd_stmt;
            currentline = &fcc_sqlite->hd_line;
            break;

        case HAM_FCC_FILE_HS:
            error = ham_alloc_string_array(&fields, HAM_FCC_HS_FIELDS, HAM_BUFFER_SIZE);
            if(error != HAM_OK)
                return HAM_ERROR_MALLOC_FAIL;

            num_fields = HAM_FCC_HS_FIELDS;
            sql_stmt = fcc_sqlite->hs_stmt;
            currentline = &fcc_sqlite->hs_line;
            break;

        case HAM_FCC_FILE_CO:
            error = ham_alloc_string_array(&fields, HAM_FCC_CO_FIELDS, HAM_BUFFER_SIZE);
            if(error != HAM_OK)
                return HAM_ERROR_MALLOC_FAIL;

            num_fields = HAM_FCC_CO_FIELDS;
            sql_stmt = fcc_sqlite->co_stmt;
            currentline = &fcc_sqlite->co_line;
            break;

        case HAM_FCC_FILE_LA:
            error = ham_alloc_string_array(&fields, HAM_FCC_LA_FIELDS, HAM_BUFFER_SIZE);
            if(error != HAM_OK)
                return HAM_ERROR_MALLOC_FAIL;

            num_fields = HAM_FCC_LA_FIELDS;
            sql_stmt = fcc_sqlite->la_stmt;
            currentline = &fcc_sqlite->la_line;
            break;

        case HAM_FCC_FILE_SC:
            error = ham_alloc_string_array(&fields, HAM_FCC_SC_FIELDS, HAM_BUFFER_SIZE);

            if(error != HAM_OK)
                return HAM_ERROR_MALLOC_FAIL;
            num_fields = HAM_FCC_SC_FIELDS;
            sql_stmt = fcc_sqlite->sc_stmt;
            currentline = &fcc_sqlite->sc_line;
            break;

        case HAM_FCC_FILE_SF:
            error = ham_alloc_string_array(&fields, HAM_FCC_SF_FIELDS, HAM_BUFFER_SIZE);
            if(error != HAM_OK)
                return HAM_ERROR_MALLOC_FAIL;

            num_fields = HAM_FCC_SF_FIELDS;
            sql_stmt = fcc_sqlite->sf_stmt;
            currentline = &fcc_sqlite->sf_line;
            break;

        default:
            return HAM_ERROR_GENERIC;
    }

    memset(buffer, HAM_NULL_CHAR, HAM_BUFFER_SIZE);

    while(fgets(buffer, HAM_BUFFER_SIZE, data) != NULL) {
        (*currentline)++;

        /*
         * fgets includes the new line at the end of the buffer; we need to replace it with a null
         * char.
         */
         char *endofline = strpbrk(buffer, "\n");
         if(endofline != NULL) {
           memset(endofline, HAM_NULL_CHAR, 1);
         }

         endofline = strpbrk(buffer, "\r");
         if(endofline != NULL) {
           memset(endofline, HAM_NULL_CHAR, 1);
         }



        error = ham_parse_line_with_delimiter((char **)fields, buffer, num_fields, HAM_DELIMITER);
        if(error != HAM_OK) {
            ham_free_string_array(&fields, num_fields, HAM_BUFFER_SIZE);
            return HAM_ERROR_GENERIC;
        }

        ham_sqlite_insert_fields(fcc_sqlite, fields, num_fields, sql_stmt, fcc_file, *currentline);

        memset(buffer, HAM_NULL_CHAR, HAM_BUFFER_SIZE);
    }

    ham_free_string_array(&fields, num_fields, HAM_BUFFER_SIZE);

    return error;
}

int ham_sqlite_insert_fields(ham_fcc_sqlite *fcc_sqlite, char **fields, const int num_fields,
                                sqlite3_stmt *sql_stmt, const int fcc_file, const int currentline) {

    int rc = 0;

    for(int i = 0; i < num_fields; i++) {

        if(strlen(fields[i]) == 0)
            rc = sqlite3_bind_null(sql_stmt, i+1);
        else
            rc = sqlite3_bind_text(sql_stmt, i+1, fields[i], -1, SQLITE_TRANSIENT);

        if(rc != SQLITE_OK) {
            fprintf(stderr, "Error (%d): paramater binding failed. * File: %s * Index: %d\n", rc,
                        FCC_FILE_NAMES[fcc_file], i);

            return HAM_ERROR_GENERIC;
        }
    }

    sqlite3_bind_text(sql_stmt, num_fields + 1, fcc_sqlite->time, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(sql_stmt, num_fields + 2, fcc_sqlite->time, -1, SQLITE_TRANSIENT);

    rc = sqlite3_step(sql_stmt);

    sqlite3_clear_bindings(sql_stmt);
    sqlite3_reset(sql_stmt);

    if(rc != SQLITE_DONE)
    {
        fprintf(stderr, "Error (%d): Message: %s - Failed to insert record. File: %s; Line: %u\n", rc,
                    sqlite3_errmsg(fcc_sqlite->database), FCC_FILE_NAMES[fcc_file], currentline);

        return HAM_ERROR_SQLITE_INSERT;
    }

    fcc_sqlite->sql_insert_calls++;

    return HAM_OK;
}
