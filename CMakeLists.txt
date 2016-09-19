cmake_minimum_required(VERSION 3.5)
project(libhamdata)

# Get sqlite3
find_path(SQLITE3_INCLUDE_DIR sqlite3.h)
find_library(SQLITE3_LIBRARY sqlite3)
message(${SQLITE3_LIBRARY})
set(CMAKE_SHARED_LIBRARY_PREFIX "")

if((NOT SQLITE3_INCLUDE_DIR) OR (NOT SQLITE3_LIBRARY))

  # Check the source directory for the sqlite amalgamation files to compile
  if((EXISTS ${PROJECT_SOURCE_DIR}/sqlite3.h) AND (EXISTS ${PROJECT_SOURCE_DIR}/sqlite3.c))
    add_library(sqlite3 SHARED sqlite3.c)
    set_target_properties(sqlite3 PROPERTIES COMPILE_FLAGS "/DSQLITE_API=__declspec(dllexport)")
    set(SQLITE3_SRC true)
  else()
  message(SEND_ERROR "Failed to find sqlite3. You're probably on Windows so you should download the amalgamation "
                                     "files from sqlite.org and place them in the project directory")
  endif()
endif()

add_library(libhamdata SHARED libhamdata.c)
add_executable(ham_data ham_data.c)

if(MSVC)
  set_target_properties(libhamdata PROPERTIES COMPILE_FLAGS "/D_CRT_SECURE_NO_WARNINGS")
endif()

set_target_properties(libhamdata PROPERTIES DEFINE_SYMBOL "LIBHAMDATA_EXPORTS")

if(SQLITE3_SRC)
  target_link_libraries(libhamdata sqlite3)
else()
  target_link_libraries(libhamdata ${SQLITE3_LIBRARY})
endif()

target_link_libraries(ham_data libhamdata)
