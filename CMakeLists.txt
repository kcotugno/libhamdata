cmake_minimum_required(VERSION 3.5)
project(libhamdata)

add_library(sqlite3 SHARED sqlite3.c)
add_library(libhamdata SHARED libhamdata.c)
add_executable(ham_data ham_data.c)

if(MSVC)
set_target_properties(sqlite3
                            PROPERTIES COMPILE_FLAGS "/DSQLITE_API=__declspec(dllexport)")

set_target_properties(libhamdata
                        PROPERTIES COMPILE_FLAGS "/D_CRT_SECURE_NO_WARNINGS")
else() # *nix type systems
target_link_libraries(sqlite3 pthread dl)
endif()

set_target_properties(libhamdata
                            PROPERTIES DEFINE_SYMBOL "LIBHAMDATA_EXPORTS")

target_link_libraries(libhamdata sqlite3)
target_link_libraries(ham_data libhamdata)
