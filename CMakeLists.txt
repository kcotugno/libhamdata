cmake_minimum_required(VERSION 3.6)
project(libhamdata)

add_library(sqlite3 SHARED sqlite3.c)

add_library(libhamdata SHARED libhamdata.c)
target_link_libraries(libhamdata sqlite3)

add_executable(ham_data ham_data.c)
target_link_libraries(ham_data libhamdata)

if(MSVC)
set_target_properties(sqlite3
                            PROPERTIES COMPILE_FLAGS "/D SQLITE_API=__declspec(dllexport)")
set_target_properties(libhamdata
                            PROPERTIES COMPILE_FLAGS "/DLIBHAMDATA_EXPORTS /D_CRT_SECURE_NO_WARNINGS")
endif()

install(TARGETS  sqlite3 libhamdata ham_data
        RUNTIME DESTINATION ${CMAKE_BINARY_DIR}/bin
        LIBRARY DESTINATION ${CMAKE_BINARY_DIR}/lib)
