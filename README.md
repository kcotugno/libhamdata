# libhamdata
Library for converting the FCC Amateur Radio database files to SQLite.

# Building

## Windows
To build on Windows you must install cmake version 3.6 or greater. Open a MSVC command prompt, and
navigate to the project folder, and run the following commands:
```
mkdir build
cd build
cmake -G "NMake Makefiles" -DCMAKE_BUILD_TYPE=release ..
nmake
```
This will install the necessary executable into build\bin folder. All you need now is the FCC files
and you're ready to go.

## Linux
Tested on Ubuntu and Arch. To compile, navigate to the project directory and run the following commands in a terminal.
```
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=release ..
make
```

# Running
To run, just unzip the FCC files into the program directory and run.
