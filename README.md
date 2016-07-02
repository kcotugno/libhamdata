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
nmake install
```
This will install the necessary executable into build\bin folder. All you need now is the FCC files
and you're ready to go.

## Linux
I haven't tested it on Linux yet. Will do soon...

# Running
To run, just unzip the FCC files into the program directory and run.
