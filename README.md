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

# Notes
While the proper SQLite file is produced, my test run took over 24hrs. I have tracked down the
bottleneck to the SQLite insertion calls. As the code is now, a statement is executed for each line
in the files. This is over 7 million insert commands! I will fix soon by doing batch insertions of
1000 entries. This will cut down the calls to modify the database immensely. If performance still
is not satisfactory, I may increase it to 10000.
