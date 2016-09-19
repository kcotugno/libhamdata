# libhamdata
Library for converting the FCC Amateur Radio database files to SQLite.

# Building

## Windows
To build on Windows you must install cmake version 3.6 or greater. You will also probably need to download the SQLite
amalgamation file and place them in the source folder so they can be built. Now, open a MSVC command prompt, and
navigate to the project folder, and run the following commands:
```
mkdir build
cd build
cmake -G "NMake Makefiles" -DCMAKE_BUILD_TYPE=release ..
nmake
```

## Linux
Tested on Ubuntu and Arch. To compile, navigate to the project directory and run the following commands in a terminal:
```
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=release ..
make
```

## Mac OS X
Same as Linux. If you want a newer sqlite version, install it from brew and add these defines. Set the path to
whatever your brew specifies.
```
cmake -DCMAKE_BUILD_TYPE=release -DCMAKE_INCLUDE_PATH=/usr/local/Cellar/sqlite/3.14.2/include/ -DCMAKE_LIBRARY_PATH=/usr/local/Cellar/sqlite/3.14.2/lib/ ..
```

# Running
To run the included conversion program, just unzip the FCC files into the program directory and run ham_data.
