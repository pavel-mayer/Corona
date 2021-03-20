
import argparse
import datatable as dt
import numpy as np
import psutil
import os
import time

def makeRandomTable(rows, columns):
    array = np.random(rows, columns)
    return dt.Frame(array)

def memUsageGB():
    return psutil.Process(os.getpid()).memory_info().rss/1024/1024/1024

def printMemoryUsage(where):
    print("Memory Usage @ {}: {:.3f} GB".format(where, memUsageGB()))  # in bytes

def main():
    parser = argparse.ArgumentParser(description='Test performance of large table writes')
    parser.add_argument('-d', '--output-dir', dest='outputDir', default=".")
    parser.add_argument('-r','--rows', type=int, default=100000000, help='Number of rows for the test table')
    parser.add_argument('-c','--columns', type=int, default=100, help='Number of columns for the test table')
    parser.add_argument('-f','--factor', type=int, default=30, help='Number of .rbind() -calls to create large table')
    parser.add_argument('-m','--memorylimit', type=int, default=20, help='stop enlarging the table when memory usage reaches this limit in GB')
    args = parser.parse_args()
    print(args)

    rows = int(args.rows)
    columns = int(args.columns)
    columnsSmall = int(args.columns/args.factor)

    array = np.random.rand(rows, columnsSmall)
    fullTable = dt.Frame(array)
    print(fullTable)

    testFileName = args.outputDir+"/large-testfile.jay"
    for n in range(int(args.factor)):
        start = time.perf_counter()
        array = np.random.rand(rows, columnsSmall)
        newTable = dt.Frame(array)
        printMemoryUsage("#{} after new smaller table created".format(n))

        fullTable.rbind(newTable)
        printMemoryUsage("#{} after rbind".format(n))

        startSave = time.perf_counter()
        fullTable.to_jay(testFileName)
        duration = time.perf_counter() - startSave
        print("{} Writing {} rows took {:.3f} secs {:.3f} minutes {:.3f} hours, {:.2f} rows/sec".
              format(n, fullTable.nrows,duration, duration/60, duration/60/60, fullTable.nrows/duration))
        printMemoryUsage("#{} after to_jay()".format(n))

        if memUsageGB() > args.memorylimit:
            print("Memory limit reached")
            break;

        startRead = time.perf_counter()
        fullTable = dt.fread(testFileName)
        duration = time.perf_counter() - startRead
        print("{} Reading {} rows took {:.3f} secs {:.3f} minutes {:.3f} hours, {:.2f} rows/sec".
              format(n, fullTable.nrows, duration, duration / 60, duration / 60 / 60, fullTable.nrows / duration))
        printMemoryUsage("#{} after fread()".format(n))






    #dt.options.progress.enabled = False

    for f in sorted(args.files):
        convert(f, args.outputDir)

if __name__ == "__main__":
    # execute only if run as a script
    main()