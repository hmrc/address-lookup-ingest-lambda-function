#!/usr/bin/env python
"""
PREREQUISITE

	Python 2.7 - this script has only been tested against Python 2.7

SYNOPSIS

    To run script: python AddressBasePremium_RecordSplitter.py

DESCRIPTION

    This Python script can be used to seperate each AddressBase Premium 
    CSV file into new CSV files based on the record identifiers found
    within AddressBase Premium. The script will read both AddressBase Premium 
    CSV files and zipped CSV files. If you have zipped files it is unneccessary
    to extract the zip file first as the script reads data directly from the zip
    file.

LICENSE

    Crown Copyright (c) 2016 Ordnance Survey

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" 
    AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
    WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. 
    IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, 
    INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, 
    BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, 
    DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF 
    LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR 
    OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED 
    OF THE POSSIBILITY OF SUCH DAMAGE

VERSION

    0.2
"""

# Imported modules
import zipfile
import csv
import sys
import os
import StringIO
import time
from time import strftime
from constants import *


def createCSV(input_directory_path):
    print 'This program will split OS AddressBase Premium Zip CSV or extracted CSV files by record identifier into new CSV files'
    starttime = time.time()

    # Rather than using arguments the program asks the user to input the path to the folder of OS AddressBase Premium files
    if input_directory_path is None:
        print 'Please type in the full path to the directory of OS AddressBase zip files:'
        directorypath = raw_input('Directory Path: ')
    else:
        directorypath = input_directory_path

    # An emtpy array and counter used to store and count the number of CSV files the program finds
    csvfileList = []
    # An emtpy array and counter used to store and count the number of Zip files the program finds
    zipfileList = []
    zipfileCount = 0
    # The following code walks the directory path that the user input earlier and searches for either CSV files or Zip files
    # It will initially see if AddressBasePremium is in the filename if not it will then check the first value of the file against
    # the record types above. Obviously is there is a CSV in the folder that has one of these record types as a first value
    # it will also be included.
    for dirname, dirnames, filenames in os.walk(directorypath):
        for filename in filenames:
            if filename.endswith(".zip"):
                zippath = os.path.join(directorypath, filename)
                zipfileList.append(zippath)
                zipfileCount += 1
            else:
                pass
    # The following section makes sure that it is dealing with the correct files, either CSV or Zip but not both types
    try:
        if zipfileCount > 0:
            print "Program has found %s OS AddressBase Premium Zipped CSV Files" % zipfileCount
        else:
            pass

        if zipfileCount == 0:
            print "Program could not find any OS AddressBase Premium files and will now exit"
            time.sleep(5)
            sys.exit()
    finally:
        pass

    csv_writers = make_csv_writers(fileNameToHeadingsMap)

    print 'Finished creating empty CSV files with Header line'

    # Counter to keep track of the number of files processed
    processed = 0

    # The following section processes the Zip files.
    # It uses the Python Zipfile module to read the data directly from the Zip file preventing the user having
    # to extract the files before splitting the data.
    if zipfileCount > 0:
        print "Program will now split the OS AddressBase Premium files"
        for filepath in zipfileList:
            processed += 1
            print "Processing file number " + str(processed) + " out of " + str(zipfileCount)
            basename = os.path.basename(filepath)
            shortzipfile = os.path.splitext(basename)[0]
            removecsvzip = shortzipfile[0:-4]
            zipcsv = '.'.join([shortzipfile, 'csv'])
            zipcsv2 = '.'.join([removecsvzip, 'csv'])
            with open(filepath, 'rb') as zname:
                zfile = zipfile.ZipFile(zname)
                try:
                    data = StringIO.StringIO(zfile.read(zipcsv))
                    csvreader = csv.reader(data, delimiter=',', doublequote=False, lineterminator='\n', quotechar='"',
                                           quoting=0, skipinitialspace=True)
                    for row in csvreader:
                        abtype = row[0]
                        csv_writers[abtype].writeRow(row)
                except KeyError as e:
                    data = StringIO.StringIO(zfile.read(zipcsv2))
                    csvreader = csv.reader(data, delimiter=',', doublequote=False, lineterminator='\n', quotechar='"',
                                           quoting=0, skipinitialspace=True)
                    for row in csvreader:
                        abtype = row[0]
                        csv_writers[abtype].writeRow(row)
                finally:
                    pass

    endtime = time.time()
    elapsed = endtime - starttime
    # Summary statistics showing number of records and time taken
    print "Program has finished splitting the AddressBase Premium Files"
    print 'Finished translating data at ', strftime("%a, %d %b %Y %H:%M:%S")
    print 'Elapsed time: ', round(elapsed / 60, 1), ' minutes'
    print "Number of Header Records: %s" % str(csv_writers['10'].counter)
    print "Number of Street Records: %s" % str(csv_writers['11'].counter)
    print "Number of Street Descriptor Records: %s" % str(csv_writers['15'].counter)
    print "Number of BLPU Records: %s" % str(csv_writers['21'].counter)
    print "Number of XRef Records: %s" % str(csv_writers['23'].counter)
    print "Number of LPI Records: %s" % str(csv_writers['24'].counter)
    print "Number of Delivery Point Records: %s" % str(csv_writers['28'].counter)
    print "Number of Metadata Records: %s" % str(csv_writers['29'].counter)
    print "Number of Successor Records: %s" % str(csv_writers['30'].counter)
    print "Number of Organisation Records: %s" % str(csv_writers['31'].counter)
    print "Number of Classification Records: %s" % str(csv_writers['32'].counter)
    print "Number of Trailer Records: %s" % str(csv_writers['99'].counter)


def make_csv_writers(files_map):
    return {k: make_csv_writer(v) for k, v in files_map.items()}


def make_csv_writer(value):
    if os.path.isfile(value['file_name']):
        os.remove(value['file_name'])

    output_file = open(value['file_name'], 'a')
    csv_writer = csv.writer(output_file, delimiter=',', quotechar='"', lineterminator='\n')
    csv_writer.writerow(value['headers'])

    return CountingCsvWriter(csv_writer)


class CountingCsvWriter:
    def __init__(self, a_csv_writer):
        self.csv_writer = a_csv_writer
        self.counter = 0

    def writeRow(self, row):
        self.csv_writer.writerow(row)
        self.counter += 1


def main(input_dir):
    createCSV(input_dir)


if __name__ == '__main__':
    main(sys.argv[1])
