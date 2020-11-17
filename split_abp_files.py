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
    csvfileCount = 0
    # An emtpy array and counter used to store and count the number of Zip files the program finds
    zipfileList = []
    zipfileCount = 0
    # The following code walks the directory path that the user input earlier and searches for either CSV files or Zip files
    # It will initially see if AddressBasePremium is in the filename if not it will then check the first value of the file against
    # the record types above. Obviously is there is a CSV in the folder that has one of these record types as a first value
    # it will also be included.
    for dirname, dirnames, filenames in os.walk(directorypath):
        for filename in filenames:
            if filename.endswith(".csv"):
                csvfile = os.path.join(directorypath, filename)
                csvfileList.append(csvfile)
                csvfileCount += 1
            elif filename.endswith(".zip"):
                zippath = os.path.join(directorypath, filename)
                zipfileList.append(zippath)
                zipfileCount += 1
            else:
                pass
    # The following section makes sure that it is dealing with the correct files, either CSV or Zip but not both types
    try:
        if csvfileCount > 0 and zipfileCount > 0:
            print "Program has found both OS AddressBase Premium CSV  and ZIP files."
            print "Please tidy up the folder of files and try again"
            time.sleep(5)
            sys.exit()
        else:
            pass
        if csvfileCount > 0:
            print "Program has found %s OS AddressBase Premium CSV Files" % csvfileCount
        else:
            pass
        if zipfileCount > 0:
            print "Program has found %s OS AddressBase Premium Zipped CSV Files" % zipfileCount
        else:
            pass

        if csvfileCount == 0 and zipfileCount == 0:
            print "Program could not find any OS AddressBase Premium files and will now exit"
            time.sleep(5)
            sys.exit()
    finally:
        pass

    # Create the ID10_Header_Records.csv file, it checks to see if it exists first, if so deletes it, then creates a fresh one
    if os.path.isfile('ID10_Header_Records.csv'):
        os.remove('ID10_Header_Records.csv')
        header_10 = open('ID10_Header_Records.csv', 'a')
        write10 = csv.writer(header_10, delimiter=',', quotechar='"', lineterminator='\n')
    else:
        header_10 = open('ID10_Header_Records.csv', 'a')
        write10 = csv.writer(header_10, delimiter=',', quotechar='"', lineterminator='\n')
    # Create the ID11_Street_Records.csv file, it checks to see if it exists first, if so deletes it, then creates a fresh one
    if os.path.isfile('ID11_Street_Records.csv'):
        os.remove('ID11_Street_Records.csv')
        street_11 = open('ID11_Street_Records.csv', 'a')
        write11 = csv.writer(street_11, delimiter=',', quotechar='"', lineterminator='\n')
    else:
        street_11 = open('ID11_Street_Records.csv', 'a')
        write11 = csv.writer(street_11, delimiter=',', quotechar='"', lineterminator='\n')
    # Create the ID15_StreetDesc_Records.csv file, it checks to see if it exists first, if so deletes it, then creates a fresh one
    if os.path.isfile('ID15_StreetDesc_Records.csv'):
        os.remove('ID15_StreetDesc_Records.csv')
        streetdesc_15 = open('ID15_StreetDesc_Records.csv', 'a')
        write15 = csv.writer(streetdesc_15, delimiter=',', quotechar='"', lineterminator='\n')
    else:
        streetdesc_15 = open('ID15_StreetDesc_Records.csv', 'a')
        write15 = csv.writer(streetdesc_15, delimiter=',', quotechar='"', lineterminator='\n')
    # Create the ID21_BLPU_Records.csv file, it checks to see if it exists first, if so deletes it, then creates a fresh one
    if os.path.isfile('ID21_BLPU_Records.csv'):
        os.remove('ID21_BLPU_Records.csv')
        blpu_21 = open('ID21_BLPU_Records.csv', 'a')
        write21 = csv.writer(blpu_21, delimiter=',', quotechar='"', lineterminator='\n')
    else:
        blpu_21 = open('ID21_BLPU_Records.csv', 'a')
        write21 = csv.writer(blpu_21, delimiter=',', quotechar='"', lineterminator='\n')
    # Create the ID23_XREF_Records.csv file, it checks to see if it exists first, if so deletes it, then creates a fresh one
    if os.path.isfile('ID23_XREF_Records.csv'):
        os.remove('ID23_XREF_Records.csv')
        xref_23 = open('ID23_XREF_Records.csv', 'a')
        write23 = csv.writer(xref_23, delimiter=',', quotechar='"', lineterminator='\n')
    else:
        xref_23 = open('ID23_XREF_Records.csv', 'a')
        write23 = csv.writer(xref_23, delimiter=',', quotechar='"', lineterminator='\n')
    # Create the ID24_LPI_Records.csv file, it checks to see if it exists first, if so deletes it, then creates a fresh one
    if os.path.isfile('ID24_LPI_Records.csv'):
        os.remove('ID24_LPI_Records.csv')
        lpi_24 = open('ID24_LPI_Records.csv', 'a')
        write24 = csv.writer(lpi_24, delimiter=',', quotechar='"', lineterminator='\n')
    else:
        lpi_24 = open('ID24_LPI_Records.csv', 'a')
        write24 = csv.writer(lpi_24, delimiter=',', quotechar='"', lineterminator='\n')
    # Create the ID28_DPA_Records.csv file, it checks to see if it exists first, if so deletes it, then creates a fresh one
    if os.path.isfile('ID28_DPA_Records.csv'):
        os.remove('ID28_DPA_Records.csv')
        dp_28 = open('ID28_DPA_Records.csv', 'a')
        write28 = csv.writer(dp_28, delimiter=',', quotechar='"', lineterminator='\n')
    else:
        dp_28 = open('ID28_DPA_Records.csv', 'a')
        write28 = csv.writer(dp_28, delimiter=',', quotechar='"', lineterminator='\n')
    # Create the ID29_Metadata_Records.csv file, it checks to see if it exists first, if so deletes it, then creates a fresh one
    if os.path.isfile('ID29_Metadata_Records.csv'):
        os.remove('ID29_Metadata_Records.csv')
        meta_29 = open('ID29_Metadata_Records.csv', 'a')
        write29 = csv.writer(meta_29, delimiter=',', quotechar='"', lineterminator='\n')
    else:
        meta_29 = open('ID29_Metadata_Records.csv', 'a')
        write29 = csv.writer(meta_29, delimiter=',', quotechar='"', lineterminator='\n')
    # Create the ID30_Successor_Records.csv file, it checks to see if it exists first, if so deletes it, then creates a fresh one
    if os.path.isfile('ID30_Successor_Records.csv'):
        os.remove('ID30_Successor_Records.csv')
        suc_30 = open('ID30_Successor_Records.csv', 'a')
        write30 = csv.writer(suc_30, delimiter=',', quotechar='"', lineterminator='\n')
    else:
        suc_30 = open('ID30_Successor_Records.csv', 'a')
        write30 = csv.writer(suc_30, delimiter=',', quotechar='"', lineterminator='\n')
    # Create the ID31_Org_Records.csv file, it checks to see if it exists first, if so deletes it, then creates a fresh one
    if os.path.isfile('ID31_Org_Records.csv'):
        os.remove('ID31_Org_Records.csv')
        org_31 = open('ID31_Org_Records.csv', 'a')
        write31 = csv.writer(org_31, delimiter=',', quotechar='"', lineterminator='\n')
    else:
        org_31 = open('ID31_Org_Records.csv', 'a')
        write31 = csv.writer(org_31, delimiter=',', quotechar='"', lineterminator='\n')
    # Create the ID32_Class_Records.csv file, it checks to see if it exists first, if so deletes it, then creates a fresh one
    if os.path.isfile('ID32_Class_Records.csv'):
        os.remove('ID32_Class_Records.csv')
        class_32 = open('ID32_Class_Records.csv', 'a')
        write32 = csv.writer(class_32, delimiter=',', quotechar='"', lineterminator='\n')
    else:
        class_32 = open('ID32_Class_Records.csv', 'a')
        write32 = csv.writer(class_32, delimiter=',', quotechar='"', lineterminator='\n')
    # Create the ID99_Trailer_Records.csv file, it checks to see if it exists first, if so deletes it, then creates a fresh one
    if os.path.isfile('ID99_Trailer_Records.csv'):
        os.remove('ID99_Trailer_Records.csv')
        trailer_99 = open('ID99_Trailer_Records.csv', 'a')
        write99 = csv.writer(trailer_99, delimiter=',', quotechar='"', lineterminator='\n')
    else:
        trailer_99 = open('ID99_Trailer_Records.csv', 'a')
        write99 = csv.writer(trailer_99, delimiter=',', quotechar='"', lineterminator='\n')
    print 'Finished creating empty CSV files with Header line'
    # The following counters are used to keep track of how many records of each Record Identifier type are found
    counter10 = 0
    counter11 = 0
    counter15 = 0
    counter21 = 0
    counter23 = 0
    counter24 = 0
    counter28 = 0
    counter29 = 0
    counter30 = 0
    counter31 = 0
    counter32 = 0
    counter99 = 0

    # Counter to keep track of the number of files processed
    processed = 0
    # There is a different routine for processing CSV files compared to ZIP files
    # This sections processes the CSV files using the Python CSV reader and write modules
    # It used the first value of the row to determine which CSV file that row should be written to.
    if csvfileCount > 0:
        print "Program will now split the OS AddressBase Premium files"
        for filepath in csvfileList:
            processed += 1
            print "Processing file number " + str(processed) + " out of " + str(csvfileCount)
            with open(filepath) as f:
                csvreader = csv.reader(f, delimiter=',', doublequote=False, lineterminator='\n', quotechar='"',
                                       quoting=0, skipinitialspace=True)
                try:
                    for row in csvreader:
                        abtype = row[0]
                        if "10" in abtype:
                            write10.writerow(row)
                            counter10 += 1
                        elif "11" in abtype:
                            write11.writerow(row)
                            counter11 += 1
                        elif "15" in abtype:
                            write15.writerow(row)
                            counter15 += 1
                        elif "21" in abtype:
                            write21.writerow(row)
                            counter21 += 1
                        elif "23" in abtype:
                            write23.writerow(row)
                            counter23 += 1
                        elif "24" in abtype:
                            write24.writerow(row)
                            counter24 += 1
                        elif "28" in abtype:
                            write28.writerow(row)
                            counter28 += 1
                        elif "29" in abtype:
                            write29.writerow(row)
                            counter29 += 1
                        elif "30" in abtype:
                            write30.writerow(row)
                            counter30 += 1
                        elif "31" in abtype:
                            write31.writerow(row)
                            counter31 += 1
                        elif "32" in abtype:
                            write32.writerow(row)
                            counter32 += 1
                        elif "99" in abtype:
                            write99.writerow(row)
                            counter99 += 1
                        else:
                            pass
                except KeyError as e:
                    pass
        header_10.close()
        street_11.close()
        streetdesc_15.close()
        blpu_21.close()
        xref_23.close()
        lpi_24.close()
        dp_28.close()
        meta_29.close()
        suc_30.close()
        org_31.close()
        class_32.close()
        trailer_99.close()

    else:
        pass
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
                        if "10" in abtype:
                            write10.writerow(row)
                            counter10 += 1
                        elif "11" in abtype:
                            write11.writerow(row)
                            counter11 += 1
                        elif "15" in abtype:
                            write15.writerow(row)
                            counter15 += 1
                        elif "21" in abtype:
                            write21.writerow(row)
                            counter21 += 1
                        elif "23" in abtype:
                            write23.writerow(row)
                            counter23 += 1
                        elif "24" in abtype:
                            write24.writerow(row)
                            counter24 += 1
                        elif "28" in abtype:
                            write28.writerow(row)
                            counter28 += 1
                        elif "29" in abtype:
                            write29.writerow(row)
                            counter29 += 1
                        elif "30" in abtype:
                            write30.writerow(row)
                            counter30 += 1
                        elif "31" in abtype:
                            write31.writerow(row)
                            counter31 += 1
                        elif "32" in abtype:
                            write32.writerow(row)
                            counter32 += 1
                        elif "99" in abtype:
                            write99.writerow(row)
                            counter99 += 1
                        else:
                            pass

                except KeyError as e:
                    data = StringIO.StringIO(zfile.read(zipcsv2))
                    csvreader = csv.reader(data, delimiter=',', doublequote=False, lineterminator='\n', quotechar='"',
                                           quoting=0, skipinitialspace=True)
                    for row in csvreader:
                        abtype = row[0]
                        if "10" in abtype:
                            write10.writerow(row)
                            counter10 += 1
                        elif "11" in abtype:
                            write11.writerow(row)
                            counter11 += 1
                        elif "15" in abtype:
                            write15.writerow(row)
                            counter15 += 1
                        elif "21" in abtype:
                            write21.writerow(row)
                            counter21 += 1
                        elif "23" in abtype:
                            write23.writerow(row)
                            counter23 += 1
                        elif "24" in abtype:
                            write24.writerow(row)
                            counter24 += 1
                        elif "28" in abtype:
                            write28.writerow(row)
                            counter28 += 1
                        elif "29" in abtype:
                            write29.writerow(row)
                            counter29 += 1
                        elif "30" in abtype:
                            write30.writerow(row)
                            counter30 += 1
                        elif "31" in abtype:
                            write31.writerow(row)
                            counter31 += 1
                        elif "32" in abtype:
                            write32.writerow(row)
                            counter32 += 1
                        elif "99" in abtype:
                            write99.writerow(row)
                            counter99 += 1
                        else:
                            pass
                finally:
                    pass
        header_10.close()
        street_11.close()
        streetdesc_15.close()
        blpu_21.close()
        xref_23.close()
        lpi_24.close()
        dp_28.close()
        meta_29.close()
        suc_30.close()
        org_31.close()
        class_32.close()
        trailer_99.close()

    endtime = time.time()
    elapsed = endtime - starttime
    # Summary statistics showing number of records and time taken
    print "Program has finished splitting the AddressBase Premium Files"
    print 'Finished translating data at ', strftime("%a, %d %b %Y %H:%M:%S")
    print 'Elapsed time: ', round(elapsed / 60, 1), ' minutes'
    print "Number of Header Records: %s" % str(counter10)
    print "Number of Street Records: %s" % str(counter11)
    print "Number of Street Descriptor Records: %s" % str(counter15)
    print "Number of BLPU Records: %s" % str(counter21)
    print "Number of XRef Records: %s" % str(counter23)
    print "Number of LPI Records: %s" % str(counter24)
    print "Number of Delivery Point Records: %s" % str(counter28)
    print "Number of Metadata Records: %s" % str(counter29)
    print "Number of Successor Records: %s" % str(counter30)
    print "Number of Organisation Records: %s" % str(counter31)
    print "Number of Classification Records: %s" % str(counter32)
    print "Number of Trailer Records: %s" % str(counter99)

    print "The program will close in 10 seconds"
    time.sleep(10)


# sys.exit()

def main():
    createCSV()


if __name__ == '__main__':
    main()
