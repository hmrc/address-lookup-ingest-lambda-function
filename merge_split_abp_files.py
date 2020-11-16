#!/usr/bin/env python

import sys
import os
import time

# Header lines for the new CSV files these are used later to when writing the header to the new CSV files
fileNameToHeadingsMap = {
    'ID10_Header_Records.csv': ["RECORD_IDENTIFIER", "CUSTODIAN_NAME", "LOCAL_CUSTODIAN_NAME", "PROCESS_DATE",
                                "VOLUME_NUMBER", "ENTRY_DATE", "TIME_STAMP", "VERSION", "FILE_TYPE"],
    'ID11_Street_Records.csv': ["RECORD_IDENTIFIER", "CHANGE_TYPE", "PRO_ORDER", "USRN", "RECORD_TYPE",
                                "SWA_ORG_REF_NAMING", "STATE", "STATE_DATE", "STREET_SURFACE", "STREET_CLASSIFICATION",
                                "VERSION", "STREET_START_DATE", "STREET_END_DATE", "LAST_UPDATE_DATE",
                                "RECORD_ENTRY_DATE", "STREET_START_X", "STREET_START_Y", "STREET_START_LAT",
                                "STREET_START_LONG", "STREET_END_X", "STREET_END_Y", "STREET_END_LAT",
                                "STREET_END_LONG", "STREET_TOLERANCE"],
    'ID15_StreetDesc_Records.csv': ["RECORD_IDENTIFIER", "CHANGE_TYPE", "PRO_ORDER", "USRN", "STREET_DESCRIPTION",
                                    "LOCALITY_NAME", "TOWN_NAME", "ADMINSTRATIVE_AREA", "LANGUAGE", "START_DATE",
                                    "END_DATE", "LAST_UPDATE_DATE", "ENTRY_DATE"],
    'ID21_BLPU_Records.csv': ["RECORD_IDENTIFIER", "CHANGE_TYPE", "PRO_ORDER", "UPRN", "LOGICAL_STATUS", "BLPU_STATE",
                              "BLPU_STATE_DATE", "PARENT_UPRN", "X_COORDINATE", "Y_COORDINATE", "LATITUDE", "LONGITUDE",
                              "RPC", "LOCAL_CUSTODIAN_CODE", "COUNTRY", "START_DATE", "END_DATE", "LAST_UPDATE_DATE",
                              "ENTRY_DATE", "ADDRESSBASE_POSTAL", "POSTCODE_LOCATOR", "MULTI_OCC_COUNT"],
    'ID23_XREF_Records.csv': ["RECORD_IDENTIFIER", "CHANGE_TYPE", "PRO_ORDER", "UPRN", "XREF_KEY", "CROSS_REFERENCE",
                              "VERSION", "SOURCE", "START_DATE", "END_DATE", "LAST_UPDATE_DATE", "ENTRY_DATE"],
    'ID24_LPI_Records.csv': ["RECORD_IDENTIFIER", "CHANGE_TYPE", "PRO_ORDER", "UPRN", "LPI_KEY", "LANGUAGE",
                             "LOGICAL_STATUS", "START_DATE", "END_DATE", "LAST_UPDATE_DATE", "ENTRY_DATE",
                             "SAO_START_NUMBER", "SAO_START_SUFFIX", "SAO_END_NUMBER", "SAO_END_SUFFIX", "SAO_TEXT",
                             "PAO_START_NUMBER", "PAO_START_SUFFIX", "PAO_END_NUMBER", "PAO_END_SUFFIX", "PAO_TEXT",
                             "USRN", "USRN_MATCH_INDICATOR", "AREA_NAME", "LEVEL", "OFFICIAL_FLAG"],
    'ID28_DPA_Records.csv': ["RECORD_IDENTIFIER", "CHANGE_TYPE", "PRO_ORDER", "UPRN", "UDPRN", "ORGANISATION_NAME",
                             "DEPARTMENT_NAME", "SUB_BUILDING_NAME", "BUILDING_NAME", "BUILDING_NUMBER",
                             "DEPENDENT_THOROUGHFARE", "THOROUGHFARE", "DOUBLE_DEPENDENT_LOCALITY",
                             "DEPENDENT_LOCALITY", "POST_TOWN", "POSTCODE", "POSTCODE_TYPE", "DELIVERY_POINT_SUFFIX",
                             "WELSH_DEPENDENT_THOROUGHFARE", "WELSH_THOROUGHFARE", "WELSH_DOUBLE_DEPENDENT_LOCALITY",
                             "WELSH_DEPENDENT_LOCALITY", "WELSH_POST_TOWN", "PO_BOX_NUMBER", "PROCESS_DATE",
                             "START_DATE", "END_DATE", "LAST_UPDATE_DATE", "ENTRY_DATE"],
    'ID29_Metadata_Records.csv': ["RECORD_IDENTIFIER", "GAZ_NAME", "GAZ_SCOPE", "TER_OF_USE", "LINKED_DATA",
                                  "GAZ_OWNER", "NGAZ_FREQ", "CUSTODIAN_NAME", "CUSTODIAN_UPRN", "LOCAL_CUSTODIAN_CODE",
                                  "CO_ORD_SYSTEM", "CO_ORD_UNIT", "META_DATE", "CLASS_SCHEME", "GAZ_DATE", "LANGUAGE",
                                  "CHARACTER_SET"],
    'ID30_Successor_Records.csv': ["RECORD_IDENTIFIER", "CHANGE_TYPE", "PRO_ORDER", "UPRN", "SUCC_KEY", "START_DATE",
                                   "END_DATE", "LAST_UPDATE_DATE", "ENTRY_DATE", "SUCCESSOR"],
    'ID31_Org_Records.csv': ["RECORD_IDENTIFIER", "CHANGE_TYPE", "PRO_ORDER", "UPRN", "ORG_KEY", "ORGANISATION",
                             "LEGAL_NAME", "START_DATE", "END_DATE", "LAST_UPDATE_DATE", "ENTRY_DATE"],
    'ID32_Class_Records.csv': ["RECORD_IDENTIFIER", "CHANGE_TYPE", "PRO_ORDER", "UPRN", "CLASS_KEY",
                               "CLASSIFICATION_CODE", "CLASS_SCHEME", "SCHEME_VERSION", "START_DATE", "END_DATE",
                               "LAST_UPDATE_DATE", "ENTRY_DATE"],
    'ID99_Trailer_Records.csv': ["RECORD_IDENTIFIER", "NEXT_VOLUME_NUMBER", "RECORD_COUNT", "ENTRY_DATE", "TIME_STAMP"]
}

file_names = fileNameToHeadingsMap.keys()
csv_folder_prefix = 'ID_CSV_'


# Process processed .csv files in the current directory
# Since prcessing happens in parallel batches, the root directory should contain directories names like ID_CSV_* where
# '*' is some unique suffix. Each of these directories will contain the full complement of partial files - see
# `file_names` above. To merge, find all files with the same name across all the ID_CSV_* directories and concat them
# into one file in the root directory.
def mergeCSV(input_directory_path):
    print 'This program will merge extracted CSV files by record identifier into new CSV files'

    for f, h in fileNameToHeadingsMap.items():
        with open(input_directory_path + '/' + f, 'w') as ef:
            ef.write(','.join(h) + '\n')

    for dirname, dirnames, filenames in os.walk(input_directory_path):
        if os.path.basename(dirname).startswith(csv_folder_prefix):
            for csvf in filenames:
                with open(input_directory_path + '/' + os.path.basename(csvf), 'a') as outf:
                    with open(dirname + '/' + csvf, 'r') as inf:
                        outf.write(inf.read())


def main(d):
    mergeCSV(d)


if __name__ == '__main__':
    main(sys.argv[0])
