ID10_Header_Records = 'ID10_Header_Records.csv'
ID11_Street_Records = 'ID11_Street_Records.csv'
ID15_StreetDesc_Records = 'ID15_StreetDesc_Records.csv'
ID21_BLPU_Records = 'ID21_BLPU_Records.csv'
ID23_XREF_Records = 'ID23_XREF_Records.csv'
ID24_LPI_Records = 'ID24_LPI_Records.csv'
ID28_DPA_Records = 'ID28_DPA_Records.csv'
ID29_Metadata_Records = 'ID29_Metadata_Records.csv'
ID30_Successor_Records = 'ID30_Successor_Records.csv'
ID31_Org_Records = 'ID31_Org_Records.csv'
ID32_Class_Records = 'ID32_Class_Records.csv'
ID99_Trailer_Records = 'ID99_Trailer_Records.csv'

fileNameToHeadingsMap = {
    '10': {'file_name': ID10_Header_Records,
         'headers': ["RECORD_IDENTIFIER", "CUSTODIAN_NAME", "LOCAL_CUSTODIAN_NAME", "PROCESS_DATE",
                     "VOLUME_NUMBER", "ENTRY_DATE", "TIME_STAMP", "VERSION", "FILE_TYPE"]},
    '11': {'file_name': ID11_Street_Records,
         'headers': ["RECORD_IDENTIFIER", "CHANGE_TYPE", "PRO_ORDER", "USRN", "RECORD_TYPE",
                     "SWA_ORG_REF_NAMING", "STATE", "STATE_DATE", "STREET_SURFACE",
                     "STREET_CLASSIFICATION",
                     "VERSION", "STREET_START_DATE", "STREET_END_DATE", "LAST_UPDATE_DATE",
                     "RECORD_ENTRY_DATE", "STREET_START_X", "STREET_START_Y", "STREET_START_LAT",
                     "STREET_START_LONG", "STREET_END_X", "STREET_END_Y", "STREET_END_LAT",
                     "STREET_END_LONG", "STREET_TOLERANCE"]},
    '15': {'file_name': ID15_StreetDesc_Records,
         'headers': ["RECORD_IDENTIFIER", "CHANGE_TYPE", "PRO_ORDER", "USRN", "STREET_DESCRIPTION",
                     "LOCALITY_NAME", "TOWN_NAME", "ADMINSTRATIVE_AREA", "LANGUAGE", "START_DATE",
                     "END_DATE", "LAST_UPDATE_DATE", "ENTRY_DATE"]},
    '21': {'file_name': ID21_BLPU_Records,
         'headers': ["RECORD_IDENTIFIER", "CHANGE_TYPE", "PRO_ORDER", "UPRN", "LOGICAL_STATUS",
                     "BLPU_STATE",
                     "BLPU_STATE_DATE", "PARENT_UPRN", "X_COORDINATE", "Y_COORDINATE", "LATITUDE",
                     "LONGITUDE",
                     "RPC", "LOCAL_CUSTODIAN_CODE", "COUNTRY", "START_DATE", "END_DATE",
                     "LAST_UPDATE_DATE",
                     "ENTRY_DATE", "ADDRESSBASE_POSTAL", "POSTCODE_LOCATOR", "MULTI_OCC_COUNT"]},
    '23': {'file_name': ID23_XREF_Records,
         'headers': ["RECORD_IDENTIFIER", "CHANGE_TYPE", "PRO_ORDER", "UPRN", "XREF_KEY",
                     "CROSS_REFERENCE",
                     "VERSION", "SOURCE", "START_DATE", "END_DATE", "LAST_UPDATE_DATE", "ENTRY_DATE"]},
    '24': {'file_name': ID24_LPI_Records,
         'headers': ["RECORD_IDENTIFIER", "CHANGE_TYPE", "PRO_ORDER", "UPRN", "LPI_KEY", "LANGUAGE",
                     "LOGICAL_STATUS", "START_DATE", "END_DATE", "LAST_UPDATE_DATE", "ENTRY_DATE",
                     "SAO_START_NUMBER", "SAO_START_SUFFIX", "SAO_END_NUMBER", "SAO_END_SUFFIX", "SAO_TEXT",
                     "PAO_START_NUMBER", "PAO_START_SUFFIX", "PAO_END_NUMBER", "PAO_END_SUFFIX", "PAO_TEXT",
                     "USRN", "USRN_MATCH_INDICATOR", "AREA_NAME", "LEVEL", "OFFICIAL_FLAG"]},
    '28': {'file_name': ID28_DPA_Records,
         'headers': ["RECORD_IDENTIFIER", "CHANGE_TYPE", "PRO_ORDER", "UPRN", "UDPRN", "ORGANISATION_NAME",
                     "DEPARTMENT_NAME", "SUB_BUILDING_NAME", "BUILDING_NAME", "BUILDING_NUMBER",
                     "DEPENDENT_THOROUGHFARE", "THOROUGHFARE", "DOUBLE_DEPENDENT_LOCALITY",
                     "DEPENDENT_LOCALITY", "POST_TOWN", "POSTCODE", "POSTCODE_TYPE",
                     "DELIVERY_POINT_SUFFIX",
                     "WELSH_DEPENDENT_THOROUGHFARE", "WELSH_THOROUGHFARE",
                     "WELSH_DOUBLE_DEPENDENT_LOCALITY",
                     "WELSH_DEPENDENT_LOCALITY", "WELSH_POST_TOWN", "PO_BOX_NUMBER", "PROCESS_DATE",
                     "START_DATE", "END_DATE", "LAST_UPDATE_DATE", "ENTRY_DATE"]},
    '29': {'file_name': ID29_Metadata_Records,
         'headers': ["RECORD_IDENTIFIER", "GAZ_NAME", "GAZ_SCOPE", "TER_OF_USE", "LINKED_DATA",
                     "GAZ_OWNER", "NGAZ_FREQ", "CUSTODIAN_NAME", "CUSTODIAN_UPRN",
                     "LOCAL_CUSTODIAN_CODE",
                     "CO_ORD_SYSTEM", "CO_ORD_UNIT", "META_DATE", "CLASS_SCHEME", "GAZ_DATE",
                     "LANGUAGE",
                     "CHARACTER_SET"]},
    '30': {'file_name': ID30_Successor_Records,
         'headers': ["RECORD_IDENTIFIER", "CHANGE_TYPE", "PRO_ORDER", "UPRN", "SUCC_KEY",
                     "START_DATE",
                     "END_DATE", "LAST_UPDATE_DATE", "ENTRY_DATE", "SUCCESSOR"]},
    '31': {'file_name': ID31_Org_Records,
         'headers': ["RECORD_IDENTIFIER", "CHANGE_TYPE", "PRO_ORDER", "UPRN", "ORG_KEY", "ORGANISATION",
                     "LEGAL_NAME", "START_DATE", "END_DATE", "LAST_UPDATE_DATE", "ENTRY_DATE"]},
    '32': {'file_name': ID32_Class_Records,
         'headers': ["RECORD_IDENTIFIER", "CHANGE_TYPE", "PRO_ORDER", "UPRN", "CLASS_KEY",
                     "CLASSIFICATION_CODE", "CLASS_SCHEME", "SCHEME_VERSION", "START_DATE", "END_DATE",
                     "LAST_UPDATE_DATE", "ENTRY_DATE"]},
    '99': {'file_name': ID99_Trailer_Records,
         'headers': ["RECORD_IDENTIFIER", "NEXT_VOLUME_NUMBER", "RECORD_COUNT", "ENTRY_DATE",
                     "TIME_STAMP"]}
}
