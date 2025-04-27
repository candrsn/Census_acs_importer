#!/usr/bin/python3


import sys
import os
import shutil
from zipfile import ZipFile
import requests
import sqlite3
import zipfile
import logging
import pandas
import glob
import numpy
import xlrd


logger = logging.getLogger(__name__)
DBCORE = "acs5yr_2019_master.sqlite"
DBTYPE = "sqlite"
REFDATA = "refdata.sqlite"


def setup():
    url = "https://www2.census.gov/geo/docs/reference/state.txt"
    dest = "refdata/" + os.path.basename(url)
    if not os.path.exists(dest):
        req = requests.get(url)
        if req.status_code in [200,201,202]:
            with open(dest, "w") as f:
                f.write(req.text)

    if not os.path.exists(REFDATA):
        df =  pandas.read_csv(dest, delimiter="|", header=0)
        refdata = sqlite3.connect('refdata.sqlite')
        df.to_sql('states', refdata)
        refdata.close()


def get_url_to_file(rurl, rfile):
    headers = {'user-agent': 'myx-app/0.5.1', 'Request-Encoding': 'en'}
    if not os.path.exists(rfile):
        r = requests.get(rurl, headers=headers)
        if r.status_code in [200, 201, 202]:
            with open(rfile, 'wb') as fd:
                for chunk in r.iter_content(chunk_size=16384):
                    fd.write(chunk)
        else:
            print(f"{rurl} not found", os.error())


def get_lookup_tables(acs_yr):

    urls = [f"https://www2.census.gov/programs-surveys/acs/summary_file/{acs_yr}/data/{acs_yr}_5yr_Summary_FileTemplates.zip",
            f"https://www2.census.gov/programs-surveys/acs/summary_file/{acs_yr}/data/{acs_yr}_5yr_Summary_FileTemplates.zip",  
            f"https://www2.census.gov/programs-surveys/acs/{acs_yr}/summaryfile/ACS{acs_yr}_5-Year_TableShells.xls",
            f"https://www2.census.gov/programs-surveys/acs/summary_file/{acs_yr}/table-based-SF/documentation/ACS{acs_yr}5YR_Table_Shells.txt",
            f"https://www2.census.gov/programs-surveys/acs/summary_file/{acs_yr}/table-based-SF/documentation/ACS{acs_yr}5YR_Table_Shells.csv",
            f"https://www2.census.gov/acs{acs_yr}_5yr/summaryfile/Sequence_Number_and_Table_Number_Lookup.xls"]

    for url in urls:
        get_url_to_file(url, f"shells/{acs_yr}/{os.path.basename(url)}")


def read_table_pandas(zF, zPart, DB):
    import tarfile
    import pandas as pd
    
    flds="*"

    db = sqlite3.connect("test.sqlite")

    with tarfile.open("sample.tar.gz", "r:*") as tar:
        csv_path = tar.getnames()[0]
        df = pd.read_csv(tar.extractfile(csv_path), header=None, names=flds, sep=",", index_col=False, engine='pyarrow')
        df.to_sql("csv_path", con=db, if_exists="append", method="multi", chucksize=50000)


def build_info(acs_yr):

    files = [f"shells/{acs_yr}/ACS_5yr_Seq_Table_Number_Lookup.txt",
             f"shells/{acs_yr}/ACS{acs_yr}_Sequence_Number_and_Table_Number_Lookup.xls",
             f"shells/{acs_yr}/ACS{acs_yr}5YR_Table_Shells.txt",
             f"shells/{acs_yr}/ACS{acs_yr}5YR_Table_Shells.csv",
             f"shells/{acs_yr}/ACS{acs_yr}_Table_Shells.csv",
             f"shells/{acs_yr}/{acs_yr}_5yr_Summary_FileTemplates.zip"] 

    tbl_data = None

    # treat the Zip file as a directory as well
    for itm in files:
        if os.path.exists(itm):
            try:
                tbl_data = parse_sequence_and_table_lookup(itm)
                break
            except Exception as e:
                logger.warning(f"failed to comprehend {itm} \n\n {e}")
                sys.exit()

    return tbl_data


def build_tables():
    pass


def build_geography_lookup():
    pass


def build_geography_detail():
    pass


def build_sequence_tables(tbl_data, dbname):
    db = sqlite3.connect(dbname)
    cur = db.cursor()

    for tbl in set([t[0] for t in tbl_data]):
        ddl = ddl_sequence_tables([d for d in tbl_data if d[0] == tbl])
        cur.execute(ddl[f"SEQ{tbl:04d}"])
        assert cur.fetchall() is not None, "failed to create table SEQ{tbl:04d}"

    cur.close()
    db.commit()


def ddl_sequence_tables(seq_data):
    cmd = {}
    seq = 0
    tdef = ""
    for item in seq_data:
        if not item[0] == seq:
            logger.debug(f"start new seq {item[0]}")
            # emit the prior table definition, if any
            if tdef > "":
                cmd [f"SEQ{seq:04d}"] = tdef[:-1] + "\n" + ");\n\n"
            seq = item[0]
            tdef = f"CREATE TABLE SEQ{item[0]:04d} ("
        
        if item[4] >= '':
            tdef += "\n" + item[2] + " " + item[4] + ","
        else:
            tdef += "\n" + item[2] + " NUMERIC,"

    cmd [f"SEQ{seq:04d}"] = tdef[:-1] + "\n" + ");\n\n"
    #logger.info(f"{cmd}")
    return cmd


def ddl_ptables(tbl_data, tbl):
    cmd = {}

    tdef = f"CREATE TABLE IF NOT EXISTS {tbl} ("
    for item in tbl_data:
        if item[3] < 7:
            tdef += "\n" + item[2] + " TEXT,"
        else:
            tdef += "\n" + item[2] + " NUMERIC,"

    # strip he last comma fro the field list
    cmd [f"{tbl}"] = tdef[:-1] + "\n" + ");\n\n"
    #logger.info(f"{cmd}")
    return cmd


def get_ptable_names(tbl_data):
    tbls = [d for d in set([t[1] for t in tbl_data if not t[1].startswith("SEQ")] )]
    return tbls


def get_ptable_fields_seq(tbl_data, tbl):
    ptable_flds = {}

    # the return is in the form of
    # remove duplicate field definitions, the field definition XLS has issues
    # not using SET() as it can change object order 
    for fld in [d for d in tbl_data if d[1] == tbl]:
        if not fld[2] in ptable_flds:
            ptable_flds[fld[2]] = fld

    ptable_flds = list(ptable_flds.values())

    #seq file, tbl name, cols, col_order
    flds = [[1, tbl, p[0], p[1]] for p in base_fields()] + ptable_flds
    
    return flds


def get_ptable_fields_direct(tbl_data, tbl):
    ptable_flds = {}

    # the return is in the form of
    # remove duplicate field definitions, the field definition XLS has issues
    # not using SET() as it can change object order 
    for fld in [d for d in tbl_data if d[1] == tbl]:
        if not fld[2] in ptable_flds:
            ptable_flds[fld[2]] = fld

    ptable_flds = list(ptable_flds.values())

    #seq file, tbl name, cols, col_order
    # no need to add base_fields
    #flds = [[1, tbl, p[0], p[1]] for p in base_fields()] + ptable_flds
    flds = ptable_flds

    return flds


def build_ptables_seq(tbl_data, dbname):
    db = sqlite3.connect(dbname)
    cur = db.cursor()

    for tbl in get_ptable_names(tbl_data):
        logger.info(f"Create table {tbl}")
        #ddl = ddl_ptables(get_ptable_fields_direct(tbl_data, tbl), tbl)
        ddl = ddl_ptables(get_ptable_fields_seq(tbl_data, tbl), tbl)
        cur.execute(ddl[f"{tbl}"])
        assert cur.fetchall() is not None, "failed to create table {tbl}"

    cur.close()
    db.commit()


def build_ptables_direct(tbl_data, dbname):
    db = sqlite3.connect(dbname)
    cur = db.cursor()

    for tbl in sorted(set([ t[1] for t in tbl_data])):
        logger.info(f"Create table {tbl}")
        ddl = ddl_ptables(get_ptable_fields_direct(tbl_data, tbl), tbl)
        cur.execute(ddl[f"{tbl}"])
        assert cur.fetchall() is not None, "failed to create table {tbl}"

    cur.close()
    db.commit()


def base_fields():
    base_cols = [
        ["FILEID", 1, "TEXT"],
        ["FILETYPE", 2, "TEXT"],
        ["STUSAB", 3, "TEXT"],
        ["CHARITER", 4, "TEXT"],
        ["SEQUENCE",  5, "TEXT"],
        ["LOGRECNO", 6, "TEXT"]
        ]

    return base_cols


def parse_sequence_and_table_lookup(datfile):

    if datfile[-4:] == ".xls":
        tbl_info = parse_sequence_and_table_lookup_xls(datfile)
    elif datfile[-4:] in (".csv", ".txt"):
        tbl_info = parse_sequence_and_table_lookup_txt(datfile)
    elif datfile[-4:] in (".zip"):
        for fitm in zipfile(datfile):
            fitm.open()
            tbl_info = parse_sequence_and_table_lookup_txt(datfile)

    return tbl_info


def parse_sequence_and_table_lookup_txt(txtfile):
    base_cols = base_fields()
    num_base_cols = len(base_cols)

    seq_file = -1
    col_iter = 0
    last_table = ""
    cols = []

    ws = pandas.read_csv(txtfile, encoding='Latin1')

    for irow in range(0, ws.shape[0]):
        row_data = ws.iloc[irow]
        tbl_name = row_data.iloc[0]

        if row_data.iloc[0] == " ":
            # skip non column data
            continue
        elif row_data.iloc[1] is numpy.nan or row_data.iloc[1] in ("", ' ') or row_data.iloc[1].endswith('.5'):
            #Grab numeric data types
            continue

        else:
            if row_data.iloc[1] == '1':
                seq_file = 0
                cols += [[seq_file, tbl_name] + b for b in base_cols]
                col_iter = len(base_cols)

            cols.append([seq_file, row_data.iloc[0], row_data.iloc[2], 
                col_iter + int(row_data.iloc[1]), "NUMERIC"])

    return cols


def parse_sequence_and_table_lookup_xls(xlfile):
    import xlrd

    base_cols = base_fields()
    num_base_cols = len(base_cols)

    seq_file = -1
    last_table = ""
    cols = []

    wb = xlrd.open_workbook(filename=xlfile)
    ws = wb.sheet_by_index(0)
    for irow in range(1,ws.nrows):
        row_data = ws.row(irow)

        # ignore comments 
        if type(row_data[2].value) is float and type(row_data[3].value) is float:
            # when we see a new sequence file number along with a column name
            if row_data[3].value >= 1.0 and not row_data[2].value == seq_file:
                seq_file = int(row_data[2].value)
                cols += [[seq_file, f"SEQ{seq_file:04d}"] + b for b in base_cols]

                # if we see a ptable split into 2 Seq files, restarting at 1
                if row_data[1].value == last_table and row_data[3].value == 1:
                    # account for the already seen columns
                    col_iter = num_base_cols + len([d for d in cols if d[1] == last_table])
                    logger.info(f"continue table across Seq files at {col_iter}")
                else:
                    # do account for base columns
                    col_iter = num_base_cols
                last_table = row_data[1].value

            # ignore fractional columns
            if int(row_data[3].value) == row_data[3].value:
                cols.append([seq_file, row_data[1].value, f"{row_data[1].value}_{row_data[3].value:04g}", 
                        col_iter + row_data[3].value, "NUMERIC"])
    return cols


def lookup_statename(state):
    db = sqlite3.connect(REFDATA)

    cur = db.cursor().execute(f"SELECT replace(state_name,' ', '') FROM states WHERE STUSAB = '{state.upper()}'")
    stname = cur.fetchone()[0]
    return stname


def available_states():
    db = sqlite3.connect(REFDATA)

    cur = db.cursor().execute(f"SELECT lower(stusab) FROM states order by stusab")
    states = cur.fetchall()
    states = [s[0] for s in states]
    return states


def extract_files(state="co", yr="", tbl_data=[], dbname="main.sqlite", dbtype="sqlite"):
    stateName = lookup_statename(state)
    rtn = None

    for itm in glob.glob(f"rawdata/{yr}/{stateName}*.zip"):
        rtn = extract_files_zip(itm, state, yr, tbl_data, dbname, dbtype)

    return rtn


def extract_files_zip(archivefile, state="co", yr="", tbl_data=[], dbname="main.sqlite", dbtype=None):
    if not os.path.exists(archivefile):
        return "archive not found"

    basezip = zipfile.ZipFile(archivefile, 'r')

    # import from the Seq Files
    for fseq in basezip.namelist():
        if fseq[0] in ('e','m'):
            seq = int(fseq[9:-7])
        else:
            seq = "geo"
            continue

        fclass = fseq[0]
        with basezip.open(fseq, "r") as fh:
            if dbtype == "sqlite":
                extract_file_sqlite(seq, tbl_data, fh, dbname)
            elif dbtype == "PQ":
                extract_file_pq(seq, tbl_data, fh, dbname)

    return "imported"


def extract_file_pq(seq, tbl_data, fh, dbname, colnames=None):
    db = sqlite3.connect(dbname, timeout=15)
    cur = db.cursor()
    cols = [d[2] for d in tbl_data if d[0] == seq]
    tbl = f"SEQ{seq:04d}"
    rcnt = 0

    fpos = ",".join(['?' for d in cols])
    fcols = ",".join([d for d in cols])
    data = []
    for row in fh:
        row = row.decode('iso_8859_1')
        datarow = row.split(',')
        for itm in range(6):
            datarow[itm] = str(datarow[itm])
    
        # NULLS are encoded as "." in the text files
        for itm in range(6, len(datarow)):
            if datarow[itm] == ".":
                datarow[itm] = None

        data.append(datarow)
        if len(data) > 5000:
            logging.debug(f"insert {len(data)} rows of data info {tbl}")
            stmt = f"INSERT INTO {tbl} ({fcols}) VALUES ({fpos})"
            cur.executemany(stmt, data)
            rcnt += len(data)
            data = []

    if len(data) > 0:
        logging.debug(f"insert {len(data)} rows of data info {tbl}")
        stmt = f"INSERT INTO {tbl} ({fcols}) VALUES ({fpos})"
        cur.executemany(stmt, data)
        rcnt += len(data)

    assert cur.fetchall() is not None, "Failed to add data to {tbl}"
    db.commit()
    cur.close()

    logger.info(f"fininshed import of {rcnt} records into {tbl} in {dbname}")


def extract_file_sqlite(seq, tbl_data, fh, dbname, colnames=None):
    db = sqlite3.connect(dbname, timeout=15)
    cur = db.cursor()
    tbl = tbl_data[1][1]
    cols = [d[2] for d in tbl_data if d[1] == tbl]
    rcnt = 0

    fpos = ",".join(['?' for d in cols])
    fcols = ",".join([d for d in cols])
    data = []
    for row in fh:
        row = row.decode('iso_8859_1')
        datarow = row.split(',')
        for itm in range(6):
            datarow[itm] = str(datarow[itm])
    
        # NULLS are encoded as "." in the text files
        for itm in range(6, len(datarow)):
            if datarow[itm] == ".":
                datarow[itm] = None

        data.append(datarow)
        if len(data) > 5000:
            logging.debug(f"insert {len(data)} rows of data info {tbl}")
            stmt = f"INSERT INTO {tbl} ({fcols}) VALUES ({fpos})"
            cur.executemany(stmt, data)
            rcnt += len(data)
            data = []

    if len(data) > 0:
        logging.debug(f"insert {len(data)} rows of data info {tbl}")
        stmt = f"INSERT INTO {tbl} ({fcols}) VALUES ({fpos})"
        cur.executemany(stmt, data)
        rcnt += len(data)

    assert cur.fetchall() is not None, "Failed to add data to {tbl}"
    db.commit()
    cur.close()

    logger.info(f"fininshed import of {rcnt} records into {tbl} in {dbname}")


def import_state(state, yr, tbl_data, dbcore=DBCORE, dbtype=DBTYPE, refresh=False):

    dbname = f"data/{yr}/acs5yr_{yr}_{state}.sqlite"
    if os.path.exists(dbname):
        if refresh == True:
            os.remove(dbname)
        else:
            # skip reload unless refresh is True
            logger.info(f"skipping state {state} {yr} as it is already available, and Refresh is not set")
            return

    if not os.path.exists(dbname):
        #build_sequence_tables(tbl_data, dbcore)
        # build_ptables_seq(tbl_data, dbname)
        build_ptables_direct(tbl_data, dbname)     

    result = extract_files(state, yr, tbl_data, dbname, dbtype=dbtype)

    return result


def main(args=[]):
        
    if "--target_year" in args:
        target_year = args[args.index("--target_year") + 1]
    else:
        target_year = "2019"

    if "--dbtype" in args:
        dbtype = args[args.index("--dbtype") + 1]
    else:
        dbtype = "2019"

    setup()

    # get_datatables(target_data)
    #get_lookup_tables(target_year)

    tbl_info = build_info(target_year)
    if tbl_info is None:
        sys.exit("no config info")

    # states = ['al','ga','ca','ny','ri','wy','hi','co']
    states = available_states()

    for st in states:
        import_state(st, target_year, tbl_info, dbtype=dbtype)

    logger.info("All Done")


if __name__ == "__main__":

    logging.basicConfig(level=logging.DEBUG)

    main(sys.argv)