import os
import sys
import re
import zipfile
import requests
import warnings
import logging
import pandas as pd
import pyarrow.csv as pv
import pyarrow.parquet as pq
import numpy as np
from stat import S_IREAD, S_IRGRP, S_IROTH
import getpass
import pymysql

# Code by Jeff Levy (jlevy@urban.org), 2016-2017


class LoadData():
    """
    This class is inherited by the Data class, and contains the methods related to retrieving data remotely.
    From the web, that includes the raw 990 IRS data, the raw epostcard (990N) IRS data, and the raw BMR IRS
    data.  From NCCS MySQL, it has the methods for nteedocAllEins, lu_fipsmsa, and all of the prior NCCS
    core file releases.
    """

    def get_urls(self):
        """
        Base method for loading the URLs necessary for downloads into memory.

        Main core file URL: https://www.irs.gov/uac/soi-tax-stats-annual-extract-of-tax-exempt-organization-financial-data

        ARGUMENTS
        None

        RETURNS
        None
        """

        main = self.main
        path = main.path

        entries = {'PF': {}, 'EZ': {}, 'Full': {}, 'BMF': {}, 'epostcard': {}}

        entries = self.form_urls(entries, path)
        entries = self.epost_urls(entries, path)
        entries = self.bmf_urls(entries, path)

        self.urls = entries

    def form_urls(self, entries, path):
        """
        Processes the text file in the "settings/urls" folder for EZ, Full and PF download paths.

        ARGUMENTS
        entries (dict) : A dictionary with keys=form and values=URLs or dict of URLs
        path (str) : The base path on the local system

        RETURNS
        entries (dict) : Updated with the core file URLs as an entry.
        """
        main = self.main
        urlregex = re.compile(
            r'(\d{4})\s*=\s*(https?:\/\/.+\.(dat|zip|csv|txt|xlsx))\s*')
        skipline = re.compile(r'^#')
        for form in main.forms:
            with open(os.path.join(path, 'settings', 'urls', form.lower()+'.txt')) as f:
                for line in f:
                    regex_match = urlregex.match(line)
                    skip_match = skipline.match(line)
                    if regex_match and not skip_match:
                        year = int(regex_match.group(1))
                        url = regex_match.group(2)
                        entries[form][year] = url

        print('')
        return entries

    def epost_urls(self, entries, path):
        """
        Processes the text file in the "settings/urls" folder for the epostcard (990N) download path.

        ARGUMENTS
        entries (dict) : A dictionary with keys=form and values=URLs or dict of URLs
        path (str) : The base path on the local system

        RETURNS
        entries (dict) : Updated with the epostcard URLs as an entry.
        """
        epostregex = re.compile(
            r'(epostcard)\s*=\s*(https?:\/\/.+\.(dat|zip|csv|txt|xlsx))\s*')
        skipline = re.compile(r'^#')
        with open(os.path.join(path, 'settings', 'urls', 'epostcard.txt')) as f:
            for line in f:
                regex_match = epostregex.match(line)
                skip_match = skipline.match(line)
                if regex_match and not skip_match:
                    url = regex_match.group(2)
                    entries['epostcard'] = url
        return entries

    def bmf_urls(self, entries, path):
        """
        Processes the text file in the "settings/urls" folder for BMF download path.

        ARGUMENTS
        entries (dict) : A dictionary with keys=form and values=URLs or dict of URLs
        path (str) : The base path on the local system

        RETURNS
        entries (dict) : Updated with the BMF URLs as an entry.
        """
        bmfregex = re.compile(
            r'(region\d)\s*=\s*(https?:\/\/.+\.(dat|zip|csv|txt|xlsx))\s*')
        skipline = re.compile(r'^#')
        with open(os.path.join(path, 'settings', 'urls', 'bmf.txt')) as f:
            for line in f:
                regex_match = bmfregex.match(line)
                skip_match = skipline.match(line)
                if regex_match and not skip_match:
                    url = regex_match.group(2)
                    region = regex_match.group(1)
                    entries['BMF'][region] = url
        return entries

    def download(self):
        """
        Base method for downloading the main core files from the IRS, setting the EIN as the index, and
        updating the SOURCE column with the appropriate file name.

        ARGUMENTS
        None

        RETURNS
        None
        """
        main = self.main
        delim = self.irs_delim
        current_yr = self.core_file_year  # int

        main.logger.info('Beginning any necessary downloads from the IRS.')

        for form in main.forms:
            try:
                url = self.urls[form][current_yr]
                main.logger.info(f'loading {url}')
            except KeyError:
                raise Exception(
                    f'URL not found for core file year {current_yr}, form {form}.  Please check the "urls" folder.')
            main.logger.info(f'downloading {url}')
            file = self.download_file(url)
            _, extension = os.path.splitext(file)
            if extension == '.xlsx':
                main.logger.info(f'loading {file}')
                df = pd.read_excel(file)
                main.logger.info(f'finished loading {file}')
            elif extension == '.csv':
                main.logger.info(f'loading {file}')
                df = pd.read_csv(file, sep=delim, dtype='str',
                                 engine='pyarrow')
                main.logger.info(f'finished loading {file}')
            elif extension == '.parquet':
                main.logger.info(f'loading {file}')
                df = pd.read_parquet(file)
                main.logger.info(f'finished loading {file}')
            elif extension == '.dat':
                with open(file) as f:
                    first_line = '\n'
                    while first_line == '\n' and first_line != '':
                        first_line = f.readline()
                if '|' in first_line:
                    delim = '|'
                elif ',' in first_line:
                    delim = ','
                elif ' ' in first_line:
                    delim = ' '
                else:
                    self.main.logger.info(
                        f'unable to determine delimiter from {first_line}')
                df = pd.read_csv(file, sep=delim)
            else:
                main.logger.info(
                    f'{file} has unsupported file type extension {extension}')

            # Most IRS files have EIN in caps, but at least one (2012 EZ) has it in lowercase
            if 'ein' in df.columns:
                df.rename(columns={'ein': 'EIN'}, inplace=True)

            df.set_index('EIN', inplace=True)

            # adds the source file name as a column
            df['SOURCE'] = url.split('/')[-1]
            self.data_dict[form] = df

        main.logger.info('Downloading complete.\n')

    def sql_auth(self):
        """
        Handles logging into the NCCS MySQL server, including prompting for credentials.

        ARGUMENTS
        None

        RETURNS
        None
        """
        if self.get_from_sql:
            self.main.logger.info(
                'Authenticating connection to MySQL server...')
            un = input('    MySQL user name: ')
            if sys.stdin.isatty():
                # program is being run in an interactive interpreter, and the password echo can't be shut off
                pw = input('    MySQL password: ')
            else:
                # system is running from the command line, and password echo can be off
                pw = getpass.getpass(prompt='    MySQL password: ')

            try:
                self.sql_connection = pymysql.connect(
                    host=self.sql_server_name, db='nccs', user=un, password=pw)
            except pymysql.OperationalError:
                self.main.logger.info(
                    '    failed to connect to server; will try to load from downloads/nccs folder.\n')
                self.sql_connection = None
            else:
                self.main.logger.info(
                    '    login successful, will attempt to retrieve all necessary data from the SQL database.\n')
        else:
            self.main.logger.info(
                'Without logging into NCCS MySQL server, will look for all files in downloads/nccs folder.\n')
            self.sql_connection = None

    def close_sql(self):
        """
        Cleanly shuts down the NCCS MySQL connection.

        ARGUMENTS
        None

        RETURNS
        None
        """
        if self.get_from_sql:
            self.main.logger.info('Cosing MySQL connection.')
            if self.sql_connection:
                self.sql_connection.close()

    def get_sql(self, fname, dbase, cols='*', index_col='EIN', match_dtypes=None, force_sql_cols=False):
        """
        Method for downloading a file, passed as the "fname" argument, from the MySQL connection established
        in the sql_auth method.

        It will first check its own cache to see if it has already downloaded the file and is holding it in
        memory, then it will look in the "downloads/nccs" folder to see if that exact fname has already been
        downloaded.  Only if both of those are false will it connect to MySQL to retrieve the file.

        For users off the Urban campus or without a login to the NCCS MySQL server, having all the necessary
        files as .csv documents in the "downloads/nccs" folder means the program can still build.  See
        "folder instructions.txt" in that folder for more details.

        ARGUMENTS
        cols (str or list): Default '*', used when only a subset of the data should be returned.
        index_col (str): Default 'EIN', specifies the column to use as the index.
        match_dtypes (DataFrame): Default None, if a dataframe is passed it will extract the schema from
                                  it and apply it to the data specified in fname; otherwise it uses the
                                  MySQL defaults.
        force_sql_cols (bool): Default False, If True it will force the columns specified in the cols argument
                               to become a part of the SQL statement; otherwise it downloads * in the SELECT
                               statement and then subsets it later.  This is used, for example, in
                               nteedocAllEINS because the full file is 1.5 gigabytes but only 1/3rd of that is
                               needed.
        RETURNS
        DataFrame
        """
        file_path = os.path.join(self.main.path, self.nccs_download_folder)
        existing_downloads = os.listdir(file_path)
        existing_parquets = [
            f for f in existing_downloads if f.endswith('.parquet')]
        existing_csvs = [
            f for f in existing_downloads if f.endswith('.csv')]

        if fname in self.sql_cache:
            self.main.logger.info(
                'File already cached; trying version in memory.')
            if isinstance(cols, list):
                try:
                    return self.sql_cache[fname][cols]
                except KeyError:
                    self.main.logger.info(
                        '    Specified columns not in memory.')
                    pass  # if the dataframe is cached already but the desired cols are missing, continue with sql loading
            else:
                return self.sql_cache[fname]

        if f'{fname}.parquet' in existing_parquets:
            self.main.logger.info(
                f'{fname}.parquet found in NCCS downloads; using already-downloaded parquet version.')

            if match_dtypes is not None:
                dtype = match_dtypes.dtypes.to_dict()
                dtype['EIN'] = 'str'
            else:
                dtype = 'str'

            df = pd.read_parquet(os.path.join(file_path, fname+'.parquet'))
            if index_col is not None:
                df.set_index(index_col, inplace=True)

            if match_dtypes is None:
                num_cols = [c for c in self.numeric_columns if c in df]
                for col in num_cols:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(
                        0)  # recast the str columns to float64 or int64
                # fill string NA columns with empty strings
                str_cols = df.select_dtypes(
                    include=[np.object_]).columns.values
                df.loc[:, str_cols] = df.loc[:, str_cols].fillna('')

        elif f'{fname}.csv' in existing_csvs:
            self.main.logger.info(
                f'{fname}.csv found in NCCS downloads; using already-downloaded csv version.')

            if match_dtypes is not None:
                dtype = match_dtypes.dtypes.to_dict()
                dtype['EIN'] = 'str'
            else:
                dtype = 'str'

            df = pd.read_csv(os.path.join(file_path, fname+'.csv'), dtype=dtype,
                             engine='pyarrow')  # , low_memory=False , encoding='utf-8')
            if index_col is not None:
                df.set_index(index_col, inplace=True)

            if match_dtypes is None:
                num_cols = [c for c in self.numeric_columns if c in df]
                for col in num_cols:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(
                        0)  # recast the str columns to float64 or int64
                # fill string NA columns with empty strings
                str_cols = df.select_dtypes(
                    include=[np.object_]).columns.values
                df.loc[:, str_cols] = df.loc[:, str_cols].fillna('')

        elif self.sql_connection is not None:
            con = self.sql_connection
            con.select_db(dbase)
            if force_sql_cols:
                sql_cols = ', '.join(cols)
            else:
                sql_cols = '*'
            df = pd.read_sql(
                f'SELECT {sql_cols} FROM {fname}', con=con, index_col=index_col)
            df.columns = [c.upper() for c in df.columns.values]

            if match_dtypes is not None:
                self.main.logger.info(
                    f'    standardizing dtypes for {fname}...')

                def _dtype_matcher(c):
                    if c.name in match_dtypes.columns:
                        desired_type = match_dtypes[c.name].dtype.type
                        if desired_type is np.object_:
                            return c.astype(str)
                        elif desired_type in [np.float64, np.int64, np.float32, np.int32]:
                            return pd.to_numeric(c, errors='coerce').fillna(0)
                        else:
                            # assume strings for anything else (e.g. dates)
                            return c.astype(str)
                            #raise Exception(f'Unknown dtype: {c.name}, {desired_type}')
                    else:
                        return c.astype(str)

                # this is not very efficient, but I haven't found a better way to make sure all dtypes match from SQL
                df = df.apply(_dtype_matcher)

            df.to_csv(os.path.join(file_path, fname+'.csv'),
                      index=df.index.name is not None)
        else:
            raise Exception(
                f'No active connection to NCCS MySQL database, and file not found in downloads/nccs folder: {fname}')

        # save all dataframes loaded from sql in case they are needed later, because sql load times are slow
        self.sql_cache[fname] = df

        if cols == '*':
            return df
        else:
            df_selected = df.loc[:, [c for c in cols if c != 'EIN']]
            # make all column names all upper case
            df_selected.columns = map(str.upper, df_selected.columns)
            return df_selected

    def download_epostcard(self, usecols=[0, 1], names=['EIN', 'EPOSTCARD'], date_col='EPOSTCARD'):
        """
        Method for downloading the epostcard (990N) data from the IRS.

        ARGUMENTS
        usecols (list) : Default [0, 1], this data comes without headers, so the subset needed is given as
                         indexes.
        names (list) : Default ['EIN', 'EPOSTCARD'], provides the header names.  Must be the same dimension
                       as usecols.
        date_col (str) : Default 'EPOSTCARD', specifies the column to be converted to date dtype.

        RETURNS
        DataFrame
        """
        url = self.urls['epostcard']
        # a df of 'EIN', 'YEAR' from the epostcard records
        file = self.download_file(url, force=True)
        delim = self.epostcard_delim
        df = pd.read_csv(file,
                         skip_blank_lines=True,
                         sep=delim,
                         usecols=usecols,
                         names=names,
                         dtype='str')
        df.set_index('EIN', inplace=True)
        df.index = df.index.astype('int64')
        df = df[df[date_col] != '']  # drop null dates
        assert(df.index.is_unique), 'Expected unique EINs in epostcard data.'
        return df

    def download_bmf(self):
        """
        Accesses the stored URLs for the raw BMF files from the IRS, then passes the necessary information
        into the download_file method.

        ARGUMENTS
        None

        RETURNS
        DataFrame
        """
        bmf_data = {}
        delim = self.bmf_delim
        for region in self.urls['BMF'].keys():
            url = self.urls['BMF'][region]
            file = self.download_file(url)
            _, extension = os.path.splitext(file)
            if extension == '.parquet':
                bmf_data[region] = pd.read_parquet(file)
            elif extension == '.csv':
                bmf_data[region] = pd.read_csv(file, sep=delim, dtype='str')
            else:
                self.main.logger(
                    f'{file} has unsupported file extension {extension}')
        df = pd.concat(bmf_data).set_index('EIN')
        assert(df.index.is_unique), 'Expected unique EINs in BMF data.'
        return df

    def download_file(self, url, force=False):
        """
        Method for downloading the specified URL, then unzipping it if necessary.  All newly-downloaded
        files are set to read-only.

        ARGUMENTS
        url (str) : Any valid URL
        force (bool) : Default False, when True it will ignore existing files in the "downloads/IRS" folder,
                       when False it will only download a new version if the file does not already exist.

        RETURNS
        str : Location on local file system of the downloaded (or pre-existing) file.
        """
        main = self.main

        output_path = os.path.join(main.path, self.irs_download_folder)
        # extracts the file name from the end of the url
        fname = url.split('/')[-1]
        # full location of file to write to
        output_file = os.path.join(output_path, fname)

        if main.force_new_download or force or not os.path.exists(output_file):
            r = requests.get(url, headers=self.headers)

            # this catches invalid URLs entered into the url text files: the IRS website returns a
            # page saying "404 error code" but since that page is a valid page, it returns an actual
            # success code of 200.  Simply searching for 'Page Not Found' in the body is very slow
            # when it is an actual download link with a large file, so it first checks the headers
            # to make sure it's not ['Content-Type'] = 'application/zip'
            if 'text/html' in r.headers['Content-Type'] and 'Page Not Found' in r.text:
                raise Exception(
                    f'Warning: the url {url} appears to be invalid.')
            if os.path.exists(output_file):
                os.chmod(output_file, 0o777)
                os.remove(output_file)
            with open(output_file, 'wb') as ofile:
                ofile.write(r.content)
                # sets the download to read-only
                # os.chmod(output_file, S_IREAD | S_IRGRP | S_IROTH)
            main.logger.info(f'File {fname} downloaded.')
        else:
            main.logger.info(
                f'Using existing contents of {fname} in downloads.')

        if fname.endswith('.zip'):
            return self.unzip_file(fname, output_file, output_path)
        else:
            # returns the output file if it's not zipped
            return self.convert_to_parquet(output_file)

    def unzip_file(self, fname, output_file, output_path):
        zip_ref = zipfile.ZipFile(output_file, 'r')
        # unzips into the download path
        zip_ref.extractall(output_path+os.sep)

        # looks at the list of unizpped items, warns if there is more than 1
        unzipped_files = zip_ref.namelist()
        zip_ref.close()  # finished with the zip object
        if len(unzipped_files) != 1:
            self.main.logger.info(
                f'WARNING: More or less than one file in {fname}; system may not be using the right one as data.')

        # sets the unzipped files to read-only
        for nfile in unzipped_files:
            output_file = os.path.join(output_path, nfile)
            # os.chmod(output_file, S_IREAD | S_IRGRP | S_IROTH)
            self.main.logger.info(f'File {nfile} extracted from zip.')

            # returns the contents of a zip file as the output file
            return self.convert_to_parquet(output_file)

    def ismixed(self, a):
        try:
            max(a)
            return False
        except TypeError as e: # we take this to imply mixed type
            msg, fst, and_, snd = str(e).rsplit(' ', 3)
            assert msg=="'>' not supported between instances of"
            assert and_=="and"
            assert fst!=snd
            return True
        except ValueError as e: # catch empty arrays
            assert str(e)=="max() arg is an empty sequence"
            return False

    def convert_to_parquet(self, file_path):
        f_name, f_extension = os.path.splitext(file_path)
        if not os.path.exists(f'{f_name}.parquet'):
            self.main.logger.info(f'converting {file_path} to parquet')
            if f_extension == '.csv':
                table = pv.read_csv(file_path)
                pq.write_table(table, f'{f_name}.parquet')
            elif f_extension == '.xlsx':
                df = pd.read_excel(file_path)
                for c in df.columns:
                    if self.ismixed(df[c]):
                        self.main.logger.info(f'{c} is a mixed type.  converting to str')
                        df[c] = df[c].astype(str)
                df.to_parquet(f'{f_name}.parquet')
            elif f_extension == '.dat':
                with open(file_path) as f:
                    first_line = '\n'
                    while first_line == '\n' and first_line != '':
                        first_line = f.readline()
                if '|' in first_line:
                    delim = '|'
                elif ',' in first_line:
                    delim = ','
                elif ' ' in first_line:
                    delim = ' '
                else:
                    self.main.logger.info(
                        f'unable to determine delimiter from {first_line}')
                df = pd.read_csv(file_path, sep=delim)
                df.to_parquet(f'{f_name}.parquet')
            else:
                self.main.logger.info(
                    f'{file_path} is an unsupported file type')
                return file_path
        return f'{f_name}.parquet'
