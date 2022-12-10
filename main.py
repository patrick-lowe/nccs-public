import build_nccs
import os
import sys

path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(path)


# Code by Jeff Levy (jlevy@urban.org), 2016-2017

###############################################################################
## Optional Paramters                                                        ##
###############################################################################
# the current NCCS release year (which is the IRS file year - 1, because the IRS releases 2015 returns under the label "2016", for example)
current_yr = 2020
# if False, allows system to use already-downloaded IRS data; if True, always re-downloads it
force_new_download = False
# if 'EZ' and 'Full' are in the list, it will produce the CO and PC files; if 'PF' is in the list it will produce the PF file
forms = ['EZ', 'Full', 'PF']
backfill = 3  # how many years into previous releases does it go to find EINs to backfill; supports 0-3
# if False, it will skip the validation steps
do_validation = {'CO': True, 'PC': True, 'PF': True}
partial_validation = True  # if True, will consider a form as passing validation if there are ANY fixes uploaded for that form, even if some EINs still fail validation
# the amount column validation can be off by before it flags it as a validation failure
tolerance = 1000
clear_old = True  # drops the downloaded source data after backfilling; True will reduce system memory usage significantly, False will allow the user to query the old data after the program runs
# if True, will attempt to get the fipsmsa and ntee files from the NCCS data store (un and pw required)
get_from_sql = True
output_full = True  # if True, will also output the CO_full and PC_full files at the end
###############################################################################


if __name__ == '__main__':
    nccs = build_nccs.BuildNCCS(path,
                                current_yr=current_yr,
                                force_new_download=force_new_download,
                                forms=forms,
                                backfill=backfill,
                                do_validation=do_validation,
                                tolerance=tolerance,
                                partial_validation=partial_validation,
                                get_from_sql=get_from_sql,
                                output_full=output_full
                                )

    nccs.data.get_urls()
    nccs.data.sql_auth()
    nccs.data.download()
    nccs.data.apply_crosswalk()
    nccs.data.drop_missing()
    nccs.data.drop_on_values()
    nccs.data.init_final()
    nccs.data.ntee()
    nccs.data.fipsmsa()
    nccs.data.epostcard()
    nccs.data.bmf()
    nccs.data.make_numeric()

    nccs.process.calculate_columns()
    nccs.process.manual_fixes()

    nccs.write.build_output()
    nccs.data.prior_year()
    nccs.data.backfill()
    nccs.data.close_sql()
    nccs.write.handle_duplicates()

    nccs.validate.validate_columns()
    nccs.validate.extract()
    nccs.validate.to_file()

    nccs.data.check_columns()
    nccs.write.to_file()

    nccs.end()
