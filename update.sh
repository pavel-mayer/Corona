#/bin/bash
CORONA=/Users/pavel/Corona
cd $CORONA/2020-rki-archive
git pull
cd $CORONA/2020-rki-archive/data/2_parsed
python $CORONA/convertARD.py -d $CORONA/archive_ard/ *.bz2

cd $CORONA/archive_ard/
python $CORONA/unify.py -d $CORONA/archive_v2 $CORONA/archive_ard/NPGEO-RKI-*.csv

cd $CORONA/
python $CORONA/database.py -d $CORONA/series