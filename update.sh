#/bin/bash
CORONA=/Users/pavel/Corona
#cd $CORONA/2020-rki-archive
#git pull
cd $CORONA
source /usr/local/Caskroom/google-cloud-sdk/latest/google-cloud-sdk/path.bash.inc
gsutil rsync -d -r gs://brdata-public-data/rki-corona-archiv/ ard-data

cd $CORONA/ard-data/2_parsed
python $CORONA/convertARD.py -d $CORONA/archive_ard/ *.xz

cd $CORONA/archive_ard/
python $CORONA/unify.py -d $CORONA/archive_v2 $CORONA/archive_ard/NPGEO-RKI-*.csv

cd $CORONA/
python $CORONA/database.py --agegroups -d $CORONA/series
