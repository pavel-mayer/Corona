#/bin/bash
CORONA=$HOME/Corona
#cd $CORONA/2020-rki-archive
#git pull
cd $CORONA

#pip install pipreqs
#pipreqs
#pip install -r requirements.txt

mkdir -p ard-data
mkdir -p archive_ard
mkdir -p archive_v2
mkdir -p series
mkdir -p series-enhanced

source /usr/local/Caskroom/google-cloud-sdk/latest/google-cloud-sdk/path.bash.inc
gsutil rsync -d -r gs://brdata-public-data/rki-corona-archiv/ ard-data

cd $CORONA/ard-data/2_parsed
python $CORONA/convertARD.py -d $CORONA/archive_ard/ *.xz

cd $CORONA/archive_ard/
python $CORONA/unify.py -d $CORONA/archive_v2 $CORONA/archive_ard/NPGEO-RKI-*.csv

cd $CORONA/
python $CORONA/database.py -d $CORONA/series

#or with age groups and gender, first run takes 10-20 hours:
#python $CORONA/database.py --agegroups --gender -d $CORONA/series

#or incremental update, creates updated series in -d <dir>, runs 50-100 times faster:
#python $CORONA/database.py --agegroups --gender -i $CORONA/series -d $CORONA/series-updated

cd $CORONA/
python $CORONA/enhance.py -d $CORONA/series-enhanced series/series-*.csv

cd $CORONA/
python $CORONA/gather-results.py -o all-series.csv series-enhanced/enhanced-series-*.csv
