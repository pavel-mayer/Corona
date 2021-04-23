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
mkdir -p archive
mkdir -p dumps
mkdir -p series-agegroups-gender
mkdir -p series-updated-agegroups-gender
mkdir -p series-enhanced-agegroups-gender
mkdir -p tmp

source /usr/local/Caskroom/google-cloud-sdk/latest/google-cloud-sdk/path.bash.inc
gsutil rsync -d -r gs://brdata-public-data/rki-corona-archiv/ ard-data

#cd $CORONA/ard-data/2_parsed
#python $CORONA/convertARD.py -d $CORONA/archive_ard/ *.xz
python $CORONA/convertARD.py -d $CORONA/archive_ard/ $CORONA/ard-data/2_parsed/*.xz

#cd $CORONA
##python $CORONA/unify.py -d $CORONA/archive_v2 $CORONA/archive_ard/NPGEO-RKI-*.csv
python $CORONA/unify.py --resume -d $CORONA/archive_v2 $CORONA/archive_csv/NPGEO-RKI-*.csv $CORONA/archive_ard/NPGEO-RKI-*.csv

cd $CORONA/
#python $CORONA/database.py -d $CORONA/series
#python $CORONA/enhance.py -d $CORONA/series-enhanced series/series-*.csv
#or
python $CORONA/database.py -i $CORONA/series -d $CORONA/series-updated
#python $CORONA/database.py -d $CORONA/series -i $CORONA/series-updated
python $CORONA/enhance.py -d $CORONA/series-enhanced series-updated/series-*.csv
#python $CORONA/enhance.py -d $CORONA/series-enhanced series/series-*.csv

python $CORONA/gather-results.py -o all-series.csv series-enhanced/enhanced-series-*.csv

exit
######################################################################

#or with age groups and gender, first run takes 10-20 hours:
cd $CORONA/
python $CORONA/database.py --agegroups --gender -d $CORONA/series-agegroups-gender
python $CORONA/enhance.py -d $CORONA/series-enhanced-agegroups-gender series-agegroups-gender/series-*.csv
python $CORONA/gather-results.py -o all-series-agegroups-gender.csv series-enhanced-agegroups-gender/enhanced-series*.csv

######################################################################
#or incremental update, creates updated series in -d <dir>, runs 50-100 times faster:
python $CORONA/database.py --agegroups --gender -i $CORONA/series-agegroups-gender -d $CORONA/series-updated-agegroups-gender
#python $CORONA/database.py --agegroups --gender -d $CORONA/series-agegroups-gender -i $CORONA/series-updated-agegroups-gender
python $CORONA/enhance.py -d $CORONA/series-enhanced-agegroups-gender $CORONA/series-updated-agegroups-gender/series-*.csv
#python $CORONA/enhance.py -d $CORONA/series-enhanced-agegroups-gender $CORONA/series-agegroups-gender/series-*.csv
python $CORONA/gather-results.py -o all-series-agegroups-gender.csv series-enhanced-agegroups-gender/enhanced-series-*.csv

# Processing took 32.00 seconds, or 0.53 minutes or 0.01 hours
# Processing took 121.80 seconds, or 2.03 minutes or 0.03 hours