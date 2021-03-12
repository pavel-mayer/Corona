#/bin/bash

# user specific settings
# (defaults match pavel's settings, but can be overriden)
if [ -z "$CORONA" ]
then
    CORONA=$HOME/Corona
fi
if [ -z "$GOOGLE_SDK_PATH" ]
then
    GOOGLE_SDK_PATH=/usr/local/Caskroom/google-cloud-sdk/latest/google-cloud-sdk
fi

SHELLNAME=$(basename `echo $SHELL`)

# prepare environment
#cd $CORONA/2020-rki-archive
#git pull
cd $CORONA

mkdir -p ard-data
mkdir -p archive_ard
mkdir -p archive_v2
mkdir -p series
mkdir -p series-enhanced

if [ -e $GOOGLE_SDK_PATH ]
then
    source $GOOGLE_SDK_PATH/path.$SHELLNAME.inc
else
    echo "Could not find Google Cloud sdk path at: $GOOGLE_SDK_PATH"
fi

gsutil rsync -d -r gs://brdata-public-data/rki-corona-archiv/ ard-data

cd $CORONA/ard-data/2_parsed
python $CORONA/convertARD.py -d $CORONA/archive_ard/ *.xz

cd $CORONA/archive_ard/
python $CORONA/unify.py -d $CORONA/archive_v2 $CORONA/archive_ard/NPGEO-RKI-*.csv

cd $CORONA/
python $CORONA/database.py -d $CORONA/series
#or:
#python $CORONA/database.py --agegroups -d $CORONA/series

cd $CORONA/
python $CORONA/enhance.py -d $CORONA/series-enhanced series/series-*.csv

cd $CORONA/
python $CORONA/gather-results.py -o all-series.csv series-enhanced/enhanced-series-*.csv
