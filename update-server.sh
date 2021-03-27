#scp -r -v app.py cov_dates.py pm_util.py assets full-latest.csv data.csv data-cached.feather pavel@westphal.uberspace.de:/home/pavel/covid/

scp all-series.csv pavel@westphal.uberspace.de:/home/pavel/covid/;date
ssh pavel@westphal.uberspace.de 'supervisorctl restart flask';date