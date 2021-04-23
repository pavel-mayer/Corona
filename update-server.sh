#scp -r -v app.py cov_dates.py pm_util.py assets full-latest.csv data.csv data-cached.feather pavel@westphal.uberspace.de:/home/pavel/covid/

scp -C all-series.csv pavel@westphal.uberspace.de:/home/pavel/covid/;date;ssh pavel@westphal.uberspace.de 'supervisorctl restart flask';date


scp -C all-series-agegroups-gender.csv  root@hn-hetzner-02.tognos.com:/home/shared
ssh root@hn-hetzner-02.tognos.com 'lxc file push /home/shared/all-series-agegroups-gender.csv app-host/home/appuser/Corona/'
ssh root@hn-hetzner-02.tognos.com 'lxc exec app-host "chown -R appuser.1023 /home/appuser/Corona/*"'
ssh root@hn-hetzner-02.tognos.com 'lxc exec app-host /usr/bin/systemctl restart app'
