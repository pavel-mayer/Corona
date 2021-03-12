# Corona
Code für Prozessierung der Corona-Daten in Deutschland.

## How to
Die notwendigen Schritte um die Inputdaten herunterzuladen und die Ergebnisse zu produzieren können mit dem Skript `update.sh` ausgeführt werden: `source update.sh`. Die Standardeinstellungen entsprechen dem Setup von Pavel, können aber mit Hilfe von zwei Umgebungsvariablen angepasst werden (mit z.B. `export CORONA=/Pfad/zum/Arbeitsordner`):
* `CORONA`: Hauptordner dieses Repositories
* `GOOGLE_SDK_PATH`: Pfad zu `google-cloud-sdk`. Das wird genutzt um die Inputdaten herunterzuladen.

## Voraussetzungen

* `google-cloud-sdk`
* `python 3.6+`?
    * [datatable](https://github.com/h2oai/datatable)
    * ndjson
    * pytz
* (aktuell unvollständig)