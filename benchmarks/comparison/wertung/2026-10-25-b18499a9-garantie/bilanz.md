# Garantie-Vergleich 20261025-192012

## Durchgang 1

| System | Profil | Störung | Status | bestätigt | ausgeführt | Verluste | Duplikate | unbestimmt | abgelehnt | Duplikate E-077 | Worker neu gestartet |
|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| swarmq | ab_werk | sigkill | ok | 61 | 61 | 0 | 0 | 0 | 0 | 0 | nein |
| celery | ab_werk | sigkill | ok | 61 | 41 | 20 | 0 | 0 | 0 | 0 | nein |
| dramatiq | ab_werk | sigkill | ok | 61 | 61 | 0 | 0 | 0 | 0 | 0 | nein |
| arq | ab_werk | sigkill | ok | 61 | 61 | 0 | 0 | 0 | 0 | 0 | nein |
| taskiq | ab_werk | sigkill | ok | 61 | 61 | 0 | 0 | 0 | 0 | 0 | nein |
| swarmq | stark | sigkill | ok | 61 | 61 | 0 | 0 | 0 | 0 | 0 | nein |
| celery | stark | sigkill | ok | 61 | 61 | 0 | 0 | 0 | 0 | 0 | nein |
| dramatiq | stark | sigkill | ok | 61 | 61 | 0 | 0 | 0 | 0 | 0 | nein |
| arq | stark | sigkill | ok | 61 | 61 | 0 | 0 | 0 | 0 | 0 | nein |
| taskiq | stark | sigkill | ok | 61 | 61 | 0 | 0 | 0 | 0 | 0 | nein |
| swarmq | ab_werk | broker_neustart | ok | 63 | 63 | 0 | 0 | 0 | 0 | 0 | nein |
| celery | ab_werk | broker_neustart | ok | 79 | 79 | 0 | 0 | 0 | 7 | 0 | nein |
| dramatiq | ab_werk | broker_neustart | ok | 62 | 62 | 0 | 32 | 0 | 28 | 0 | nein |
| arq | ab_werk | broker_neustart | ok | 82 | 82 | 0 | 0 | 0 | 3 | 0 | nein |
| taskiq | ab_werk | broker_neustart | ok | 32 | 32 | 0 | 20 | 0 | 58 | 0 | nein |
| swarmq | stark | broker_neustart | ok | 63 | 63 | 0 | 0 | 0 | 0 | 0 | nein |
| celery | stark | broker_neustart | ok | 72 | 72 | 0 | 20 | 0 | 8 | 0 | nein |
| dramatiq | stark | broker_neustart | ok | 62 | 62 | 0 | 20 | 0 | 28 | 0 | nein |
| arq | stark | broker_neustart | ok | 81 | 81 | 0 | 0 | 0 | 3 | 0 | nein |
| taskiq | stark | broker_neustart | ok | 32 | 32 | 0 | 20 | 0 | 58 | 0 | nein |
| swarmq | ab_werk | netztrennung | ok | 61 | 61 | 0 | 0 | 1 | 0 | 0 | nein |
| celery | ab_werk | netztrennung | ok | 61 | 62 | 0 | 0 | 1 | 1 | 0 | nein |
| dramatiq | ab_werk | netztrennung | ok | 61 | 61 | 0 | 31 | 0 | 29 | 0 | nein |
| arq | ab_werk | netztrennung | ok | 61 | 61 | 0 | 4 | 0 | 29 | 0 | ja |
| taskiq | ab_werk | netztrennung | ok | 31 | 31 | 0 | 20 | 0 | 59 | 0 | nein |
| swarmq | stark | netztrennung | ok | 61 | 61 | 0 | 0 | 1 | 0 | 0 | nein |
| celery | stark | netztrennung | ok | 61 | 62 | 0 | 20 | 1 | 1 | 0 | nein |
| dramatiq | stark | netztrennung | ok | 61 | 61 | 0 | 11 | 0 | 29 | 0 | nein |
| arq | stark | netztrennung | ok | 61 | 61 | 0 | 0 | 0 | 29 | 0 | ja |
| taskiq | stark | netztrennung | ok | 31 | 31 | 0 | 20 | 0 | 59 | 0 | nein |
| swarmq | ab_werk | valkey_neustart_persistent | ok | 85 | 85 | 0 | 0 | 0 | 0 | 0 | nein |
| celery | ab_werk | valkey_neustart_persistent | ok | 77 | 77 | 0 | 0 | 0 | 0 | 0 | nein |
| dramatiq | ab_werk | valkey_neustart_persistent | ok | 84 | 84 | 0 | 0 | 0 | 0 | 0 | nein |
| arq | ab_werk | valkey_neustart_persistent | ok | 81 | 81 | 0 | 0 | 0 | 3 | 0 | nein |
| taskiq | ab_werk | valkey_neustart_persistent | ok | 83 | 83 | 0 | 0 | 0 | 0 | 0 | nein |
| swarmq | stark | valkey_neustart_persistent | ok | 84 | 84 | 0 | 0 | 0 | 0 | 0 | nein |
| celery | stark | valkey_neustart_persistent | ok | 75 | 75 | 0 | 0 | 0 | 0 | 0 | nein |
| dramatiq | stark | valkey_neustart_persistent | ok | 84 | 84 | 0 | 0 | 0 | 0 | 0 | nein |
| arq | stark | valkey_neustart_persistent | ok | 81 | 81 | 0 | 0 | 0 | 3 | 0 | nein |
| taskiq | stark | valkey_neustart_persistent | ok | 85 | 85 | 0 | 0 | 0 | 0 | 0 | nein |
| swarmq | ab_werk | valkey_neustart_fluechtig | ok | 84 | 84 | 0 | 0 | 0 | 0 | 0 | nein |
| celery | ab_werk | valkey_neustart_fluechtig | ok | 76 | 76 | 0 | 0 | 0 | 0 | 0 | nein |
| dramatiq | ab_werk | valkey_neustart_fluechtig | ok | 84 | 84 | 0 | 0 | 0 | 0 | 0 | nein |
| arq | ab_werk | valkey_neustart_fluechtig | ok | 82 | 60 | 22 | 0 | 0 | 3 | 0 | nein |
| taskiq | ab_werk | valkey_neustart_fluechtig | ok | 83 | 83 | 0 | 0 | 0 | 0 | 0 | nein |
| swarmq | stark | valkey_neustart_fluechtig | ok | 85 | 85 | 0 | 0 | 0 | 0 | 0 | nein |
| celery | stark | valkey_neustart_fluechtig | ok | 75 | 75 | 0 | 0 | 0 | 0 | 0 | nein |
| dramatiq | stark | valkey_neustart_fluechtig | ok | 84 | 84 | 0 | 0 | 0 | 0 | 0 | nein |
| arq | stark | valkey_neustart_fluechtig | ok | 83 | 60 | 23 | 0 | 0 | 2 | 0 | nein |
| taskiq | stark | valkey_neustart_fluechtig | ok | 84 | 84 | 0 | 0 | 0 | 0 | 0 | nein |

## Durchgang 2

| System | Profil | Störung | Status | bestätigt | ausgeführt | Verluste | Duplikate | unbestimmt | abgelehnt | Duplikate E-077 | Worker neu gestartet |
|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| swarmq | ab_werk | sigkill | ok | 61 | 61 | 0 | 0 | 0 | 0 | 0 | nein |
| celery | ab_werk | sigkill | ok | 61 | 41 | 20 | 0 | 0 | 0 | 0 | nein |
| dramatiq | ab_werk | sigkill | ok | 61 | 61 | 0 | 0 | 0 | 0 | 0 | nein |
| arq | ab_werk | sigkill | ok | 61 | 61 | 0 | 0 | 0 | 0 | 0 | nein |
| taskiq | ab_werk | sigkill | ok | 61 | 61 | 0 | 0 | 0 | 0 | 0 | nein |
| swarmq | stark | sigkill | ok | 61 | 61 | 0 | 0 | 0 | 0 | 0 | nein |
| celery | stark | sigkill | ok | 61 | 61 | 0 | 0 | 0 | 0 | 0 | nein |
| dramatiq | stark | sigkill | ok | 61 | 61 | 0 | 0 | 0 | 0 | 0 | nein |
| arq | stark | sigkill | ok | 61 | 61 | 0 | 0 | 0 | 0 | 0 | nein |
| taskiq | stark | sigkill | ok | 61 | 61 | 0 | 0 | 0 | 0 | 0 | nein |
| swarmq | ab_werk | broker_neustart | ok | 63 | 63 | 0 | 0 | 0 | 0 | 0 | nein |
| celery | ab_werk | broker_neustart | ok | 67 | 67 | 0 | 0 | 0 | 9 | 0 | nein |
| dramatiq | ab_werk | broker_neustart | ok | 62 | 62 | 0 | 32 | 0 | 28 | 0 | nein |
| arq | ab_werk | broker_neustart | ok | 81 | 81 | 0 | 0 | 0 | 3 | 0 | nein |
| taskiq | ab_werk | broker_neustart | ok | 32 | 32 | 0 | 20 | 0 | 58 | 0 | nein |
| swarmq | stark | broker_neustart | ok | 90 | 90 | 0 | 0 | 0 | 0 | 0 | nein |
| celery | stark | broker_neustart | ok | 67 | 67 | 0 | 20 | 0 | 9 | 0 | nein |
| dramatiq | stark | broker_neustart | ok | 61 | 61 | 0 | 20 | 0 | 29 | 0 | nein |
| arq | stark | broker_neustart | ok | 82 | 82 | 0 | 0 | 0 | 3 | 0 | nein |
| taskiq | stark | broker_neustart | ok | 62 | 62 | 0 | 20 | 0 | 28 | 0 | nein |
| swarmq | ab_werk | netztrennung | ok | 61 | 61 | 0 | 0 | 1 | 0 | 0 | nein |
| celery | ab_werk | netztrennung | ok | 61 | 62 | 0 | 0 | 1 | 1 | 0 | nein |
| dramatiq | ab_werk | netztrennung | ok | 61 | 61 | 0 | 31 | 0 | 29 | 0 | nein |
| arq | ab_werk | netztrennung | ok | 61 | 61 | 0 | 3 | 0 | 29 | 0 | ja |
| taskiq | ab_werk | netztrennung | ok | 31 | 31 | 0 | 20 | 0 | 59 | 0 | nein |
| swarmq | stark | netztrennung | ok | 61 | 61 | 0 | 0 | 1 | 0 | 0 | nein |
| celery | stark | netztrennung | ok | 61 | 62 | 0 | 20 | 1 | 1 | 0 | nein |
| dramatiq | stark | netztrennung | ok | 61 | 61 | 0 | 18 | 0 | 29 | 0 | nein |
| arq | stark | netztrennung | ok | 61 | 61 | 0 | 0 | 0 | 29 | 0 | ja |
| taskiq | stark | netztrennung | ok | 31 | 31 | 0 | 20 | 0 | 59 | 0 | nein |
| swarmq | ab_werk | valkey_neustart_persistent | ok | 84 | 84 | 0 | 0 | 0 | 0 | 0 | nein |
| celery | ab_werk | valkey_neustart_persistent | ok | 76 | 76 | 0 | 0 | 0 | 0 | 0 | nein |
| dramatiq | ab_werk | valkey_neustart_persistent | ok | 84 | 84 | 0 | 0 | 0 | 0 | 0 | nein |
| arq | ab_werk | valkey_neustart_persistent | ok | 80 | 80 | 0 | 0 | 0 | 4 | 0 | nein |
| taskiq | ab_werk | valkey_neustart_persistent | ok | 84 | 84 | 0 | 0 | 0 | 0 | 0 | nein |
| swarmq | stark | valkey_neustart_persistent | ok | 84 | 84 | 0 | 0 | 0 | 0 | 0 | nein |
| celery | stark | valkey_neustart_persistent | ok | 75 | 75 | 0 | 0 | 0 | 0 | 0 | nein |
| dramatiq | stark | valkey_neustart_persistent | ok | 84 | 84 | 0 | 0 | 0 | 0 | 0 | nein |
| arq | stark | valkey_neustart_persistent | ok | 81 | 81 | 0 | 0 | 0 | 3 | 0 | nein |
| taskiq | stark | valkey_neustart_persistent | ok | 84 | 84 | 0 | 0 | 0 | 0 | 0 | nein |
| swarmq | ab_werk | valkey_neustart_fluechtig | ok | 84 | 84 | 0 | 0 | 0 | 0 | 0 | nein |
| celery | ab_werk | valkey_neustart_fluechtig | ok | 77 | 77 | 0 | 0 | 0 | 0 | 0 | nein |
| dramatiq | ab_werk | valkey_neustart_fluechtig | ok | 84 | 84 | 0 | 0 | 0 | 0 | 0 | nein |
| arq | ab_werk | valkey_neustart_fluechtig | ok | 82 | 60 | 22 | 0 | 0 | 2 | 0 | nein |
| taskiq | ab_werk | valkey_neustart_fluechtig | ok | 84 | 84 | 0 | 0 | 0 | 0 | 0 | nein |
| swarmq | stark | valkey_neustart_fluechtig | ok | 84 | 84 | 0 | 0 | 0 | 0 | 0 | nein |
| celery | stark | valkey_neustart_fluechtig | ok | 75 | 75 | 0 | 0 | 0 | 0 | 0 | nein |
| dramatiq | stark | valkey_neustart_fluechtig | ok | 84 | 84 | 0 | 0 | 0 | 0 | 0 | nein |
| arq | stark | valkey_neustart_fluechtig | ok | 82 | 60 | 22 | 0 | 0 | 3 | 0 | nein |
| taskiq | stark | valkey_neustart_fluechtig | ok | 84 | 84 | 0 | 0 | 0 | 0 | 0 | nein |

## Durchgang 3

| System | Profil | Störung | Status | bestätigt | ausgeführt | Verluste | Duplikate | unbestimmt | abgelehnt | Duplikate E-077 | Worker neu gestartet |
|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| swarmq | ab_werk | sigkill | ok | 61 | 61 | 0 | 0 | 0 | 0 | 0 | nein |
| celery | ab_werk | sigkill | ok | 61 | 41 | 20 | 0 | 0 | 0 | 0 | nein |
| dramatiq | ab_werk | sigkill | ok | 61 | 61 | 0 | 0 | 0 | 0 | 0 | nein |
| arq | ab_werk | sigkill | ok | 61 | 61 | 0 | 0 | 0 | 0 | 0 | nein |
| taskiq | ab_werk | sigkill | ok | 61 | 61 | 0 | 0 | 0 | 0 | 0 | nein |
| swarmq | stark | sigkill | ok | 61 | 61 | 0 | 0 | 0 | 0 | 0 | nein |
| celery | stark | sigkill | ok | 61 | 61 | 0 | 0 | 0 | 0 | 0 | nein |
| dramatiq | stark | sigkill | ok | 61 | 61 | 0 | 0 | 0 | 0 | 0 | nein |
| arq | stark | sigkill | ok | 61 | 61 | 0 | 0 | 0 | 0 | 0 | nein |
| taskiq | stark | sigkill | ok | 61 | 61 | 0 | 0 | 0 | 0 | 0 | nein |
| swarmq | ab_werk | broker_neustart | ok | 89 | 90 | 0 | 0 | 1 | 0 | 0 | nein |
| celery | ab_werk | broker_neustart | ok | 80 | 80 | 0 | 0 | 0 | 10 | 0 | nein |
| dramatiq | ab_werk | broker_neustart | ok | 62 | 62 | 0 | 32 | 0 | 28 | 0 | nein |
| arq | ab_werk | broker_neustart | ok | 81 | 81 | 0 | 0 | 0 | 3 | 0 | nein |
| taskiq | ab_werk | broker_neustart | ok | 62 | 62 | 0 | 20 | 0 | 28 | 0 | nein |
| swarmq | stark | broker_neustart | ok | 90 | 90 | 0 | 0 | 0 | 0 | 0 | nein |
| celery | stark | broker_neustart | ok | 79 | 79 | 0 | 20 | 0 | 11 | 0 | nein |
| dramatiq | stark | broker_neustart | ok | 62 | 62 | 0 | 20 | 0 | 28 | 0 | nein |
| arq | stark | broker_neustart | ok | 82 | 82 | 0 | 0 | 0 | 3 | 0 | nein |
| taskiq | stark | broker_neustart | ok | 62 | 62 | 0 | 20 | 0 | 28 | 0 | nein |
| swarmq | ab_werk | netztrennung | ok | 61 | 61 | 0 | 0 | 1 | 0 | 0 | nein |
| celery | ab_werk | netztrennung | ok | 61 | 62 | 0 | 0 | 1 | 1 | 0 | nein |
| dramatiq | ab_werk | netztrennung | ok | 61 | 61 | 0 | 31 | 0 | 29 | 0 | nein |
| arq | ab_werk | netztrennung | ok | 61 | 61 | 0 | 0 | 0 | 29 | 0 | ja |
| taskiq | ab_werk | netztrennung | ok | 31 | 31 | 0 | 20 | 0 | 59 | 0 | nein |
| swarmq | stark | netztrennung | ok | 61 | 61 | 0 | 0 | 1 | 0 | 0 | nein |
| celery | stark | netztrennung | ok | 61 | 62 | 0 | 20 | 1 | 1 | 0 | nein |
| dramatiq | stark | netztrennung | ok | 61 | 61 | 0 | 18 | 0 | 29 | 0 | nein |
| arq | stark | netztrennung | ok | 61 | 61 | 0 | 3 | 0 | 29 | 0 | ja |
| taskiq | stark | netztrennung | ok | 31 | 31 | 0 | 20 | 0 | 59 | 0 | nein |
| swarmq | ab_werk | valkey_neustart_persistent | ok | 84 | 84 | 0 | 0 | 0 | 0 | 0 | nein |
| celery | ab_werk | valkey_neustart_persistent | ok | 77 | 77 | 0 | 0 | 0 | 0 | 0 | nein |
| dramatiq | ab_werk | valkey_neustart_persistent | ok | 85 | 85 | 0 | 0 | 0 | 0 | 0 | nein |
| arq | ab_werk | valkey_neustart_persistent | ok | 80 | 80 | 0 | 0 | 0 | 4 | 0 | nein |
| taskiq | ab_werk | valkey_neustart_persistent | ok | 84 | 84 | 0 | 0 | 0 | 0 | 0 | nein |
| swarmq | stark | valkey_neustart_persistent | ok | 83 | 83 | 0 | 0 | 0 | 0 | 0 | nein |
| celery | stark | valkey_neustart_persistent | ok | 76 | 76 | 0 | 0 | 0 | 0 | 0 | nein |
| dramatiq | stark | valkey_neustart_persistent | ok | 84 | 84 | 0 | 0 | 0 | 0 | 0 | nein |
| arq | stark | valkey_neustart_persistent | ok | 80 | 80 | 0 | 0 | 0 | 4 | 0 | nein |
| taskiq | stark | valkey_neustart_persistent | ok | 84 | 84 | 0 | 0 | 0 | 0 | 0 | nein |
| swarmq | ab_werk | valkey_neustart_fluechtig | ok | 84 | 84 | 0 | 0 | 0 | 0 | 0 | nein |
| celery | ab_werk | valkey_neustart_fluechtig | ok | 76 | 76 | 0 | 0 | 0 | 0 | 0 | nein |
| dramatiq | ab_werk | valkey_neustart_fluechtig | ok | 84 | 84 | 0 | 0 | 0 | 0 | 0 | nein |
| arq | ab_werk | valkey_neustart_fluechtig | ok | 81 | 60 | 21 | 0 | 0 | 3 | 0 | nein |
| taskiq | ab_werk | valkey_neustart_fluechtig | ok | 84 | 84 | 0 | 0 | 0 | 0 | 0 | nein |
| swarmq | stark | valkey_neustart_fluechtig | ok | 83 | 83 | 0 | 0 | 0 | 0 | 0 | nein |
| celery | stark | valkey_neustart_fluechtig | ok | 75 | 75 | 0 | 0 | 0 | 0 | 0 | nein |
| dramatiq | stark | valkey_neustart_fluechtig | ok | 85 | 85 | 0 | 0 | 0 | 0 | 0 | nein |
| arq | stark | valkey_neustart_fluechtig | ok | 82 | 60 | 22 | 0 | 0 | 2 | 0 | nein |
| taskiq | stark | valkey_neustart_fluechtig | ok | 84 | 84 | 0 | 0 | 0 | 0 | 0 | nein |
