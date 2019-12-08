#!/bin/bash

#espera o postgres iniciar
bash -c 'while !</dev/tcp/db/5432; do sleep 2; done'

#cria as configurações necessárias
python src/config.py

python3 src/oltp.py &
python3 src/olap.py &
python3 src/preditor.py &

#não deixa o container morrer...
tail -f /dev/null