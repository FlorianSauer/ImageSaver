#!/usr/bin/env bash

python3 ImageSaver2.py wipe -c
dd if=/dev/urandom bs=1MB count=100 2> /dev/null | python3 ImageSaver2.py upload -i stdin -ow -fs=5MB
dd if=/dev/urandom bs=1MB count=100 2> /dev/null | python3 ImageSaver2.py upload -i stdin -ow -fs=0.5MB
python3 ImageSaver2.py statistic