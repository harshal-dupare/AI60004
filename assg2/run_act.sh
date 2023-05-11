#!/bin/bash
for fname in tests/*.txt ; do
   echo python assignment-2-18MA20015.py ${fname}
   python3 actual.py ${fname}
done