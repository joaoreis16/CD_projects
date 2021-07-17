#!/bin/sh
rm md5sum.txt
#cp *py ../../chord/tests
md5 -r test*py > md5sum.txt
gist -u 054f54f413a270a06b11599e9f7819be md5sum.txt  
