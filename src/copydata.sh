#!/bin/bash

PROJDIR=/lustre/isaac/proj/UTK0192/gensvc
DELIVERED=DeliveryComplete.txt
DAYSM1=31

for of in `find $PROJDIR -maxdepth 2 -name 'job.o[0-9]*' -mtime -$DAYSM1; find $PROJDIR/MiSeqRuns -maxdepth 2 -name 'job.o[0-9]*' -mtime -7`; do
    rundir=`dirname $of`
    outfile=`basename $of`
    if [ ! -f $rundir/$DELIVERED ]; then
        j=`echo $of | sed 's/.*job.o//'`
        if ! sacct --format=State -npj $j | grep -v '^COMPLETED|' > /dev/null 2>&1; then
            specsheet=`basename \`ls $rundir/*.csv | grep -v '\/SampleSheet.csv'\``
            org=`echo $specsheet | awk -F_ '{print $1;}'`
            per=`echo $specsheet | awk -F_ '{print $2;}'`
            if [ $org == UTK ]; then
                echo handle file copy for completed conversion run in $rundir: send to $per
            else
                echo cannot figure out how to copy completed conversion run in $rundir specified by $specsheet
            fi
            echo touch $rundir/$DELIVERED
        fi
    fi
done

for rundir in `dirname \`find "$PROJDIR/MiSeqRuns" -maxdepth 2 -mtime -$DAYSM1 -name CopyComplete.txt\` 2>/dev/null`; do
    if [ `find "$rundir/Alignment_1" -mindepth 2 -maxdepth 2 -type d -name Fastq | wc -l` -ne 0 ]; then
        echo handle file copy for completed sequencing run with fastq data in $rundir
    fi
done
