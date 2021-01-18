#!/bin/bash
if [[ $# != 2 ]] ; then
    echo "Got $# params, expected 2!"
    exit 1
fi

rm -rf input
mkdir input

POSTFIX=("localhost pulseaudio.desktop: W: [pulseaudio] main.c: This program is not intended to be run as root (unless --system is specified)."
          "localhost gnome-session-binary[2187]: WARNING: Application 'org.gnome.SettingsDaemon.Housekeeping.desktop' killed by signal 15")

echo "date,rank,extraInfo" >> input/"$1.csv"

for i in $(seq "$2")
  do
    HOUR=$((RANDOM % 24))
    PRIORITY=$((RANDOM % 8))
      if [ $HOUR -le 9 ]; then
        TWO_DIGIT_HOUR="0$HOUR"
      else
        TWO_DIGIT_HOUR="$HOUR"
      fi
    RESULT="Nov 15 $TWO_DIGIT_HOUR:12:33,$PRIORITY,${POSTFIX[$((RANDOM % ${#POSTFIX[*]}))]}"
    echo $RESULT >> input/"$1.csv"
  done
hdfs dfs -put ./input input
