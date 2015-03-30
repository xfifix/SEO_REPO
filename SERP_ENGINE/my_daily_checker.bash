#!/bin/bash
echo 'Killing previous processes'
sudo kill $(ps aux | grep 'position_checker.jar' | awk '{print $2}');
echo 'Cleaning up database results'
sudo java -jar /home/sduprey/My_Executable/my_close_unfinish_position_checker.jar;
echo 'Removing the previous log file'
sudo rm -r /tmp/slow_daily_position_checker.log;
echo 'Launching the new job'
sudo java -jar /home/sduprey/My_Executable/my_daily_slow_position_checker.jar "Plan ouverture Facette contextuallisé";


