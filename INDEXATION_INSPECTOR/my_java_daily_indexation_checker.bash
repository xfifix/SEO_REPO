#!/bin/bash
echo 'Killing previous processes'
sudo kill $(ps aux | grep 'my_indexation_checker_job.jar' | awk '{print $2}');
echo 'Launching the new job'
sudo java -jar /home/sduprey/My_Executable/my_indexation_checker_job.jar;