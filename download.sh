#!/bin/bash
DIR="/tmp/final_project"
if [ -d "$DIR" ]; then 
   true
else
  #make directory
  mkdir /tmp/final_project
fi
curl -o /tmp/get.tar.gz loh.istgahesalavati.ir/report.gz.tar
tar -xvzf /tmp/get.tar.gz -C /tmp/final_project
