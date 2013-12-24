#!/bin/bash

IP=`/sbin/ifconfig eth0 | grep "inet addr" | cut -d: -f2 | awk '{print $1}'`
BCASTIP=`/sbin/ifconfig eth0 | grep "inet addr" | cut -d: -f3 | awk '{print $1}'`
sed s/\@NODE_IP\@/${IP}/g jgroups-tcp.xml.tmpl | sed s/\@BPING_ADDR\@/${BCASTIP}/g  > jgroups-tcp.xml
java -cp ".:crawler.jar" -Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.SimpleLog  eu.leads.crawler.PersistentCrawl 1> /dev/null 2> /dev/null &
java -cp ".:LeadsQueryProcessor.jar" -Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.SimpleLog  eu.leads.processor.ui.SQLInterfaceBootstrap
