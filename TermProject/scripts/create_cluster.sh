#!/bin/bash

sudo aws emr create-cluster \
	--name "Test Connected Components Cluster" \ 
	--ami-version 3.11.0 \
	--service-role EMR_DefaultRole \ 
	--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole \ 
	--log-uri s3://aws-logs-232930595039-us-west-2 \ 
	--enable-debugging \ 
	--instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m3.xlarge,BidPrice=0.1 \ 
					  InstanceGroupType=CORE,InstanceCount=4,InstanceType=r3.xlarge,BidPrice=0.1 \ 
	--applications Name=Ganglia \ 
	--bootstrap-actions Path=s3://elasticmapreduce/bootstrap-actions/configure-hadoop,\
		                Name=IncreasingMapHeapSize,\
						Args=[-m,mapreduce.map.java.opts=-Xmx4684m,\
						      -m,mapreduce.map.memory.mb=5856,\
							  -m,mapred.tasktracker.reduce.tasks.maximum=4,\
							  -m,mapred.reduce.slowstart.completed.maps=0.90]
