import subprocess,sys
import json
import paramiko

#remove credentials file
subprocess.call(['rm', '-rf','~/.aws/credentials'])

#command line to execute
create_cluster_query="""
##Your emr create-cluster query goes here
"""

#Execute and capture output for create-cluster command
p=subprocess.Popen(create_cluster_query, stdout=subprocess.PIPE ,shell =True)
(output, err) = p.communicate()
cluster_info=json.loads(output)
#Get cluster ID by cluster_info["cluster_id"]

#Get master address
get_master_address_query="""aws emr describe-cluster --cluster-id """+cluster_info["cluster_id"] #Add --region parameter to filter according to region
p=subprocess.Popen(get_master_address_query, stdout=subprocess.PIPE ,shell =True)
(output, err) = p.communicate()
cluster_json=json.loads(output)
master_id_unparsed=cluster_json["Cluster"]["MasterPublicDnsName"]

#Parse master address to get the master IP
master_id_parsed=master_id_unparsed.split(".")[0].split('-',1)[1].replace('-','.')

#SSH into the master IP
ssh_client =paramiko.SSHClient()
ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh_client.connect(hostname=master_id_parsed,key_filename=sys.argv[1])

#Once you're inside the master machine, execute your commands
ssh_client.exec_command(sys.argv[2])
##############################Execute more commands################################
#                                                                                 #
#                                                                                 #
#                                                                                 #
###################################################################################

#Terminate the cluster
subprocess.call(['aws', 'emr', 'terminate-clusters', '--cluster-ids',cluster_info["cluster_id"]])