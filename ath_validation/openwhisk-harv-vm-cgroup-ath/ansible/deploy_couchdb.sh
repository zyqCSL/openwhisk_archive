ansible-playbook prereq.yml
ansible-playbook couchdb.yml
ansible-playbook initdb.yml
ansible-playbook wipe.yml
ansible-playbook openwhisk.yml
# installs a catalog of public packages and actions
ansible-playbook postdeploy.yml
# to use the API gateway
ansible-playbook apigateway.yml
ansible-playbook routemgmt.yml
