# DW-Project

## Configuring Greenplum for Kubernetes
1. Install Docker for your OS. Follow instructions [here](https://docs.docker.com/get-docker/)
2. Install Minikube for your OS. Follow instructions [here](https://minikube.sigs.k8s.io/docs/start/)
3. Install Helm for your OS through package manager. Follow instructions [here](https://helm.sh/docs/intro/install/)
4. Start Minikube `minikube start --kubernetes-version=v1.16.7 --memory 4096 --cpus 4 --vm-driver=<choose appropriate>`, where [choose appropriate](https://minikube.sigs.k8s.io/docs/drivers/)
5. Install Greenplum. Follow instructions [here](http://greenplum-kubernetes.docs.pivotal.io/2-3/installing.html), don't skip 4., skip step 8., 9., 10. or below:
    1. Download file: https://network.pivotal.io/products/greenplum-for-kubernetes
    2. `cd ~/Downloads` or the place where you store downloaded file
    3. `tar xzf greenplum-for-kubernetes-*.tar.gz`
    4. `cd ./greenplum-for-kubernetes-*/`
    5. `eval $(minikube docker-env)` (on Windows use Powershell `minikube -p minikube docker-env | Invoke-Expression`)
    6. `docker load -i ./images/greenplum-for-kubernetes`
    7. `docker load -i ./images/greenplum-operator`
    8. `helm install greenplum-operator operator/`

6 Deploy Greenplum Cluster. Follow instructions [here](http://greenplum-kubernetes.docs.pivotal.io/2-3/deploy-operator.html), or below:
    1. `cd workspace`
    2. `nano my-gp-instance.yaml`, remove # in 8th line and save file
    3. `kubectl apply -f ./my-gp-instance.yaml`

## Additional notes
- To stop minikube type `minikube stop`
- To start minikube after restarting your OS or stopping minikube type `minikube start`
- Make sure if your Greenplum Operator is working fine type `minikube dashboard`. If service is stopped, scale down greenplum-operator to 0 instances and scale it up back to 1
- For making Greenplum accessible, type command: `minikube service greenplum`
