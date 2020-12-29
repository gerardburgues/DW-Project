# DW-Project

## Configuring Greenplum for Kubernetes

1. Install Docker for your OS. Follow instructions [here](https://docs.docker.com/get-docker/)
2. Install Minikube for your OS. Follow instructions [here](https://minikube.sigs.k8s.io/docs/start/)
3. Install Helm for your OS through package manager. Follow instructions [here](https://helm.sh/docs/intro/install/)
4. Start Minikube `minikube start --kubernetes-version=v1.17.16 --memory 6144 --cpus 4 --vm-driver=<choose appropriate>`
   , where [choose appropriate](https://minikube.sigs.k8s.io/docs/drivers/)
5. Install Greenplum. Follow instructions [here](http://greenplum-kubernetes.docs.pivotal.io/2-3/installing.html), don't
   skip 4., skip step 8., 9., 10. or below:
    1. Download file: https://network.pivotal.io/products/greenplum-for-kubernetes
    2. `cd ~/Downloads` or the place where you store downloaded file
    3. `tar xzf greenplum-for-kubernetes-*.tar.gz`
    4. `cd ./greenplum-for-kubernetes-*/`
    5. `eval $(minikube docker-env)` (on Windows use Powershell `minikube -p minikube docker-env | Invoke-Expression`)
    6. `docker load -i ./images/greenplum-for-kubernetes`
    7. `docker load -i ./images/greenplum-operator`
    8. `helm install greenplum-operator operator/`

6. Deploy Greenplum Cluster. Follow
   instructions [here](http://greenplum-kubernetes.docs.pivotal.io/2-3/deploy-operator.html), or below:
    1. `cd workspace`
    2. `nano my-gp-instance.yaml` remove # in 8th line and save file
    3. `kubectl apply -f ./my-gp-instance.yaml`

7. Install RabbitMQ
   Operator `kubectl apply -f "https://github.com/rabbitmq/cluster-operator/releases/latest/download/cluster-operator.yml"`
8. Go to the folder of the project and run: `kubectl apply -f rabbitmq.yaml`

## Additional notes

- To stop minikube type `minikube stop`
- To start minikube after restarting your OS or stopping minikube type `minikube start`
- For making Greenplum accessible:
  (preferred) Use: https://github.com/txn2/kubefwd `sudo kubefwd services` -> postgresql://greenplum:5432/gpadmin type
  command: `kubectl port-forward service/greenplum 5432:5432` -> postgresql://localhost:5432/gpadmin
- To find out username and password to RabbitMQ execute:
  `echo Username: $(kubectl get secret rabbitmq-default-user -o jsonpath="{.data.username}" | base64 --decode)`
  `echo Password: $(kubectl get secret rabbitmq-default-user -o jsonpath="{.data.password}" | base64 --decode)`
  On Windows alternatively in Powershell:
  `echo "Username: $([Text.Encoding]::Utf8.GetString([Convert]::FromBase64String($(kubectl get secret rabbitmq-default-user -o jsonpath="{.data.username}"))))"`
  `echo "Password: $([Text.Encoding]::Utf8.GetString([Convert]::FromBase64String($(kubectl get secret rabbitmq-default-user -o jsonpath="{.data.password}"))))"`
  
## Application logic
There should be 3 modules of the app.
1. ETL module with queue listener for downloading data from stats.nba.com and storing it in Greenplum
2. Query module
3. Data representation module
