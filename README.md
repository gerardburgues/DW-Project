# DW-Project

## nfiguring Greenplum for Kubernetes

1. Install Docker for your OS. Follow instructions [here](https://docs.docker.com/get-docker/).
2. Install Minikube for your OS. Follow instructions [here](https://minikube.sigs.k8s.io/docs/start/).
3. Install Helm for your OS through package manager. Follow instructions [here](https://helm.sh/docs/intro/install/).
4. Start Minikube `minikube start --kubernetes-version=v1.17.16 --memory 4096 --cpus 4 --vm-driver=<driver>`
   , where \<driver\> [choose appropriate one](https://minikube.sigs.k8s.io/docs/drivers/).
5. Install Greenplum. Follow instructions [here](http://greenplum-kubernetes.docs.pivotal.io/2-3/installing.html). Don't
   skip step 4, skip steps 8-10.

Alternatively:

Download file from: https://network.pivotal.io/products/greenplum-for-kubernetes.

```shell
cd ~/Downloads or the place where you store downloaded file
tar xzf greenplum-for-kubernetes-*.tar.gz
cd ./greenplum-for-kubernetes-*/
# on Windows use Powershell: minikube -p minikube docker-env | Invoke-Expression
eval `minikube docker-env`
docker load -i ./images/greenplum-for-kubernetes
docker load -i ./images/greenplum-operator
helm install greenplum-operator operator/
```

6. Deploy Greenplum Cluster. Follow
   instructions [here](http://greenplum-kubernetes.docs.pivotal.io/2-3/deploy-operator.html).

Alternatively:

```shell
cd workspace
nano my-gp-instance.yaml remove # in 8th line and save file
kubectl apply -f ./my-gp-instance.yaml
```

7. Install RabbitMQ.

```shell
helm repo add bitnami https://charts.bitnami.com/bitnami
helm install rabbitmq bitnami/rabbitmq --set=auth.username=admin --set=auth.password=admin
```

8. Install kubefwd. Follow instructions [here](https://github.com/txn2/kubefwd).

## Additional notes

- To stop minikube:

```shell
minikube stop
```

- To start minikube after restarting your OS or stopping minikube:

```shell
minikube start
```

- To make Greenplum and other minikube deployed apps accessible:

```shell
sudo kubefwd services # Linux/MacOS
```

```powershell
kubefwd services # Windows, run Powershell as administrator
```

## Application logic

There should be 3 modules of the app.

1. ETL module with RabbitMQ queue listener for downloading data from stats.nba.com and storing it in Greenplum database
2. Query module
3. Data representation module

## Database migrations

User gradle to execute SQL migration scripts.

```shell
./gradlew update
```

Scripts have to be put in src/main/resources/db/changelog/ folder with the version name at the end of the file, eg.

```text
db.changelog-1.sql
db.changelog-2.sql <- new migration
etc.
```

If you want SQL script to be applied, you need to add appropriate line in db.changelog.xml with the name of file

```xml
<databaseChangeLog
        xmlns="http://www.liquibase.org/xml/ns/dbchangelog"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="http://www.liquibase.org/xml/ns/dbchangelog
        http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.8.xsd">

    <include file="db.changelog-1.sql" relativeToChangelogFile="true"/>
    <!--New migration below-->
    <include file="db.changelog-2.sql" relativeToChangelogFile="true"/>

</databaseChangeLog>
```
