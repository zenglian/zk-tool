import os
import yaml
import commands
from style import yellow

def readYaml(fileName):
  with open(fileName, 'r') as stream:
    return yaml.safe_load(stream)


def apply(resource):
  kcmd = "echo '{}' | kubectl apply -f -".format(yaml.dump(resource))
  os.system(kcmd)


class Chart:
  def __init__(self, name):
    self.name = name
    self.replicas = 1
    self.valuesFile = "../" + self.name + "/values.yaml"
    self.values = readYaml(self.valuesFile)
    self.pvSize = self.values["persistence"]["size"]

  def create(self):
    os.system("helm install --name {0} ../{0}".format(self.name))

  def delete(self):
    os.system("helm delete --purge " + self.name)

  def createPv(self, replicas=0):
    for i in range(replicas if replicas > 0 else self.replicas):
      path = "/tmp/k8s-pv/" + self.name + "-" + str(i)
      if not os.path.exists(path):
        os.system("mkdir -p " + path)

      pv = readYaml("yaml/pv.yaml")
      pv["metadata"]["name"] = self.name + "-pv-" + str(i)
      pv["metadata"]["labels"]["release"] = self.name
      pv["spec"]["capacity"]["storage"] = self.pvSize
      pv["spec"]["local"]["path"] = path
      apply(pv)

  def deletePvc(self):
    os.system("kubectl delete pvc -l release=" + self.name)

  def deletePV(self):
    os.system("kubectl delete pv -l release=" + self.name)

  def scale(self, replicas):
    self.createPv(replicas)
    os.system("kubectl scale --replicas=" + str(replicas) + " statefulset/" + self.name)


class KafkaChart(Chart):
  def __init__(self):
    Chart.__init__(self, "kafka")
    self.replicas = self.values["replicas"]

  def configLbr(self):
    self.values["external"]["type"] = "LoadBalancer"
    # TODO

  def configNodePort(self):
    self.values["external"]["type"] = "NodePort"
    # TODO


class ZookeeperChart(Chart):
  def __init__(self):
    Chart.__init__(self, "zookeeper")
    self.replicas = self.values["replicaCount"]

  def hackGfw(self):
    self.values["image"]["repository"] = "gcr.azk8s.cn/google_samples/k8szk"


class K8s:
  oke = 0

  def __init__(self):
    self.__checkOke()
    self.__createStorageClass()

  def status(self):
    os.system("kubectl get pv -l 'release in (kafka, zookeeper)'")
    print
    os.system("kubectl get pvc,sts,pod,svc -o wide -l 'release in (kafka, zookeeper)' ")

  def __createStorageClass(self):
    output = commands.getoutput("kubectl get sc -o=jsonpath='{range .items[*]}{.metadata.name}{\",\"}'")
    if output.find("zk,") >= 0:
      return

    sc = readYaml("yaml/sc.yaml")
    if self.oke:
      sc["provisioner"] = "oracle.com/oci"
    else:
      sc["provisioner"] = "kubernetes.io/no-provisioner"
      sc["volumeBindingMode"] = "WaitForFirstConsumer"
    apply(sc)

  def __checkOke(self):
    node0 = commands.getoutput("kubectl get node -o=jsonpath='{.items[0].metadata.labels.hostname}'")
    self.oke = node0.startswith("oke-")
    if self.oke:
      print yellow("Node " + node0 + " is assumed to be running inside oke.")
    else:
      print yellow("This box is assumed to be a hosted or local box (since node names are not started with \"oke-\").")

  def test(self, command):
    command = "kubectl exec " + ("-it " if command.find("console") > 0 else "") + "kafka-0 -- " + command
    print command
    os.system(command)
