from k8s import K8s
import os
import shutil


class Chart:
    def __init__(self):
        self.values = K8s.read_yaml(self.path + "/values.yaml")
        self.pvSize = self.values["persistence"]["size"]

    def create(self):
        for i in range(self.replicas):
            if not K8s.oke:
                K8s.create_pv(self.name, i, self.values["persistence"]["size"])
        os.system("helm install --name {0} ../{0}".format(self.name))

    def delete(self):
        os.system("helm delete --purge " + self.name)
        if not K8s.oke:
            K8s.delete_pv(self.name)

    def scale(self, replicas):
        for i in range(replicas):
            if not K8s.oke:
                K8s.create_pv(self.name, i, self.values["persistence"]["size"])
        os.system("kubectl scale --replicas=" + str(replicas) + " statefulset/" + self.name)


class KafkaChart(Chart):
    name = "kafka"
    path = "../kafka"

    def __init__(self):
        if K8s.oke:
            shutil.copyfile(self.path + "/values-lbr.yaml", self.path + "/values.yaml")
        else:
            shutil.copy(self.path + "/values-np.yaml", self.path + "/values.yaml")
        Chart.__init__(self)
        self.replicas = self.values["replicas"]

    def scale(self, replicas):
        for i in range(replicas):
            K8s.create_external_svc(i)
        Chart.scale(self, replicas)

class ZookeeperChart(Chart):
    name = "zookeeper"
    path = "../zookeeper"

    def __init__(self):
        Chart.__init__(self)
        self.replicas = self.values["replicaCount"]

    def hack_gfw(self):
        self.values["image"]["repository"] = "gcr.azk8s.cn/google_samples/k8szk"


