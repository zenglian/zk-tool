import os
import yaml
import commands

PV_YAML = """
apiVersion: v1
kind: PersistentVolume
metadata:
  labels:
    release: x
spec:
  capacity:
    storage: 1Gi
  accessModes:
    - ReadWriteOnce
  persistentVolumeReclaimPolicy: Delete
  storageClassName: zk
  local:
    path: x
  nodeAffinity:
    required:
      nodeSelectorTerms:
        - matchExpressions:
            - key: kubernetes.io/hostname
              operator: Exists
"""

SC_YAML = """
kind: StorageClass
apiVersion: storage.k8s.io/v1
metadata:
  name: zk
  labels:
    release: zk
"""

SVC_YAML = """
apiVersion: v1
kind: Service
metadata:
  labels:
    app: kafka
    pod: kafka-{0}
    release: kafka
  name: kafka-{0}-external
spec:
  ports:
  - name: external-broker
    nodePort: {2}
    port: 19092
    protocol: TCP
    targetPort: {2}
  selector:
    app: kafka
    release: kafka
    statefulset.kubernetes.io/pod-name: kafka-{0}
  type: {1}
"""


class K8s:
    oke = 0

    @staticmethod
    def init():
        K8s.__check_oke()
        if K8s.oke:
            print "oke: true"
        else:
            print "oke: false"

        if not K8s.exists("sc", "zk"):
            K8s.__create_storage_class()

    @staticmethod
    def __check_oke():
        node0 = commands.getoutput("kubectl get node -o=jsonpath='{.items[0].metadata.labels.hostname}'")
        K8s.oke = node0.startswith("oke-")

    @staticmethod
    def read_yaml(file_name):
        with open(file_name, 'r') as stream:
            return yaml.safe_load(stream)

    # resource - yaml or object
    @staticmethod
    def apply(resource):
        cmd = "echo '{}' | kubectl apply -f -".format(resource if type(resource) == str else yaml.dump(resource))
        os.system(cmd)

    @staticmethod
    def exists(res, name):
        output = commands.getoutput("kubectl get {} --field-selector='metadata.name={}'".format(res, name))
        return output != "No resources found."

    @staticmethod
    def status():
        print
        os.system("kubectl get no -o wide")
        print
        os.system("kubectl get sc --field-selector='metadata.name=zk'")
        print
        for line in commands.getoutput("kubectl get pv").splitlines():
            if line.find("STORAGECLASS") > 0 or line.find("zk    ") > 0:
                print line
        print
        os.system("kubectl get pvc,sts,pod,svc -o wide -l 'release in (kafka, zookeeper)' ")

    @staticmethod
    def __create_storage_class():
        sc = yaml.load(SC_YAML)
        if K8s.oke:
            sc["provisioner"] = "oracle.com/oci"
        else:
            sc["provisioner"] = "kubernetes.io/no-provisioner"
            sc["volumeBindingMode"] = "WaitForFirstConsumer"
        K8s.apply(sc)

    @staticmethod
    def create_pv(release, i, size):
        name = "{}-pv-{}".format(release, i)
        path = "/tmp/k8s-pv/" + name
        if not os.path.exists(path):
            os.system("mkdir -p " + path)

        if K8s.exists("pv", name):
            return
        pv = yaml.load(PV_YAML)
        pv["metadata"]["name"] = name
        pv["metadata"]["labels"]["release"] = release
        # todo pod stick to the same pv/pvc
        pv["metadata"]["labels"]["pod"] = release + "-" + str(i)
        pv["spec"]["capacity"]["storage"] = size
        pv["spec"]["local"]["path"] = path
        K8s.apply(pv)

    @staticmethod
    def delete_pv(release):
        os.system("kubectl delete pvc -l release=" + release)
        os.system("kubectl delete pv -l release=" + release)

    @staticmethod
    def create_external_svc(i):
        if not K8s.exists("svc", "kafka-{0}-external".format(i)):
            svc = SVC_YAML.format(i, "LoadBalancer" if K8s.oke else "NodePort", 31090 + i)
            K8s.apply(svc)


class KafkaClient:
    def __init__(self):
        pass

    def __test(self, command):
        command = "kubectl exec " + ("-it " if command.find("console") > 0 else "") + "kafka-0 -- " + command
        print command
        os.system(command)

    def list_topics(self):
        cmd = "kafka-topics --zookeeper zookeeper:2181 --list"
        self.__test(cmd)

    def create_topic(self, name, partition=1, replicas=1):
        cmd = ("kafka-topics --zookeeper zookeeper:2181 --topic {} --create --partitions {} "
               "--replication-factor {} --if-not-exists").format(name, partition, replicas)
        self.__test(cmd)

    def console_produce(self):
        cmd = "kafka-console-producer --broker-list kafka-0.kafka-headless:9092 --topic test"
        self.__test(cmd)

    def console_consume(self):
        cmd = ("kafka-console-consumer --bootstrap-server kafka-0.kafka-headless:9092 "
               "--topic test --from-beginning --group zktool")
        self.__test(cmd)

    def test_producer_perf(self, records=1000, record_size=128, max=200000):
        cmd = ("kafka-producer-perf-test --topic perf --num-records {} --record-size {} --throughput {} "
               " --producer-props bootstrap.servers=kafka-0.kafka-headless:9092").format(records, record_size, max)
        self.__test(cmd)

    def test_consume_perf(self, records=1000, print_metrics=False):
        cmd = ("kafka-consumer-perf-test --broker-list kafka-0.kafka-headless:9092 --group zktool-perf "
               "--topic perf --messages {} {}").format(records, "--print-metrics" if print_metrics else "")
        self.__test(cmd)

    def tests(self):
        for num in (100, 1000, 10000):
            cmd = ("kafka-producer-perf-test --topic perf --num-records {} --throughput 1000000 "
                   "--record-size 256 --producer-props bootstrap.servers=kafka-0.kafka-headless:9092").format(num)
            self.__test(cmd)
