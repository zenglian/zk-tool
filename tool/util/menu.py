#!/usr/bin/env python
# -*- coding: UTF-8 -*-

# using Java naming convention

from k8s import K8s, KafkaChart, ZookeeperChart
from style import yellow, bold, red


class Menu:
  k8s = K8s()
  kafka = KafkaChart()
  zookeeper = ZookeeperChart()

  def __styleItem(self, title):
    pos = title.find("*")
    return title if pos < 0 else title[0:pos] + yellow(title[pos + 1]) + title[pos + 2:]

  def __showItems(self, title, items):
    print bold("-" * 50 + title + "-" * 50)
    for item in items:
      print self.__styleItem(item)
    print bold("-" * (100 + len(title)))

  def __test(self):
    print yellow("make sure pod status=Running and ready=1/1")
    items = ["*list topics", "*create topic", "*write message", "*read message", "*producer perf", "con*sumer perf", "*quit"]
    while True:
      print
      self.__showItems("Test tool", items)
      op = raw_input("choose command: ")
      if op == "l":
        cmd = "kafka-topics --zookeeper zookeeper:2181 --list"
      elif op == "c":
        tokens = raw_input("Input name, partitions, replications: ").split(",")
        cmd = "kafka-topics --zookeeper zookeeper:2181 --topic {} --create --partitions {} --replication-factor {}" \
          .format(tokens[0], tokens[1], tokens[2])
      elif op == "w":
        cmd = "kafka-console-producer --broker-list kafka-0.kafka-headless:9092 --topic test"
      elif op == "r":
        cmd = "kafka-console-consumer --bootstrap-server kafka-0.kafka-headless:9092 --topic test --from-beginning --group zktool"
      elif op == "p":
        num = raw_input("number of records: ")
        if num == "": num = "10000"
        cmd = "kafka-producer-perf-test --topic test --num-records {} --print-metrics --throughput 1000000 --record-size 256 " \
              "--producer-props bootstrap.servers=kafka-0.kafka-headless:9092".format(num)
      elif op == "s":
        count = raw_input("number of records: ")
        if count == "": count = "10000"
        cmd = "kafka-consumer-perf-test --broker-list kafka-0.kafka-headless:9092 --group zktool-perf --print-metrics --topic test " \
              "--messages {}".format(count)
      elif op == "q":
        return
      else:
        print red("Unknown command.")
        continue
      self.k8s.test(cmd)
      print

  def main(self):
    items = ["*create cluster", "*read status", "*delete cluster", "scale *kafka", "scale *zookeeper", "*test", "*help", "*quit"];
    while True:
      print
      self.__showItems("kafka+zookeeper cluster helper", items)
      op = raw_input("choose command: ")
      if op == "c":
        if not self.k8s.oke:
          self.zookeeper.createPv()
          self.kafka.createPv()
        self.zookeeper.create()
        self.kafka.create()
      elif op == "r":
        self.k8s.status()
      elif op == "k":
        self.kafka.scale(input("kafka replicas: "))
      elif op == "z":
        self.zookeeper.scale(input("zookeeper replicas: "))
      elif op == "d":
        print yellow("confirm to delete the zookeeper+kafka cluster? (y/n)"),
        yn = raw_input("")
        if yn != "Y" and yn != "y": continue
        self.kafka.delete()
        self.kafka.deletePvc()
        self.zookeeper.delete()
        self.zookeeper.deletePvc()
        if not self.k8s.oke:
          self.kafka.deletePV()
          self.zookeeper.deletePV()
      elif op == "t":
        self.__test()
      elif op == "h":
        print "To suppress .pyc files: export PYTHONDONTWRITEBYTECODE=1"
      elif op == "q":
        exit()
      else:
        print red("Unknown command.")
      print
