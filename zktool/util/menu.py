#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from util.style import warn, red, style
from k8s import K8s, KafkaClient

K8s.init()

from chart import KafkaChart, ZookeeperChart
import re
import os


class Menu:
    title = ""
    items = []

    @staticmethod
    def __replace(matched):
        op = matched.group('op')
        return style(op[1:len(op) - 1], mode="bold", fore="yellow")

    @staticmethod
    def __style_item(title):
        return re.sub("(?P<op>\([a-z]+\))", Menu.__replace, title)

    def _show(self):
        print
        stars = 40
        print "-" * stars + self.title + "-" * stars
        for item in self.items:
            print self.__style_item(item)
        print "-" * (stars * 2 + len(self.title))

    def main(self):
        pass


class AdminMenu(Menu):
    kafka = KafkaChart()
    zookeeper = ZookeeperChart()

    def __init__(self):
        self.title = "kafka+zookeeper cluster helper"
        self.items = [
            "(c)reate zookeeper and kafka",
            "(r)ead status (node, sc, pv, pvc, sts, pod, svc)",
            "(k)afka/(z)ookeeper scaling, e.g., k 3",
            "(d)elete zookeeper and kafka",
            "(desc)ribe/(log) resource, e.g. desc pv/kafka-pv-0, log statefulset.apps/kafka",
            "(h)elp", "(q)uit"
        ]

    def main(self):
        while True:
            self._show()
            op = raw_input("command: ").strip()
            if op == "c":
                self.zookeeper.create()
                self.kafka.create()
            elif op == "r":
                K8s.status()
            elif op.startswith("k "):
                replicas = int(op.split()[1])
                self.kafka.scale(replicas)
            elif op.startswith("z "):
                replicas = int(op.split()[1])
                self.zookeeper.scale(replicas)
            elif op == "d":
                print warn("do want to delete the zookeeper+kafka cluster? (y/n)"),
                yn = raw_input("").strip()
                if yn != "Y" and yn != "y": continue
                self.kafka.delete()
                self.zookeeper.delete()
            elif op.startswith("des"):
                resource = op.split()[1]
                os.system("kubectl describe " + resource)
            elif op.startswith("log"):
                resource = op.split()[1]
                os.system("kubectl logs " + resource)
            elif op == "h":
                if K8s.oke:
                    os.system("kubectl get svc -l release=kafka -o=jsonpath='{range .items[*]} "
                              "{.status.loadBalancer.ingress[0].ip}{\"\\t\"}{.metadata.labels.pod}"
                              "{\".example.com\\n\"}' | grep kafka")
            elif op == "q":
                exit()
            else:
                print red("Invalid command.")
            print


class TestMenu(Menu):
    client = KafkaClient()

    def __init__(self):
        self.title = "Test tool"
        self.items = [
            "(l)ist topics",
            "(c)reate topic, e.g., c test 1 1",
            "(w)rite message",
            "(r)ead message",
            "(p)roducer (p)erf, e.g, pp 100000",
            "(c)onsumer (p)erf, e.g., cp 100000",
            "(q)uit"
        ]

    def main(self):
        print warn("make sure pods are running and ready")

        while True:
            self._show()
            op = raw_input("input command: ").strip()
            if op == "l":
                self.client.list_topics()
            elif op.startswith("c "):
                tokens = op.split()
                if len(tokens) < 4:
                    print "args missing"
                else:
                    self.client.create_topic(tokens[1], tokens[2], tokens[3])
            elif op == "w":
                self.client.console_produce()
            elif op == "r":
                self.client.console_consume()
            elif op.startswith("pp "):
                records = op.split()[1]
                self.client.test_producer_perf(records)
            elif op.startswith("cp "):
                records = op.split()[1]
                self.client.test_consume_perf(records)
            elif op == "q":
                return
            else:
                print red("Invalid command.")
