import Pyro4
import socket
import threading
import sys


SERV_A_URI = None
SERV_B_URI = None
SERV_C_URI = None
SERV_D_URI = None
SERV_E_URI = None
NUM_SERVERS = None

Pyro4.config.SERIALIZER = "marshal"


@Pyro4.expose
@Pyro4.behavior(instance_mode="single")
class Coordinator(object):
    def __init__(self):
        self.tsDict = {"A": {}, "B": {}, "C": {}, "D": {}, "E": {}}
        self.tsDictLock = threading.Lock()
        self.txIdList = []
        self.txIdListLock = threading.Lock()
        self.txIdAccList = []
        self.txIdAccListLock = threading.Lock()
        self.deadTxId = set()
        self.deadTxIdLock = threading.Lock()
        self.txId = 0
        self.servA = Pyro4.Proxy(SERV_A_URI)
        self.servB = Pyro4.Proxy(SERV_B_URI)
        self.servC = Pyro4.Proxy(SERV_C_URI)
        self.servD = Pyro4.Proxy(SERV_D_URI)
        self.servE = Pyro4.Proxy(SERV_E_URI)

    def getTimeStampRead(self, serv, acc):
        if acc in self.tsDict[serv].keys():
            return self.tsDict[serv][acc][0]
        else:
            with self.tsDictLock:
                self.tsDict[serv].update({acc: (-1, -1)})
            return -1

    def getTimeStampWrite(self, serv, acc):
        if acc in self.tsDict[serv].keys():
            return self.tsDict[serv][acc][1]  # = write ts
        else:
            with self.tsDictLock:
                self.tsDict[serv].update({acc: (-1, -1)})
            return -1

    def updateTimeStampRead(self, serv, acc, ts):
        with self.tsDictLock:
            tsTuple = self.tsDict[serv][acc]
            newTuple = (ts, tsTuple[1])
            self.tsDict[serv][acc] = newTuple

    def updateTimeStampWrite(self, serv, acc, ts):
        with self.tsDictLock:
            print("A")
            tsTuple = self.tsDict[serv][acc]
            print("B", tsTuple)
            newTuple = (tsTuple[0], ts)
            print("C", newTuple)
            self.tsDict[serv][acc] = newTuple
            print("D")

    def abortNewerTx(self, serv, ip, inst, tid, acc, rw):
        tsRevive = self.txIdList.index((ip, inst, tid))
        print("A", tsRevive)
        toAbort = []
        print("B")
        for idx in range(tsRevive + 1, len(self.txIdList)):
            print((serv, acc), "||", self.txIdAccList[idx])
            if self.txIdAccList[idx] != None:
                if (serv, acc) in self.txIdAccList[idx]:
                    toAbort.append(idx)
        print(toAbort)
        with self.txIdAccListLock and self.txIdListLock:
            for idx in toAbort:
                print(idx, self.txIdList[idx])
                tup = self.txIdList[idx]
                for i in range(NUM_SERVERS):
                    if i == 0:
                        self.servA.coordinatorAbort(tup[0], tup[1], tup[2])
                    elif i == 1:
                        self.servB.coordinatorAbort(tup[0], tup[1], tup[2])
                    elif i == 2:
                        self.servC.coordinatorAbort(tup[0], tup[1], tup[2])
                    elif i == 3:
                        self.servD.coordinatorAbort(tup[0], tup[1], tup[2])
                    elif i == 4:
                        self.servE.coordinatorAbort(tup[0], tup[1], tup[2])

                self.txIdList[idx] = None
                self.txIdAccList[idx] = None
        if rw == "WRITE":
            self.updateTimeStampWrite(serv, acc, self.txIdList.index((ip, inst, tid)))
        elif rw == "READ":
            self.updateTimeStampRead(serv, acc, self.txIdList.index((ip, inst, tid)))
        else:
            self.updateTimeStampWrite(serv, acc, self.txIdList.index((ip, inst, tid)))
            self.updateTimeStampRead(serv, acc, self.txIdList.index((ip, inst, tid)))

    def Begin(self, ip, inst, tid):
        with self.txIdListLock:
            if (ip, inst, tid) not in self.txIdList:
                self.txIdList.append((ip, inst, tid))
                with self.txIdAccListLock:
                    self.txIdAccList.append(set())

    def Read(self, serv, ip, inst, tid, acc):
        print("coordinatorRead txIdList:", self.txIdList)
        if (ip, inst, tid) in self.txIdList:
            ts = self.txIdList.index((ip, inst, tid))
            rts = self.getTimeStampRead(serv, acc)
            wts = self.getTimeStampWrite(serv, acc)
            if wts > ts:
                with self.txIdAccListLock:
                    self.txIdAccList[ts].add((serv, acc))
                self.abortNewerTx(serv, ip, inst, tid, acc, "READ")
                return True
            else:
                with self.txIdAccListLock:
                    self.txIdAccList[ts].add((serv, acc))
                self.updateTimeStampRead(serv, acc, max(ts, rts))
                return True
        else:
            with self.txIdListLock:
                self.txIdList.append((ip, inst, tid))
            with self.txIdAccListLock:
                self.txIdAccList.append(set())
            ts = self.txIdList.index((ip, inst, tid))
            rts = self.getTimeStampRead(serv, acc)
            wts = self.getTimeStampWrite(serv, acc)
            if wts > ts:
                with self.txIdAccListLock:
                    self.txIdAccList[ts].add((serv, acc))
                self.abortNewerTx(serv, ip, inst, tid, acc, "READ")
                return True
            else:
                with self.txIdAccListLock:
                    self.txIdAccList[ts].add((serv, acc))
                self.updateTimeStampRead(serv, acc, max(ts, rts))
                return True

    def Write(self, serv, ip, inst, tid, acc):
        if (ip, inst, tid) in self.txIdList:
            ts = self.txIdList.index((ip, inst, tid))
            rts = self.getTimeStampRead(serv, acc)
            wts = self.getTimeStampWrite(serv, acc)
            if rts > ts or wts > ts:
                with self.txIdAccListLock:
                    self.txIdAccList[ts].add((serv, acc))
                self.abortNewerTx(serv, ip, inst, tid, acc, "WRITE")
                return True
            else:
                with self.txIdAccListLock:
                    self.txIdAccList[ts].add((serv, acc))
                self.updateTimeStampWrite(serv, acc, ts)
                return True
        else:
            with self.txIdListLock:
                self.txIdList.append((ip, inst, tid))
            with self.txIdAccListLock:
                self.txIdAccList.append(set())
            ts = self.txIdList.index((ip, inst, tid))
            rts = self.getTimeStampRead(serv, acc)
            wts = self.getTimeStampWrite(serv, acc)
            if rts > ts or wts > ts:
                with self.txIdAccListLock:
                    self.txIdAccList[ts].add((serv, acc))
                self.abortNewerTx(serv, ip, inst, tid, acc, "WRITE")
                return True
            else:
                with self.txIdAccListLock:
                    self.txIdAccList[ts].add((serv, acc))
                self.updateTimeStampWrite(serv, acc, ts)
                return True

    def abortCommit(self, serv, ip, inst, tid):
        print("abortCommit", serv, ip, inst, tid)
        with self.txIdAccListLock and self.txIdListLock and self.deadTxIdLock:
            if (ip, inst, tid) in self.txIdList:
                ts = self.txIdList.index((ip, inst, tid))
                tup = self.txIdList[ts]
                for i in range(NUM_SERVERS):
                    if i == 0 and "A" != serv:
                        self.servA.coordinatorAbort(tup[0], tup[1], tup[2])
                    elif i == 1 and "B" != serv:
                        self.servB.coordinatorAbort(tup[0], tup[1], tup[2])
                    elif i == 2 and "C" != serv:
                        self.servC.coordinatorAbort(tup[0], tup[1], tup[2])
                    elif i == 3 and "D" != serv:
                        self.servD.coordinatorAbort(tup[0], tup[1], tup[2])
                    elif i == 4 and "E" != serv:
                        self.servE.coordinatorAbort(tup[0], tup[1], tup[2])
                self.txIdList[ts] = None
                self.txIdAccList[ts] = None
                self.deadTxId.add((ip, inst, tid))
                print("aborted commit", serv, ip, inst, tid)
                return True
            elif (ip, inst, tid) in self.deadTxId:
                return True
            else:
                print("failed to server abort")
                return False

    def abortInMiddle(self, serv, ip, inst, tid):
        print("abortInMiddle", serv, ip, inst, tid)
        print(self.txIdList)
        print(self.txIdAccList)
        if (ip, inst, tid) in self.txIdList:
            ts = self.txIdList.index((ip, inst, tid))
            with self.txIdListLock:
                self.txIdList[ts] = None
            with self.txIdAccListLock:
                self.txIdAccList[ts] = None
            with self.deadTxIdLock:
                self.deadTxId.add((ip, inst, tid))
            print("aborted in middle of tx", serv, ip, inst, tid)
            return True
        elif (ip, inst, tid) in self.deadTxId:
            return True
        else:
            print("no transaction to abort in coordinator yet")
            return True

    def Commit(self, serv, ip, inst, tid):
        print("Commit", serv, ip, inst, tid)
        if (ip, inst, tid) in self.txIdList:
            ts = self.txIdList.index((ip, inst, tid))
            with self.txIdAccListLock and self.txIdListLock:
                for idx in range(ts, -1, -1):
                    if ts != idx and self.txIdAccList[idx] != None:
                        for accTemp in self.txIdAccList[ts]:
                            if accTemp in self.txIdAccList[idx]:
                                print(
                                    "Found previous uncommited transaction",
                                    idx,
                                    ", aborted.",
                                )
                                self.txIdAccList[ts] = None
                                self.txIdList[ts] = None
                                return False
                for idx in range(ts, len(self.txIdAccList)):
                    if (
                        ts != idx
                        and self.txIdAccList[idx] != None
                        and self.txIdAccList[ts] != None
                    ):
                        for accTemp in self.txIdAccList[ts]:
                            if accTemp in self.txIdAccList[idx]:
                                tup = self.txIdList[idx]
                                for i in range(NUM_SERVERS):
                                    if i == 0:
                                        self.servA.coordinatorAbort(
                                            tup[0], tup[1], tup[2]
                                        )
                                    elif i == 1:
                                        self.servB.coordinatorAbort(
                                            tup[0], tup[1], tup[2]
                                        )
                                    elif i == 2:
                                        self.servC.coordinatorAbort(
                                            tup[0], tup[1], tup[2]
                                        )
                                    elif i == 3:
                                        self.servD.coordinatorAbort(
                                            tup[0], tup[1], tup[2]
                                        )
                                    elif i == 4:
                                        self.servE.coordinatorAbort(
                                            tup[0], tup[1], tup[2]
                                        )
                                self.txIdList[idx] = None
                                self.txIdAccList[idx] = None
            with self.txIdListLock:
                self.txIdList[ts] = None
            with self.txIdAccListLock:
                self.txIdAccList[ts] = None
            with self.deadTxIdLock:
                self.deadTxId.add((ip, inst, tid))
            return True
        elif (ip, inst, tid) in self.deadTxId:
            print("Commit another server has already commited for you", serv, "!")
            return True
        else:
            print("Commit transaction does not exist")
            return False


def main():
    global NUM_SERVERS, SERV_A_URI, SERV_B_URI, SERV_C_URI, SERV_D_URI, SERV_E_URI
    if len(sys.argv) < 2:
        print("USAGE: python3 coordinator.py <config.txt>")
        return
    config_file = sys.argv[1]
    config_list = None
    with open(config_file) as file:
        lines = file.readlines()
        config_list = [line.strip().split(" ") for line in lines]
        NUM_SERVERS = len(lines) - 1
    assert NUM_SERVERS > 0
    SERV_A_URI = "PYRO:servA@" + config_list[1][1] + ":" + config_list[1][2]
    SERV_B_URI = "PYRO:servB@" + config_list[2][1] + ":" + config_list[2][2]
    SERV_C_URI = "PYRO:servC@" + config_list[3][1] + ":" + config_list[3][2]
    SERV_D_URI = "PYRO:servD@" + config_list[4][1] + ":" + config_list[4][2]
    SERV_E_URI = "PYRO:servE@" + config_list[5][1] + ":" + config_list[5][2]
    Pyro4.Daemon.serveSimple(
        {Coordinator: "coordinator"},
        host=socket.gethostbyname(socket.gethostname()),
        port=int(config_list[0][2]),
        ns=False,
    )


if __name__ == "__main__":
    main()
