import Pyro4
import socket
import threading
import sys


SERV_NAME = None

COORDINATOR_URI = None

Pyro4.config.SERIALIZER = "marshal"


@Pyro4.expose
@Pyro4.behavior(instance_mode="single")
class Server(object):
    def __init__(self):
        self.finalDict = {}
        self.finalDictLock = threading.Lock()
        self.tentativeDicts = {}
        self.tentativeDictsLock = threading.Lock()
        self.coordinator = Pyro4.Proxy(COORDINATOR_URI)
        self.clients = set()

    def Begin(self, ip, inst, tid, uri):
        self.tentativeDicts.update({(ip, inst, tid): {}})
        self.clients.add((ip, inst, uri))
        self.coordinator.Begin(ip, inst, tid)
        return "OK"

    def Deposit(self, ip, inst, tid, acc, amount):
        readValue = None
        if (ip, inst, tid) in self.tentativeDicts.keys():
            if acc in self.tentativeDicts[(ip, inst, tid)].keys():
                if self.coordinator.Read(SERV_NAME, ip, inst, tid, acc):
                    readValue = self.tentativeDicts[(ip, inst, tid)][acc]
                else:
                    print("Deposit ABORT", ip, inst, tid)
                    return "ABORTED"
            else:
                if acc in self.finalDict.keys():
                    if self.coordinator.Read(SERV_NAME, ip, inst, tid, acc):
                        readValue = self.finalDict[acc]
                else:
                    if self.coordinator.Read(SERV_NAME, ip, inst, tid, acc):
                        readValue = 0
                    else:
                        print("Deposit ABORT", ip, inst, tid)
                        return "ABORTED"
        else:
            if acc in self.finalDict.keys():
                if self.coordinator.Read(SERV_NAME, ip, inst, tid, acc):
                    readValue = self.finalDict[acc]
            else:
                if self.coordinator.Read(SERV_NAME, ip, inst, tid, acc):
                    readValue = 0
                else:
                    print("Deposit ABORT", ip, inst, tid)
                    return "ABORTED"

        assert readValue != None

        if self.coordinator.Write(SERV_NAME, ip, inst, tid, acc):
            with self.tentativeDictsLock:
                if (ip, inst, tid) in self.tentativeDicts.keys():
                    nextValue = -1
                    if readValue + float(amount) >= 1000000000:
                        nextValue = 1000000000
                    else:
                        nextValue = readValue + float(amount)
                    assert nextValue != -1
                    self.tentativeDicts[(ip, inst, tid)][acc] = nextValue
                    return "OK"
                else:
                    if (ip, inst, tid) not in self.tentativeDicts.keys():
                        return ""
                    else:
                        nextValue = -1
                        if readValue + float(amount) >= 1000000000:
                            nextValue = 1000000000
                        else:
                            nextValue = readValue + float(amount)
                        assert nextValue != -1
                        self.tentativeDicts[(ip, inst, tid)][acc] = nextValue
                        return "OK"
        else:
            print("Deposit ABORT", ip, inst, tid)
            return "ABORTED"

    def Balance(self, ip, inst, txId, acc):
        print("Balance", ip, inst, txId, acc)
        if (ip, inst, txId) in self.tentativeDicts.keys():
            if acc in self.tentativeDicts[(ip, inst, txId)].keys():
                if self.coordinator.Read(SERV_NAME, ip, inst, txId, acc):
                    return (
                        SERV_NAME
                        + "."
                        + acc
                        + " = "
                        + str(self.tentativeDicts[(ip, inst, txId)][acc])
                    )
                else:
                    self.coordinator.abortCommit(SERV_NAME, ip, inst, txId)
                    with self.tentativeDictsLock:
                        del self.tentativeDicts[(ip, inst, txId)]
                    return "NOT FOUND, ABORTED"
            else:
                if acc in self.finalDict.keys():
                    if self.coordinator.Read(SERV_NAME, ip, inst, txId, acc):
                        return SERV_NAME + "." + acc + " = " + str(self.finalDict[acc])
                else:
                    self.coordinator.abortCommit(SERV_NAME, ip, inst, txId)
                    with self.tentativeDictsLock:
                        del self.tentativeDicts[(ip, inst, txId)]
                    return "NOT FOUND, ABORTED"
        else:
            if acc in self.finalDict.keys():
                if self.coordinator.Read(SERV_NAME, ip, inst, txId, acc):
                    return SERV_NAME + "." + acc + " = " + str(self.finalDict[acc])
                else:
                    self.coordinator.abortCommit(SERV_NAME, ip, inst, txId)
                    return "NOT FOUND, ABORTED"

    def Withdraw(self, ip, inst, tid, acc, amount):
        print("Withdraw", ip, inst, tid, acc, amount)
        readValue = None
        if (ip, inst, tid) in self.tentativeDicts.keys():
            if acc in self.tentativeDicts[(ip, inst, tid)].keys():
                if self.coordinator.Read(SERV_NAME, ip, inst, tid, acc):
                    readValue = self.tentativeDicts[(ip, inst, tid)][acc]
                else:
                    print("Withdraw ABORT", ip, inst, tid)
                    return "NOT FOUND, ABORTED"
            else:
                if acc in self.finalDict.keys():
                    if self.coordinator.Read(SERV_NAME, ip, inst, tid, acc):
                        readValue = self.finalDict[acc]
                else:
                    if self.coordinator.Read(SERV_NAME, ip, inst, tid, acc):
                        readValue = 0
                    else:
                        print("Withdraw ABORT", ip, inst, tid)
                        return "NOT FOUND, ABORTED"
        else:
            if acc in self.finalDict.keys():
                if self.coordinator.Read(SERV_NAME, ip, inst, tid, acc):
                    readValue = self.finalDict[acc]
            else:
                if self.coordinator.Read(SERV_NAME, ip, inst, tid, acc):
                    readValue = 0
                else:
                    print("Withdraw ABORT", ip, inst, tid)
                    return "NOT FOUND, ABORTED"

        assert readValue != None

        if self.coordinator.Write(SERV_NAME, ip, inst, tid, acc):
            with self.tentativeDictsLock:
                if (ip, inst, tid) in self.tentativeDicts.keys():
                    self.tentativeDicts[(ip, inst, tid)][acc] = readValue - float(
                        amount
                    )
                    return "OK"
                else:
                    if (ip, inst, tid) not in self.tentativeDicts.keys():
                        return ""
                    else:
                        self.tentativeDicts.update(
                            {(ip, inst, tid): {acc: readValue - float(amount)}}
                        )
                        return "OK"
        else:
            print("Deposit ABORT", ip, inst, tid)
            return "ABORTED"

    def Commit(self, ip, inst, txId):
        print("Commit", ip, inst, txId)

        with self.tentativeDictsLock:
            if (ip, inst, txId) in self.tentativeDicts.keys():
                for balance in self.tentativeDicts[(ip, inst, txId)].values():
                    if balance < 0:
                        print("Commit found negative balance")
                        self.coordinator.abortCommit(SERV_NAME, ip, inst, txId)
                        del self.tentativeDicts[(ip, inst, txId)]
                        return "COMMIT ABORTED"

        flag = False
        try:
            flag = self.coordinator.Commit(SERV_NAME, ip, inst, txId)
        except:
            print("Pyro TraceBack server COMMIT")
            print("".join(Pyro4.util.getPyroTraceback()))
        if flag:
            with self.finalDictLock and self.tentativeDictsLock:
                if (ip, inst, txId) in self.tentativeDicts.keys():
                    for acc, amount in self.tentativeDicts[(ip, inst, txId)].items():
                        self.finalDict.update({acc: amount})
                    del self.tentativeDicts[(ip, inst, txId)]
                    return "COMMIT OK"
                else:
                    self.coordinator.abortCommit(SERV_NAME, ip, inst, txId)
                    return "COMMIT ABORTED"
        else:
            self.coordinator.abortCommit(SERV_NAME, ip, inst, txId)
            return "COMMIT ABORTED"

    def coordinatorAbort(self, ip, inst, txId):
        with self.tentativeDictsLock:
            if (ip, inst, txId) in self.tentativeDicts.keys():
                print("Abort", ip, inst, txId)
                del self.tentativeDicts[(ip, inst, txId)]
                for client in self.clients:
                    if client[0] == ip and client[1] == inst:
                        c = Pyro4.Proxy(client[2])
                        c.Abort()
                        break
            else:
                print("No need to abort", ip, inst, txId)

    def clientAbort(self, ip, inst, txId):
        with self.tentativeDictsLock:
            if (ip, inst, txId) in self.tentativeDicts.keys():
                print("clientAbort", ip, inst, txId)
                del self.tentativeDicts[(ip, inst, txId)]
                self.coordinator.abortCommit(SERV_NAME, ip, inst, txId)
                print("clientAbort completed")
                return "CLIENT ABORTED"
            else:
                print("clientAbort failed")
                return "CLIENT ABORT FAILED"


def main():
    global SERV_NAME, COORDINATOR_URI
    if len(sys.argv) < 2:
        print("USAGE: python3 server.py <server_name> <config_file_name>")
        print("<server_name> is A, B, C, D, or E")
        return
    config_file = sys.argv[2]
    config_list = None
    with open(config_file) as file:
        lines = file.readlines()
        config_list = [line.strip().split(" ") for line in lines]
    SERV_NAME = sys.argv[1].capitalize()
    COORDINATOR_URI = "PYRO:coordinator@" + config_list[0][1] + ":" + config_list[0][2]
    name = "serv" + SERV_NAME
    offset = None
    if SERV_NAME == "A":
        offset = int(config_list[1][2])
    elif SERV_NAME == "B":
        offset = int(config_list[2][2])
    elif SERV_NAME == "C":
        offset = int(config_list[3][2])
    elif SERV_NAME == "D":
        offset = int(config_list[4][2])
    elif SERV_NAME == "E":
        offset = int(config_list[5][2])
    else:
        print("USAGE: python3 server.py <server_name> <config_file_name>")
        print("<server_name> is A, B, C, D, or E")
        return
    assert offset != None
    Pyro4.Daemon.serveSimple(
        {Server: name},
        host=socket.gethostbyname(socket.gethostname()),
        port=offset,
        ns=False,
    )


if __name__ == "__main__":
    main()
