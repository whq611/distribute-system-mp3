import Pyro4
import sys
import socket
import threading

MY_IP = socket.gethostbyname(socket.gethostname())
MY_INST = 0
MY_URI = None
NUM_SERVERS = 0
SERV_A_URI = None
SERV_B_URI = None
SERV_C_URI = None
SERV_D_URI = None
SERV_E_URI = None
SERV_A = None
SERV_B = None
SERV_C = None
SERV_D = None
SERV_E = None

inTransaction = False
txId = 0

Pyro4.config.SERIALIZER = "marshal"


@Pyro4.expose
class ClientAborter(object):
    def __init__(self):
        """
        """

    def Abort(self):
        global inTransaction
        global txId
        if inTransaction == True:
            inTransaction = False
            txId += 1


def connectToAll():
    sys.execepthook = Pyro4.util.excepthook
    return (
        Pyro4.Proxy(SERV_A_URI),
        Pyro4.Proxy(SERV_B_URI),
        Pyro4.Proxy(SERV_C_URI),
        Pyro4.Proxy(SERV_D_URI),
        Pyro4.Proxy(SERV_E_URI),
    )


def parseCommand(cmd):
    splitted = cmd.split()
    cmdType = None
    server = None
    acc = None
    amount = None
    assert len(splitted) <= 3
    for i in range(len(splitted)):
        if i == 0:
            cmdType = splitted[i]
        elif i == 1:
            server = splitted[i].split(".")[0].capitalize()
            acc = splitted[i].split(".")[1]
        elif i == 2:
            amount = splitted[i]
    return cmdType, server, acc, amount


def aborter():
    global MY_URI
    daemon = Pyro4.Daemon(host=socket.gethostbyname(socket.gethostname()))
    MY_URI = daemon.register(ClientAborter)
    daemon.requestLoop()


def main():
    global MY_INST
    global txId
    global inTransaction
    global NUM_SERVERS, SERV_A_URI, SERV_B_URI, SERV_C_URI, SERV_D_URI, SERV_E_URI
    if len(sys.argv) < 3:
        print("USAGE: python3 client.py <INSTACE_ID> <config_file_name>")
        return
    MY_INST = sys.argv[1]
    config_file = sys.argv[2]
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
    print("You are instance ID", MY_INST, "on", MY_IP)
    SERV_A, SERV_B, SERV_C, SERV_D, SERV_E = connectToAll()
    servProxyDict = {"A": SERV_A, "B": SERV_B, "C": SERV_C, "D": SERV_D, "E": SERV_E}

    aborterThread = threading.Thread(target=aborter, args=())
    aborterThread.start()

    while True:
        try:
            cmd = input()
        except EOFError:
            pass
        cmdType, server, acc, amount = parseCommand(cmd)
        if inTransaction == False and (cmdType == "BEGIN"):
            inTransaction = True
            p_flag = True
            for i in range(NUM_SERVERS):
                if i == 0:
                    ret = SERV_A.Begin(MY_IP, MY_INST, txId, MY_URI)
                    if ret != "OK":
                        p_flag = False
                elif i == 1:
                    ret = SERV_B.Begin(MY_IP, MY_INST, txId, MY_URI)
                    if ret != "OK":
                        p_flag = False
                elif i == 2:
                    ret = SERV_C.Begin(MY_IP, MY_INST, txId, MY_URI)
                    if ret != "OK":
                        p_flag = False
                elif i == 3:
                    ret = SERV_D.Begin(MY_IP, MY_INST, txId, MY_URI)
                    if ret != "OK":
                        p_flag = False
                elif i == 4:
                    ret = SERV_E.Begin(MY_IP, MY_INST, txId, MY_URI)
                    if ret != "OK":
                        p_flag = False
            if p_flag == True:
                print("OK")
        elif inTransaction == True:
            if (cmdType == "DEPOSIT") and acc != None and amount != None:
                try:
                    ret = servProxyDict[server].Deposit(
                        MY_IP, MY_INST, txId, acc, amount
                    )
                    print(ret)
                except:
                    print("Pyro Traceback DEPOSIT")
                    print("".join(Pyro4.util.getPyroTraceback()))
            elif cmdType == "BALANCE":
                ret = servProxyDict[server].Balance(MY_IP, MY_INST, txId, acc)
                print(ret)
                if ret == "NOT FOUND, ABORTED":
                    inTransaction = False
                    txId += 1
            elif cmdType == "WITHDRAW":
                ret = servProxyDict[server].Withdraw(MY_IP, MY_INST, txId, acc, amount)
                print(ret)
            elif cmdType == "COMMIT":
                c_flag = True
                for i in range(NUM_SERVERS):
                    if i == 0:
                        try:
                            ret = SERV_A.Commit(MY_IP, MY_INST, txId)
                            if ret != "COMMIT OK":
                                c_flag = False
                        except:
                            print("Pyro Traceback COMMIT A")
                            print("".join(Pyro4.util.getPyroTraceback()))
                    elif i == 1:
                        try:
                            ret = SERV_B.Commit(MY_IP, MY_INST, txId)
                            if ret != "COMMIT OK":
                                c_flag = False
                        except:
                            print("Pyro Traceback COMMIT B")
                            print("".join(Pyro4.util.getPyroTraceback()))
                    elif i == 2:
                        try:
                            ret = SERV_C.Commit(MY_IP, MY_INST, txId)
                            if ret != "COMMIT OK":
                                c_flag = False
                        except:
                            print("Pyro Traceback COMMIT C")
                            print("".join(Pyro4.util.getPyroTraceback()))
                    elif i == 3:
                        try:
                            ret = SERV_D.Commit(MY_IP, MY_INST, txId)
                            if ret != "COMMIT OK":
                                c_flag = False
                        except:
                            print("Pyro Traceback COMMIT D")
                            print("".join(Pyro4.util.getPyroTraceback()))
                    elif i == 4:
                        try:
                            ret = SERV_E.Commit(MY_IP, MY_INST, txId)
                            if ret != "COMMIT OK":
                                c_flag = False
                        except:
                            print("Pyro Traceback COMMIT E")
                            print("".join(Pyro4.util.getPyroTraceback()))
                if c_flag == True:
                    print("COMMIT OK")
                txId += 1
                inTransaction = False
            elif cmdType == "ABORT" or cmdType == "abort":
                print("client", MY_IP, MY_INST, "txId", txId, "abort")
                for i in range(NUM_SERVERS):
                    if i == 0:
                        ret = SERV_A.clientAbort(MY_IP, MY_INST, txId)
                        print(ret)
                    elif i == 1:
                        ret = SERV_B.clientAbort(MY_IP, MY_INST, txId)
                        print(ret)
                    elif i == 2:
                        ret = SERV_C.clientAbort(MY_IP, MY_INST, txId)
                        print(ret)
                    elif i == 3:
                        ret = SERV_D.clientAbort(MY_IP, MY_INST, txId)
                        print(ret)
                    elif i == 4:
                        ret = SERV_E.clientAbort(MY_IP, MY_INST, txId)
                        print(ret)
                inTransaction = False
                txId += 1
            else:
                print("client: INVALID COMMAND TYPE")


if __name__ == "__main__":
    main()
