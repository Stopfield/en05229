import multiprocessing
import time

from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError, NoNodeError

barrier_path = "/barreira"

NUM_PROCESSOS = 5


def init_zookeeper():
    zk = KazooClient(hosts="127.0.0.1:2181")
    zk.start()
    zk.ensure_path(barrier_path)
    return zk


def barrier_enter(zk, name):  # entra na barreira e espera que todos entrem tbm
    try:
        zk.create(f"{barrier_path}/{name}", ephemeral=True)  # cria nó pra cada processo
    except NodeExistsError:
        pass

    while True:
        children = zk.get_children(barrier_path)
        print(f"{name} vê {len(children)} processos na barreira: {children}")
        if len(children) >= NUM_PROCESSOS:  # espera todos os processos entrarem
            break
        time.sleep(2)


def barrier_leave(zk, name):  # processo sai só quando todos estiverem prontos
    try:
        zk.delete(f"{barrier_path}/{name}")  # Remove o nó
    except NoNodeError:
        pass

    while True:
        children = zk.get_children(barrier_path)
        print(f"{name} vê {len(children)} processos restantes na barreira: {children}")
        if len(children) == 0:  # espera todos sairem
            break
        time.sleep(2)


def philosopher(name):
    zk = init_zookeeper()

    print(f"{name} está executando...")
    time.sleep(2)

    print(f"{name} está tentando entrar na barreira...")
    time.sleep(2)
    barrier_enter(zk, name)  # entra na barreira
    print(f"{name} entrou na barreira!")
    time.sleep(2)

    print(f"{name} está comendo...")
    time.sleep(2)

    print(f"{name} está tentando sair da barreira...")
    time.sleep(2)
    barrier_leave(zk, name)  # sai da barreira
    print(f"{name} saiu da barreira e voltou a pensar.")
    time.sleep(2)

    zk.stop()


if __name__ == "__main__":
    processes = []

    for i in range(NUM_PROCESSOS):
        p = multiprocessing.Process(target=philosopher, args=(f"Processo-{i + 1}",))
        processes.append(p)
        p.start()

    for p in processes:  # aguardo todos os processos executarem
        p.join()

    print("Todos os processos terminaram.")
