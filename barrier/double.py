import multiprocessing
import time

from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError, NoNodeError

class DoubleBarrier:
    def __init__(self, client: KazooClient, path: str, condition):
        self.client = client
        self.path = path
        self.condition = condition
    
    def create(self):
        self.client.ensure_path(self.path)

    def barrier_enter(self, name):  # entra na barreira e espera que todos entrem tbm
        try:
            self.client.create(f"{self.path}/{name}", ephemeral=True)  # cria nó pra cada processo
        except NodeExistsError:
            pass

        while True:
            children = self.client.get_children(self.path)
            print(f"{name} vê {len(children)} processos na barreira: {children}")
            if len(children) >= self.condition:  # espera todos os processos entrarem
                break
            time.sleep(2)


    def barrier_leave(self, name):  # processo sai só quando todos estiverem prontos
        try:
            self.client.delete(f"{self.path}/{name}")  # Remove o nó
        except NoNodeError:
            pass

        while True:
            children = self.client.get_children(self.path)
            print(f"{name} vê {len(children)} processos restantes na barreira: {children}")
            if len(children) == 0:  # espera todos sairem
                break
            time.sleep(2)


if __name__ == "__main__":
    barrier_path = "/barreira"
    NUM_PROCESSOS = 5

    def init_zookeeper():
        zk = KazooClient(hosts="127.0.0.1:2181")
        zk.start()
        return zk

    def philosopher(name):
        zk = init_zookeeper()
        barrier = DoubleBarrier(zk, barrier_path, NUM_PROCESSOS)

        print(f"{name} está executando...")
        time.sleep(2)

        print(f"{name} está tentando entrar na barreira...")
        time.sleep(2)
        barrier.barrier_enter(zk, name)  # entra na barreira
        print(f"{name} entrou na barreira!")
        time.sleep(2)

        print(f"{name} está comendo...")
        time.sleep(2)

        print(f"{name} está tentando sair da barreira...")
        time.sleep(2)
        barrier.barrier_leave(zk, name)  # sai da barreira
        print(f"{name} saiu da barreira e voltou a pensar.")
        time.sleep(2)

        zk.stop()

    zk = init_zookeeper()

    processes = []

    for i in range(NUM_PROCESSOS):
        p = multiprocessing.Process(target=philosopher, args=(f"Processo-{i + 1}",))
        processes.append(p)
        p.start()

    for p in processes:  # aguardo todos os processos executarem
        p.join()

    print("Todos os processos terminaram.")
